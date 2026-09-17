from collections.abc import Sequence
from dataclasses import replace
from datetime import datetime
from itertools import cycle

import pandas as pd

from database_connector.ClickHouseConnection import ClickHouseConnection
from database_connector.DatabaseConfig import (
    DatabaseConfig, THEATRE_RU_HOSTS, THEATRE_CONNECTIONS, normalize_connection,
)
from extractor.ExecutionConfig import ExecutionConfig
from extractor.ParallelExtractor import ParallelExtractor
from request.ExtractionRequest import ExtractionRequest
from time_chunks.TimeChunkType import TimeChunkType


class DataExtractor:

    """Выгрузка THEATRE с возможностью задать свой список серверов.

    Создайте экземпляр с параметрами запроса, затем вызовите .extract().
    Конструктор читает настройки; соединения открываются при выгрузке.
    Для простого вызова и явного env_file используйте parallel_request.extract.
    Параметры конструктора описаны в __init__.
    """

    def __init__(
        self,
        query: str,
        start_date: datetime,
        end_date: datetime,
        chunk_type: TimeChunkType,
        workers: int = 6,
        database_type: str = "clickhouse",
        clickhouse_hosts: Sequence[str] | None = None,
        conn: str = "THEATRE_RU",
    ) -> None:

        """Настроить выгрузку и прочитать реквизиты выбранного источника.

        Args:
            query: SQL с {start_date} и {end_date} без кавычек.
            start_date: datetime, включённое начало периода.
            end_date: datetime, исключённый конец, позже start_date.
                Обе даты должны быть с часовым поясом или обе без него.
            chunk_type: Обязательный TimeChunkType: HOUR, DAY, WEEK, MONTH,
                QUARTER или YEAR. Например, TimeChunkType.DAY.
            workers: Общий максимум параллельных запросов, целое число > 0.
                По умолчанию 6; фактически не больше количества чанков.
            database_type: Только "clickhouse" (по умолчанию).
                Параметр сохранён для совместимости.
            clickhouse_hosts: Список уникальных имён серверов без протокола
                и порта. None — три сервера выбранного conn. Явный список
                заменяет серверы, но не меняет регион реквизитов. Порт 8123.
            conn: "THEATRE_RU" (по умолчанию) или "THEATRE_ASIA".
                Используются THEATRE_LOGIN_RU / THEATRE_PASS_RU либо
                THEATRE_LOGIN_ASIA / THEATRE_PASS_ASIA соответственно.

        Notes:
            .env загружается через load_dotenv(); уже заданные переменные
            окружения не заменяются. Параметра env_file у класса нет.
            Конструктор не выполняет SQL. Вызовите .extract() для выгрузки.

        Raises:
            ValueError: Неверные настройки или отсутствующие реквизиты.
        """

        self.query = query
        self.start_date = start_date
        self.end_date = end_date
        self.chunk_type = chunk_type
        # Аргумент сохранён для существующих вызовов из main.py.
        if database_type.strip().lower() != "clickhouse":
            raise ValueError("Поддерживается только ClickHouse THEATRE.")

        self.execution_config = ExecutionConfig(workers=workers)
        conn = normalize_connection(conn)
        hosts = self._validate_clickhouse_hosts(
            THEATRE_CONNECTIONS[conn][0] if clickhouse_hosts is None else clickhouse_hosts
        )
        self.database_config = DatabaseConfig.from_env(conn=conn)
        self.database_configs = tuple(
            replace(self.database_config, host=host) for host in hosts
        )
        # Фабрика вызывается последовательно координатором до запуска каждого
        # воркера. У каждого воркера свой клиент, закреплённый за одним сервером.
        self._connection_configs = cycle(self.database_configs)

        self.parallel_extractor = ParallelExtractor(
            connection_factory=self._create_connection,
            execution_config=self.execution_config,
        )

    def extract(self) -> pd.DataFrame:
        """Выполнить настроенный запрос по чанкам и вернуть pandas.DataFrame.

        Ожидает завершения всех чанков и объединяет строки в порядке завершения,
        без общей сортировки. Результат хранится в памяти; файл не создаётся.
        Агрегации, LIMIT и ORDER BY применяются отдельно внутри каждого чанка.
        При ошибке выбрасывается исключение без возврата частичного результата.
        При прерывании ожидает завершения уже запущенных запросов.
        """
        request = ExtractionRequest(
            query=self.query,
            start_date=self.start_date,
            end_date=self.end_date,
            chunk_type=self.chunk_type,
        )

        return self.parallel_extractor.extract(request)

    def _create_connection(self) -> ClickHouseConnection:
        return ClickHouseConnection(next(self._connection_configs))

    @staticmethod
    def _validate_clickhouse_hosts(hosts: Sequence[str] | None) -> tuple[str, ...]:
        if hosts is None:
            return THEATRE_RU_HOSTS

        if isinstance(hosts, (str, bytes)) or not isinstance(hosts, Sequence):
            raise ValueError("clickhouse_hosts должен быть списком имён серверов.")

        if not hosts:
            raise ValueError("Нужно указать хотя бы один сервер ClickHouse.")

        normalized_hosts = []
        for host in hosts:
            if not isinstance(host, str) or not host.strip():
                raise ValueError("Имя сервера ClickHouse не может быть пустым.")
            host = host.strip()
            if any(char.isspace() for char in host) or any(char in host for char in ":/@?#"):
                raise ValueError(
                    "Укажите только имя сервера ClickHouse, без протокола, порта и реквизитов. "
                    "Порт 8123 задан в DatabaseConfig."
                )
            normalized_hosts.append(host)

        if len({host.lower() for host in normalized_hosts}) != len(normalized_hosts):
            raise ValueError("Список серверов ClickHouse не должен содержать повторов.")

        return tuple(normalized_hosts)
