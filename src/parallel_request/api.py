import os
from dataclasses import replace
from datetime import date, datetime, time
from itertools import cycle
from os import PathLike
from pathlib import Path

import pandas as pd
from dotenv import dotenv_values

from database_connector.ClickHouseConnection import ClickHouseConnection
from database_connector.DatabaseConfig import (
    DatabaseConfig,
    THEATRE_CONNECTIONS,
    normalize_connection as _as_connection,
)
from extractor.ExecutionConfig import ExecutionConfig
from extractor.ParallelExtractor import ParallelExtractor
from request.ExtractionRequest import ExtractionRequest
from time_chunks.TimeChunkType import TimeChunkType


def extract(
    query: str,
    start_date: str | date | datetime,
    end_date: str | date | datetime,
    *,
    chunk_type: str | TimeChunkType = "day",
    workers: int = 6,
    env_file: str | PathLike[str] | None = ".env",
    conn: str = "THEATRE_RU",
) -> pd.DataFrame:
    """Выгрузить данные THEATRE_RU или THEATRE_ASIA в один pandas.DataFrame.

    Делит период [start_date, end_date) на чанки, выполняет запросы параллельно
    и объединяет результаты в памяти. Ожидает завершения всей выгрузки.

    Args:
        query: Непустой SQL с {start_date} и {end_date} без кавычек.
            Не используйте f-строку или .format() для подстановки этих дат.
        start_date: Включённое начало периода: строка ISO ("2026-05-01",
            "2026-05-01T10:30:00"), date или datetime. date означает полночь.
        end_date: Исключённый конец периода, в тех же форматах.
            Должен быть позже start_date. Обе даты — с часовым поясом
            или обе без него. Для всего мая укажите end_date="2026-06-01".
        chunk_type: Размер чанка: "hour", "day", "week", "month",
            "quarter", "year" или TimeChunkType. По умолчанию "day".
            hour/day/week отсчитываются от начала периода; month/quarter/year
            заканчиваются на календарных границах. Пустая строка недопустима.
        workers: Общий максимум одновременных запросов на все серверы.
            Целое число > 0, по умолчанию 6. Не больше количества чанков.
            Воркеры распределяются по трём серверам выбранного региона
            по кругу; workers=6 даёт по два на сервер при >= 6 чанках.
        env_file: Путь к .env, по умолчанию ".env" в текущей рабочей папке.
            Принимает str или PathLike; None — только переменные окружения.
            Окружение имеет приоритет над файлом.
        conn: "THEATRE_RU" (по умолчанию) или "THEATRE_ASIA".
            Для RU нужны THEATRE_LOGIN_RU и THEATRE_PASS_RU;
            для Asia — THEATRE_LOGIN_ASIA и THEATRE_PASS_ASIA.
            Реквизиты другого региона не нужны. Порт 8123,
            база BookingFormAnalyticsV2. Регистр conn не важен.

    Returns:
        pandas.DataFrame: Результаты всех чанков в порядке их завершения,
            с новым индексом и без общей сортировки. Файлы не создаются.
            Весь результат должен помещаться в памяти.

    Raises:
        TypeError: Неподдерживаемый тип query, start_date или end_date.
        ValueError: Неверные даты, workers, chunk_type, conn, шаблоны SQL
            или отсутствующие реквизиты выбранного региона.
        Exception: Ошибка драйвера при подключении или выполнении SQL.
            Исключение передаётся вызывающему коду без частичного результата.

    Notes:
        Агрегации, LIMIT и ORDER BY выполняются отдельно для каждого чанка.
        Объединение не пересчитывает общий итог и не удаляет дубликаты.
        При прерывании выдача новых чанков прекращается; уже запущенные
        запросы завершаются перед закрытием соединений.
        Все параметры после end_date передаются только по имени.

    Examples:
        >>> query = ("SELECT * FROM your_table "
        ...          "WHERE created_at >= {start_date} "
        ...          "AND created_at < {end_date}")
        >>> df = extract(query, "2026-05-01", "2026-05-18",
        ...              conn="THEATRE_ASIA", chunk_type="day", workers=6)
        >>> df = extract(query, "2026-05-01", "2026-06-01",
        ...              conn="THEATRE_RU", env_file="D:/settings/theatre.env")
    """
    if not isinstance(query, str):
        raise TypeError("query должен быть строкой SQL-запроса.")

    conn = _as_connection(conn)

    start = _as_datetime(start_date, "start_date")
    end = _as_datetime(end_date, "end_date")
    if (start.utcoffset() is None) != (end.utcoffset() is None):
        raise ValueError("Обе даты должны быть с часовым поясом или обе без него.")

    request = ExtractionRequest(
        query=query,
        start_date=start,
        end_date=end,
        chunk_type=_as_chunk_type(chunk_type),
    )
    execution_config = ExecutionConfig(workers=workers)
    config = _load_config(env_file, conn)
    hosts = THEATRE_CONNECTIONS[conn][0]
    configs = cycle(replace(config, host=host) for host in hosts)

    # Существующий механизм очереди, выполнения и объединения не меняется.
    return ParallelExtractor(
        connection_factory=lambda: ClickHouseConnection(next(configs)),
        execution_config=execution_config,
    ).extract(request)


def _load_config(
    env_file: str | PathLike[str] | None, conn: str = "THEATRE_RU"
) -> DatabaseConfig:
    hosts, port, database, region = THEATRE_CONNECTIONS[_as_connection(conn)]
    file_values = (
        dotenv_values(Path(env_file).expanduser(), encoding="utf-8-sig")
        if env_file is not None
        else {}
    )

    def credential(name: str) -> str:
        value = os.environ.get(name, file_values.get(name))
        if value is None or not value.strip():
            raise ValueError(
                f"Переменная {name} не задана. Укажите её в окружении или в .env "
                "в рабочей папке; другой путь можно передать через env_file."
            )
        return value

    return DatabaseConfig(
        host=hosts[0],
        port=port,
        database=database,
        username=credential(f"THEATRE_LOGIN_{region}"),
        password=credential(f"THEATRE_PASS_{region}"),
    )


def _as_datetime(value: str | date | datetime, name: str) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, time.min)
    if isinstance(value, str):
        normalized = value.strip()
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(normalized)
        except ValueError as error:
            raise ValueError(
                f"{name}: укажите дату в формате ISO, например 2026-05-01 "
                "или 2026-05-01T10:30:00."
            ) from error
    raise TypeError(f"{name} должен быть строкой ISO, date или datetime.")


def _as_chunk_type(value: str | TimeChunkType) -> TimeChunkType:
    if isinstance(value, TimeChunkType):
        return value
    if isinstance(value, str):
        try:
            return TimeChunkType(value.strip().lower())
        except ValueError:
            pass
    choices = ", ".join(kind.value for kind in TimeChunkType)
    raise ValueError(f"Неизвестный chunk_type. Допустимые значения: {choices}.")
