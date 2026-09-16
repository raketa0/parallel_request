from collections.abc import Sequence
from dataclasses import replace
from datetime import datetime
from itertools import cycle

import pandas as pd

from database_connector.ClickHouseConnection import ClickHouseConnection
from database_connector.DatabaseConfig import DatabaseConfig, THEATRE_RU_HOSTS
from extractor.ExecutionConfig import ExecutionConfig
from extractor.ParallelExtractor import ParallelExtractor
from request.ExtractionRequest import ExtractionRequest
from time_chunks.TimeChunkType import TimeChunkType


class DataExtractor:

    def __init__(
        self,
        query: str,
        start_date: datetime,
        end_date: datetime,
        chunk_type: TimeChunkType,
        workers: int = 8,
        database_type: str = "clickhouse",
        clickhouse_hosts: Sequence[str] | None = None,
    ) -> None:

        self.query = query
        self.start_date = start_date
        self.end_date = end_date
        self.chunk_type = chunk_type
        # Аргумент сохранён для существующих вызовов из main.py.
        if database_type.strip().lower() != "clickhouse":
            raise ValueError("Поддерживается только ClickHouse THEATRE_RU.")

        self.execution_config = ExecutionConfig(workers=workers)
        hosts = self._validate_clickhouse_hosts(clickhouse_hosts)
        self.database_config = DatabaseConfig.from_env()
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
        """Возвращает результаты всех интервалов одним DataFrame в памяти."""
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
