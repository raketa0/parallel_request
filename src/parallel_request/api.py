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
    THEATRE_RU_DATABASE,
    THEATRE_RU_HOSTS,
    THEATRE_RU_PORT,
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
    workers: int = 8,
    env_file: str | PathLike[str] | None = ".env",
) -> pd.DataFrame:
    """Выполняет запрос по временным интервалам и возвращает один DataFrame.

    query должен содержать {start_date} и {end_date} без кавычек.
    start_date включается в период, end_date не включается. Даты принимаются
    как строки ISO (например, "2026-05-01"), date или datetime.

    chunk_type: hour, day, week, month, quarter, year или TimeChunkType.
    workers: общий лимит параллельных запросов, по умолчанию 8.
    env_file: путь к .env относительно рабочей папки Jupyter или скрипта.
        По умолчанию читается .env в этой папке. None отключает чтение файла.
        Переменные окружения имеют приоритет над значениями из файла.

    Требуются THEATRE_LOGIN_RU и THEATRE_PASS_RU. Результат хранится в памяти;
    файлы не создаются. При ошибке исключение передаётся вызывающему коду.

    Пример:
        df = extract(query, "2026-05-01", "2026-05-17")
    """
    if not isinstance(query, str):
        raise TypeError("query должен быть строкой SQL-запроса.")

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
    config = _load_config(env_file)
    configs = cycle(replace(config, host=host) for host in THEATRE_RU_HOSTS)

    # Существующий механизм очереди, выполнения и объединения не меняется.
    return ParallelExtractor(
        connection_factory=lambda: ClickHouseConnection(next(configs)),
        execution_config=execution_config,
    ).extract(request)


def _load_config(env_file: str | PathLike[str] | None) -> DatabaseConfig:
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
        host=THEATRE_RU_HOSTS[0],
        port=THEATRE_RU_PORT,
        database=THEATRE_RU_DATABASE,
        username=credential("THEATRE_LOGIN_RU"),
        password=credential("THEATRE_PASS_RU"),
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
