from datetime import datetime
import pandas as pd
from chunk.TimeChunkType import TimeChunkType
from database_connector.ClickHouseConnection import ClickHouseConnection
from database_connector.DatabaseConfig import DatabaseConfig
from database_connector.MSSQLConnection import MSSQLConnection
from extractor.ExecutionConfig import ExecutionConfig
from extractor.ParallelExtractor import ParallelExtractor
from request.ExtractionRequest import ExtractionRequest

class DataExtractor:

    def __init__(
        self,
        query: str,
        start_date: datetime,
        end_date: datetime,
        chunk_type: TimeChunkType,
        cpu_cores: int = 4,
        threads_per_core: int = 2,
        database_type: str = "mssql",
    ) -> None:

        self.query = query
        self.start_date = start_date
        self.end_date = end_date
        self.chunk_type = chunk_type
        self.database_type = database_type.lower()

        self.execution_config = ExecutionConfig(
            cpu_cores=cpu_cores,
            threads_per_core=threads_per_core,
        )

        self.database_config = (
            DatabaseConfig.from_env(
                self.database_type
            )
        )

        self.parallel_extractor = (
            ParallelExtractor(
                connection_factory=(
                    self._create_connection
                ),
                execution_config=(
                    self.execution_config
                ),
            )
        )

    def extract(self) -> pd.DataFrame:

        request = ExtractionRequest(
            query=self.query,
            start_date=self.start_date,
            end_date=self.end_date,
            chunk_type=self.chunk_type,
        )

        return self.parallel_extractor.extract(
            request
        )

    def _create_connection(self):

        match self.database_type:

            case "mssql":

                return MSSQLConnection(
                    self.database_config
                )

            case "clickhouse":

                return ClickHouseConnection(
                    self.database_config
                )

            case _:

                raise ValueError(
                    f"Неизвестный тип базы данных: "
                    f"{self.database_type}"
                )