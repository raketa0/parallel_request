from abc import ABC, abstractmethod
import pandas as pd
from database_connector.DatabaseConfig import DatabaseConfig

class DatabaseConnection(ABC):
    def __init__(self, config: DatabaseConfig) -> None:
        self.config = config
        self.connection = None

    @abstractmethod
    def connect(self) -> None:
        pass

    @abstractmethod
    def execute(
        self,
        query: str,
        params: dict | None = None,
    ) -> pd.DataFrame:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    def __enter__(self) -> "DatabaseConnection":
        self.connect()
        return self

    def __exit__(
        self,
        exc_type,
        exc_value,
        traceback,
    ) -> None:
        self.close()