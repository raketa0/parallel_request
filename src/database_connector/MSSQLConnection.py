import pandas as pd
import pyodbc
from database_connector.DatabaseConfig import DatabaseConfig
from database_connector.DatabaseConnection import DatabaseConnection


class MSSQLConnection(DatabaseConnection):
    def __init__(self, config: DatabaseConfig) -> None:
        super().__init__(config)

    def connect(self) -> None:
        if self.connection is not None:
            return

        connection_string = (
            f"DRIVER={{{self.config.driver}}};"
            f"SERVER={self.config.host},{self.config.port};"
            f"DATABASE={self.config.database};"
            f"UID={self.config.username};"
            f"PWD={self.config.password};"
            f"TrustServerCertificate=yes;"
        )

        self.connection = pyodbc.connect(
            connection_string,
            timeout=self.config.connection_timeout,
        )

        self.connection.timeout = self.config.query_timeout

    def execute(
        self,
        query: str,
        params: dict | None = None,
    ) -> pd.DataFrame:

        if self.connection is None:
            self.connect()

        if params:
            return pd.read_sql_query(
                query,
                self.connection,
                params=params,
            )

        return pd.read_sql_query(
            query,
            self.connection,
        )

    def close(self) -> None:
        if self.connection is not None:
            self.connection.close()
            self.connection = None