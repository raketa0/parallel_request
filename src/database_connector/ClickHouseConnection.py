import pandas as pd
import clickhouse_connect
from database_connector.DatabaseConfig import DatabaseConfig
from database_connector.DatabaseConnection import DatabaseConnection

class ClickHouseConnection(DatabaseConnection):
    def __init__(self, config: DatabaseConfig) -> None:
        super().__init__(config)

    def connect(self) -> None:
        if self.connection is not None:
            return

        self.connection = clickhouse_connect.get_client(
            host=self.config.host,
            port=self.config.port,
            username=self.config.username,
            password=self.config.password,
            database=self.config.database,
            connect_timeout=self.config.connection_timeout,
            send_receive_timeout=self.config.query_timeout,
        )

    def execute(
        self,
        query: str,
        params: dict | None = None,
    ) -> pd.DataFrame:

        if self.connection is None:
            self.connect()

        if params:
            return self.connection.query_df(
                query,
                parameters=params,
            )

        return self.connection.query_df(
            query,
        )

    def close(self) -> None:
        if self.connection is not None:
            self.connection.close()
            self.connection = None