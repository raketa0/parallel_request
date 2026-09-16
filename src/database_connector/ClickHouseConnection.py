import clickhouse_connect
import pandas as pd

from database_connector.DatabaseConfig import DatabaseConfig


class ClickHouseConnection:
    def __init__(self, config: DatabaseConfig) -> None:
        self.config = config
        self.connection = None

    @property
    def label(self) -> str:
        return f"{self.config.host}:{self.config.port}"

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
            secure=self.config.secure,
            verify=self.config.verify,
        )

    def execute(
        self,
        query: str,
        params: dict | None = None,
    ) -> pd.DataFrame:

        if self.connection is None:
            self.connect()

        if params:
            prepared_query = self._prepare_query(query)

            return self.connection.query_df(
                prepared_query,
                parameters=params,
            )

        return self.connection.query_df(
            query,
        )

    def close(self) -> None:
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def __enter__(self) -> "ClickHouseConnection":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    @staticmethod
    def _prepare_query(query: str) -> str:
        # ClickHouse server-side parameters требуют явного типа. DateTime64(6)
        # сохраняет микросекунды Python datetime и не вставляет значения в SQL.
        return (
            query.replace("{start_date}", "{start_date:DateTime64(6)}")
            .replace("{end_date}", "{end_date:DateTime64(6)}")
        )
