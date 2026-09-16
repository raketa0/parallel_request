import re

import pandas as pd

try:
    import pyodbc
except ModuleNotFoundError:  # Позволяет использовать только ClickHouse без pyodbc.
    pyodbc = None

from database_connector.DatabaseConfig import DatabaseConfig
from database_connector.DatabaseConnection import DatabaseConnection


_QUERY_PARAMETER = re.compile(r"\{(start_date|end_date)\}")


class MSSQLConnection(DatabaseConnection):
    def __init__(self, config: DatabaseConfig) -> None:
        super().__init__(config)

    def connect(self) -> None:
        if self.connection is not None:
            return

        if pyodbc is None:
            raise RuntimeError(
                "Для подключения к MSSQL установите зависимость pyodbc: "
                "python -m pip install pyodbc"
            )

        connection_string = (
            f"DRIVER={self._escape_connection_value(self.config.driver or '')};"
            f"SERVER={self._escape_connection_value(f'{self.config.host},{self.config.port}')};"
            f"DATABASE={self._escape_connection_value(self.config.database)};"
            f"UID={self._escape_connection_value(self.config.username)};"
            f"PWD={self._escape_connection_value(self.config.password)};"
            f"Encrypt={self._yes_no(self.config.encrypt)};"
            "TrustServerCertificate="
            f"{self._yes_no(self.config.trust_server_certificate)};"
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
            prepared_query, positional_params = self._prepare_query(
                query=query,
                params=params,
            )

            return pd.read_sql_query(
                prepared_query,
                self.connection,
                params=positional_params,
            )

        return pd.read_sql_query(
            query,
            self.connection,
        )

    def close(self) -> None:
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    @staticmethod
    def _prepare_query(query: str, params: dict) -> tuple[str, tuple]:
        positional_params = []

        def replace_parameter(match: re.Match) -> str:
            parameter_name = match.group(1)
            positional_params.append(params[parameter_name])
            return "?"

        prepared_query = _QUERY_PARAMETER.sub(replace_parameter, query)
        return prepared_query, tuple(positional_params)

    @staticmethod
    def _escape_connection_value(value: str) -> str:
        # Значение в фигурных скобках может содержать ';'. Закрывающая скобка
        # экранируется удвоением согласно синтаксису ODBC connection string.
        return "{" + value.replace("}", "}}") + "}"

    @staticmethod
    def _yes_no(value: bool) -> str:
        return "yes" if value else "no"
