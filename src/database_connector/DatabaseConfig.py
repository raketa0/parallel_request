import os
from dataclasses import dataclass
from dotenv import load_dotenv


@dataclass(frozen=True)
class DatabaseConfig:
    database_type: str
    host: str
    port: int
    database: str
    username: str
    password: str
    connection_timeout: int = 30
    query_timeout: int = 300
    driver: str | None = None

    @classmethod
    def from_env(cls, database_type: str) -> "DatabaseConfig":
        load_dotenv()

        database_type = database_type.lower()

        if database_type == "mssql":
            return cls._from_mssql_env()

        if database_type == "clickhouse":
            return cls._from_clickhouse_env()

        raise ValueError( f"Неизвестный тип базы данных: {database_type}")

    @classmethod
    def _from_mssql_env(cls) -> "DatabaseConfig":
        return cls(
            database_type="mssql",
            host=cls._get_required_env("MSSQL_HOST"),
            port=cls._get_int_env("MSSQL_PORT", 1433),
            database=cls._get_required_env("MSSQL_DATABASE"),
            username=cls._get_required_env("MSSQL_USERNAME"),
            password=cls._get_required_env("MSSQL_PASSWORD"),
            connection_timeout=cls._get_int_env("MSSQL_CONNECTION_TIMEOUT", 30),
            query_timeout=cls._get_int_env("MSSQL_QUERY_TIMEOUT", 300),
            driver=os.getenv("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server"),
        )

    @classmethod
    def _from_clickhouse_env(cls) -> "DatabaseConfig":
        return cls(
            database_type="clickhouse",
            host=cls._get_required_env("CLICKHOUSE_HOST"),
            port=cls._get_int_env("CLICKHOUSE_PORT", 8123),
            database=cls._get_required_env("CLICKHOUSE_DATABASE"),
            username=cls._get_required_env("CLICKHOUSE_USERNAME"),
            password=cls._get_required_env("CLICKHOUSE_PASSWORD"),
            connection_timeout=cls._get_int_env("CLICKHOUSE_CONNECTION_TIMEOUT", 30),
            query_timeout=cls._get_int_env("CLICKHOUSE_QUERY_TIMEOUT", 300)
        )

    @staticmethod
    def _get_required_env(name: str) -> str:
        value = os.getenv(name)

        if value is None or not value.strip():
            raise ValueError(f"Переменная окружения {name} не задана.")
        return value

    @staticmethod
    def _get_int_env(name: str, default: int) -> int:
        value = os.getenv(name)

        if value is None or not value.strip():
            return default

        try:
            result = int(value)
        except ValueError:
            raise ValueError(
                f"Переменная окружения {name} "
                f"должна содержать целое число."
            )

        if result <= 0:
            raise ValueError(
                f"Переменная окружения {name} "
                f"должна быть больше 0."
            )

        return result