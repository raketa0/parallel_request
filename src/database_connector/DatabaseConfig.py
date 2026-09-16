import os
from dataclasses import dataclass
from dotenv import load_dotenv


THEATRE_RU_HOST = "mv1-bfa-ch.travelline.wan"
THEATRE_RU_HOSTS = (
    "sv1-bfa-ch.travelline.wan",
    "mv1-bfa-ch.travelline.wan",
    "mv2-bfa-ch.travelline.wan",
)
THEATRE_RU_PORT = 8123
THEATRE_RU_DATABASE = "BookingFormAnalyticsV2"


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
    encrypt: bool = True
    trust_server_certificate: bool = False
    secure: bool = False
    verify: bool = True

    @classmethod
    def from_env(cls, database_type: str) -> "DatabaseConfig":
        load_dotenv()

        database_type = database_type.strip().lower()

        if database_type == "mssql":
            return cls._from_mssql_env()

        if database_type == "clickhouse":
            return cls._from_clickhouse_env()

        raise ValueError(f"Неизвестный тип базы данных: {database_type}")

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
            encrypt=cls._get_bool_env("MSSQL_ENCRYPT", True),
            trust_server_certificate=cls._get_bool_env(
                "MSSQL_TRUST_SERVER_CERTIFICATE",
                False,
            ),
        )

    @classmethod
    def _from_clickhouse_env(cls) -> "DatabaseConfig":
        return cls(
            database_type="clickhouse",
            host=THEATRE_RU_HOST,
            port=THEATRE_RU_PORT,
            database=THEATRE_RU_DATABASE,
            username=cls._get_required_env("THEATRE_LOGIN_RU"),
            password=cls._get_required_env("THEATRE_PASS_RU"),
            connection_timeout=30,
            query_timeout=300,
            secure=False,
            verify=True,
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

    @staticmethod
    def _get_bool_env(name: str, default: bool) -> bool:
        value = os.getenv(name)

        if value is None or not value.strip():
            return default

        normalized_value = value.strip().lower()

        if normalized_value in {"1", "true", "yes", "on"}:
            return True

        if normalized_value in {"0", "false", "no", "off"}:
            return False

        raise ValueError(
            f"Переменная окружения {name} должна содержать "
            "true/false, yes/no, on/off или 1/0."
        )
