import os
from dataclasses import dataclass

from dotenv import load_dotenv


THEATRE_RU_HOSTS = (
    "sv1-bfa-ch.travelline.wan",
    "mv1-bfa-ch.travelline.wan",
    "mv2-bfa-ch.travelline.wan",
)
THEATRE_RU_PORT = 8123
THEATRE_RU_DATABASE = "BookingFormAnalyticsV2"


@dataclass(frozen=True)
class DatabaseConfig:
    host: str
    port: int
    database: str
    username: str
    password: str
    connection_timeout: int = 30
    query_timeout: int = 300
    secure: bool = False
    verify: bool = True

    @classmethod
    def from_env(cls) -> "DatabaseConfig":
        load_dotenv()
        return cls(
            host=THEATRE_RU_HOSTS[0],
            port=THEATRE_RU_PORT,
            database=THEATRE_RU_DATABASE,
            username=cls._get_required_env("THEATRE_LOGIN_RU"),
            password=cls._get_required_env("THEATRE_PASS_RU"),
        )

    @staticmethod
    def _get_required_env(name: str) -> str:
        value = os.getenv(name)
        if value is None or not value.strip():
            raise ValueError(f"Переменная окружения {name} не задана.")
        return value
