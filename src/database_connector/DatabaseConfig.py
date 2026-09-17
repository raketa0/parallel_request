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

THEATRE_ASIA_HOSTS = (
    "vm1-bfa-ch-sg2.lan-as.travelline.asia",
    "vm2-bfa-ch-sg2.lan-as.travelline.asia",
    "vm1-bfa-ch-sg.lan-as.travelline.asia",
)
THEATRE_ASIA_PORT = 8123
THEATRE_ASIA_DATABASE = "BookingFormAnalyticsV2"

THEATRE_CONNECTIONS = {
    "THEATRE_RU": (THEATRE_RU_HOSTS, THEATRE_RU_PORT, THEATRE_RU_DATABASE, "RU"),
    "THEATRE_ASIA": (THEATRE_ASIA_HOSTS, THEATRE_ASIA_PORT, THEATRE_ASIA_DATABASE, "ASIA"),
}


def normalize_connection(conn: str) -> str:
    if isinstance(conn, str):
        conn = conn.strip().upper()
        if conn in THEATRE_CONNECTIONS:
            return conn
    raise ValueError("Unknown conn. Expected THEATRE_RU or THEATRE_ASIA.")


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
    def from_env(cls, conn: str = "THEATRE_RU") -> "DatabaseConfig":
        """Прочитать настройки первого сервера выбранного THEATRE.

        Args:
            conn: "THEATRE_RU" (по умолчанию) или "THEATRE_ASIA".
                Регистр и пробелы по краям не важны.

        Returns:
            DatabaseConfig: Первый сервер региона, порт 8123, база
                BookingFormAnalyticsV2 и реквизиты из THEATRE_LOGIN_RU /
                THEATRE_PASS_RU либо THEATRE_LOGIN_ASIA / THEATRE_PASS_ASIA.

        Notes:
            Вызывает load_dotenv() с поиском .env по правилам python-dotenv.
            Уже заданные переменные окружения не заменяются. Соединение
            не открывается. Нужны только реквизиты выбранного региона.

        Raises:
            ValueError: Неизвестный conn или пустые/отсутствующие реквизиты.
        """

        hosts, port, database, region = THEATRE_CONNECTIONS[normalize_connection(conn)]
        load_dotenv()
        return cls(
            host=hosts[0],
            port=port,
            database=database,
            username=cls._get_required_env(f"THEATRE_LOGIN_{region}"),
            password=cls._get_required_env(f"THEATRE_PASS_{region}"),
        )

    @staticmethod
    def _get_required_env(name: str) -> str:
        value = os.getenv(name)
        if value is None or not value.strip():
            raise ValueError(f"Переменная окружения {name} не задана.")
        return value
