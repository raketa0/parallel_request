import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from parallel_request.api import extract, _load_config
from database_connector.DatabaseConfig import DatabaseConfig, THEATRE_ASIA_HOSTS, THEATRE_RU_HOSTS
from DataExtractor import DataExtractor
from datetime import datetime
from time_chunks.TimeChunkType import TimeChunkType


class ConnectionTests(unittest.TestCase):
    def test_from_env_and_data_extractor_select_region(self):
        for region, hosts in [("RU", THEATRE_RU_HOSTS), ("ASIA", THEATRE_ASIA_HOSTS)]:
            with self.subTest(region=region), patch.dict(os.environ, {
                f"THEATRE_LOGIN_{region}": region, f"THEATRE_PASS_{region}": "secret"
            }, clear=True), patch("database_connector.DatabaseConfig.load_dotenv"):
                config = DatabaseConfig.from_env(conn=f" theatre_{region.lower()} ")
                self.assertEqual((config.host, config.username, config.password, config.port),
                                 (hosts[0], region, "secret", 8123))
                if region == "RU":
                    self.assertEqual(DatabaseConfig.from_env(), config)
                extractor = DataExtractor("SELECT {start_date}, {end_date}",
                    datetime(2026, 5, 1), datetime(2026, 5, 18), TimeChunkType.DAY,
                    conn=f"THEATRE_{region}")
                configs = [extractor._create_connection().config for _ in range(6)]
                self.assertEqual([c.host for c in configs], list(hosts) * 2)
                self.assertTrue(all(c.username == region for c in configs))
                custom = DataExtractor("SELECT {start_date}, {end_date}",
                    datetime(2026, 5, 1), datetime(2026, 5, 18), TimeChunkType.DAY,
                    conn=f"THEATRE_{region}", clickhouse_hosts=[hosts[1]])
                self.assertEqual(custom._create_connection().config.host, hosts[1])

    def test_from_env_rejects_invalid_connection_before_loading_env(self):
        with patch("database_connector.DatabaseConfig.load_dotenv") as load:
            with self.assertRaisesRegex(ValueError, "conn"):
                DatabaseConfig.from_env("unknown")
            load.assert_not_called()

    def test_from_env_requires_selected_credentials(self):
        with patch.dict(os.environ, {"THEATRE_LOGIN_RU": "ru", "THEATRE_PASS_RU": "secret"}, clear=True), patch("database_connector.DatabaseConfig.load_dotenv"):
            with self.assertRaisesRegex(ValueError, "THEATRE_LOGIN_ASIA"):
                DatabaseConfig.from_env("THEATRE_ASIA")

    def test_default_ru(self):
        with patch.dict(os.environ, {"THEATRE_LOGIN_RU": "ru", "THEATRE_PASS_RU": "secret"}, clear=True):
            config = _load_config(None)
        self.assertEqual((config.host, config.port, config.username), (THEATRE_RU_HOSTS[0], 8123, "ru"))

    def test_asia_file_and_environment_precedence(self):
        with tempfile.TemporaryDirectory() as directory:
            env = Path(directory) / ".env"
            env.write_text("THEATRE_LOGIN_ASIA=file_user\nTHEATRE_PASS_ASIA=file_password\n", encoding="utf-8-sig")
            with patch.dict(os.environ, {}, clear=True):
                config = _load_config(env, "THEATRE_ASIA")
                self.assertEqual(config.username, "file_user")
            with patch.dict(os.environ, {"THEATRE_LOGIN_ASIA": "env_user"}, clear=True):
                config = _load_config(env, "THEATRE_ASIA")
            self.assertEqual((config.host, config.port, config.username, config.password),
                             (THEATRE_ASIA_HOSTS[0], 8123, "env_user", "file_password"))

    def test_missing_asia_credentials_do_not_fall_back_to_ru(self):
        with patch.dict(os.environ, {"THEATRE_LOGIN_RU": "ru", "THEATRE_PASS_RU": "secret"}, clear=True):
            with self.assertRaisesRegex(ValueError, "THEATRE_LOGIN_ASIA"):
                _load_config(None, "THEATRE_ASIA")

    def test_invalid_connection(self):
        for conn in ["unknown", "", None, []]:
            with self.subTest(conn=conn), self.assertRaisesRegex(ValueError, "conn"):
                extract("SELECT {start_date}, {end_date}", "2026-05-01", "2026-05-18", conn=conn)

    def test_extract_routes_six_workers_to_selected_region(self):
        for conn, hosts, region in [("THEATRE_RU", THEATRE_RU_HOSTS, "RU"),
                                    (" theatre_asia ", THEATRE_ASIA_HOSTS, "ASIA")]:
            with self.subTest(conn=conn), patch.dict(os.environ, {
                f"THEATRE_LOGIN_{region}": region, f"THEATRE_PASS_{region}": "secret"
            }, clear=True), patch("parallel_request.api.ParallelExtractor") as extractor:
                result = extract("SELECT {start_date}, {end_date}", "2026-05-01", "2026-05-18",
                                 workers=6, conn=conn, env_file=None)
                self.assertIs(result, extractor.return_value.extract.return_value)
                factory = extractor.call_args.kwargs["connection_factory"]
                configs = [factory().config for _ in range(6)]
                self.assertEqual([c.host for c in configs], list(hosts) * 2)
                self.assertTrue(all(c.username == region and c.port == 8123 for c in configs))


if __name__ == "__main__":
    unittest.main()
