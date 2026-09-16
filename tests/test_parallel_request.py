import os
import sys
import threading
import time
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime
from io import StringIO
from pathlib import Path
from unittest.mock import Mock, patch

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))

import pandas as pd

from DataExtractor import DataExtractor
from database_connector.ClickHouseConnection import ClickHouseConnection
from database_connector.DatabaseConfig import DatabaseConfig, THEATRE_RU_HOSTS
from database_connector.MSSQLConnection import MSSQLConnection
from extractor.ExecutionConfig import ExecutionConfig
from extractor.ParallelExtractor import ParallelExtractor
from extractor.ResultCollector import ResultCollector
from extractor.ResultQueue import ResultQueue
from query.QueryBuilder import QueryBuilder
from request.ExtractionRequest import ExtractionRequest
from time_chunks.ChunkGenerator import ChunkGenerator
from time_chunks.TimeChunkType import TimeChunkType


class FakeConnection:
    lock = threading.Lock()
    active_connections = 0
    max_active_connections = 0
    next_connection_id = 1
    executed_by_day: dict[int, int] = {}

    def __init__(self, barrier: threading.Barrier) -> None:
        self.barrier = barrier
        self.connected = False

        with self.lock:
            self.connection_id = type(self).next_connection_id
            type(self).next_connection_id += 1

    def connect(self) -> None:
        self.connected = True

        with self.lock:
            type(self).active_connections += 1
            type(self).max_active_connections = max(
                type(self).max_active_connections,
                type(self).active_connections,
            )

        self.barrier.wait(timeout=2)

    def execute(self, query: str, params: dict | None = None) -> pd.DataFrame:
        assert self.connected
        assert params is not None
        day = params["start_date"].day

        with self.lock:
            type(self).executed_by_day[day] = self.connection_id

        # Первый чанк медленный. Освободившийся второй поток должен сам
        # забрать следующие дни из общей очереди.
        time.sleep(0.05 if day == 1 else 0.001)

        return pd.DataFrame(
            {
                "chunk_start": [params["start_date"]],
                "chunk_end": [params["end_date"]],
            }
        )

    def close(self) -> None:
        if not self.connected:
            return

        self.connected = False

        with self.lock:
            type(self).active_connections -= 1


class FailingConnection:
    label = "test-server:8123"

    def __init__(self) -> None:
        self.connected = False

    def connect(self) -> None:
        self.connected = True

    def execute(self, query: str, params: dict | None = None) -> pd.DataFrame:
        assert params is not None
        day = params["start_date"].day

        if day == 2:
            raise RuntimeError("server-side test error")

        return pd.DataFrame({"day": [day]})

    def close(self) -> None:
        self.connected = False


class ChunkGeneratorTests(unittest.TestCase):
    def test_week_chunks_are_seven_days_with_a_short_final_chunk(self) -> None:
        start = datetime(2026, 5, 1)
        end = datetime(2026, 6, 2)

        chunks = ChunkGenerator().generate(start, end, TimeChunkType.WEEK)

        self.assertEqual(len(chunks), 5)
        self.assertEqual(
            [(chunk.start_date, chunk.end_date) for chunk in chunks],
            [
                (datetime(2026, 5, 1), datetime(2026, 5, 8)),
                (datetime(2026, 5, 8), datetime(2026, 5, 15)),
                (datetime(2026, 5, 15), datetime(2026, 5, 22)),
                (datetime(2026, 5, 22), datetime(2026, 5, 29)),
                (datetime(2026, 5, 29), datetime(2026, 6, 2)),
            ],
        )

    def test_month_chunks_cover_the_range_without_gaps(self) -> None:
        start = datetime(2025, 12, 15, 10, 30)
        end = datetime(2026, 2, 10)

        chunks = ChunkGenerator().generate(start, end, TimeChunkType.MONTH)

        self.assertEqual(chunks[0].start_date, start)
        self.assertEqual(chunks[-1].end_date, end)
        self.assertEqual(
            [chunk.end_date for chunk in chunks[:-1]],
            [chunk.start_date for chunk in chunks[1:]],
        )


class QueryAdapterTests(unittest.TestCase):
    def test_query_builder_requires_both_boundaries(self) -> None:
        with self.assertRaises(ValueError):
            QueryBuilder().build(
                "SELECT * FROM t WHERE created_at >= {start_date}",
                ChunkGenerator().generate(
                    datetime(2026, 1, 1),
                    datetime(2026, 1, 2),
                    TimeChunkType.DAY,
                )[0],
            )

    def test_query_builder_rejects_quoted_parameters(self) -> None:
        chunk = ChunkGenerator().generate(
            datetime(2026, 1, 1),
            datetime(2026, 1, 2),
            TimeChunkType.DAY,
        )[0]

        with self.assertRaises(ValueError):
            QueryBuilder().build(
                "x >= '{start_date}' AND x < '{end_date}'",
                chunk,
            )

    def test_mssql_uses_positional_parameters_in_query_order(self) -> None:
        start = datetime(2026, 1, 1)
        end = datetime(2026, 1, 2)

        query, params = MSSQLConnection._prepare_query(
            "x >= {start_date} AND x < {end_date} OR y = {start_date}",
            {"start_date": start, "end_date": end},
        )

        self.assertEqual(query, "x >= ? AND x < ? OR y = ?")
        self.assertEqual(params, (start, end, start))

    def test_clickhouse_adds_server_side_parameter_types(self) -> None:
        query = ClickHouseConnection._prepare_query(
            "x >= {start_date} AND x < {end_date}"
        )

        self.assertEqual(
            query,
            "x >= {start_date:DateTime64(6)} AND x < {end_date:DateTime64(6)}",
        )


class DatabaseConfigTests(unittest.TestCase):
    def test_reads_theatre_ru_credentials(self) -> None:
        env = {
            "THEATRE_LOGIN_RU": "theatre_reader",
            "THEATRE_PASS_RU": "secret",
        }

        with (
            patch("database_connector.DatabaseConfig.load_dotenv"),
            patch.dict(os.environ, env, clear=True),
        ):
            config = DatabaseConfig.from_env("clickhouse")

        self.assertEqual(config.host, "mv1-bfa-ch.travelline.wan")
        self.assertEqual(config.port, 8123)
        self.assertEqual(config.database, "BookingFormAnalyticsV2")
        self.assertEqual(config.username, "theatre_reader")
        self.assertEqual(config.password, "secret")

    def test_reads_mssql_credentials_and_security_flags(self) -> None:
        env = {
            "MSSQL_HOST": "db.example",
            "MSSQL_DATABASE": "analytics",
            "MSSQL_USERNAME": "reader",
            "MSSQL_PASSWORD": "secret",
            "MSSQL_ENCRYPT": "yes",
            "MSSQL_TRUST_SERVER_CERTIFICATE": "no",
        }

        with (
            patch("database_connector.DatabaseConfig.load_dotenv"),
            patch.dict(os.environ, env, clear=True),
        ):
            config = DatabaseConfig.from_env(" MSSQL ")

        self.assertEqual(config.host, "db.example")
        self.assertEqual(config.port, 1433)
        self.assertTrue(config.encrypt)
        self.assertFalse(config.trust_server_certificate)

    def test_rejects_invalid_boolean(self) -> None:
        with patch.dict(os.environ, {"INVALID_BOOL": "sometimes"}, clear=True):
            with self.assertRaises(ValueError):
                DatabaseConfig._get_bool_env("INVALID_BOOL", False)


class ParallelExtractorTests(unittest.TestCase):
    def setUp(self) -> None:
        FakeConnection.active_connections = 0
        FakeConnection.max_active_connections = 0
        FakeConnection.next_connection_id = 1
        FakeConnection.executed_by_day = {}

    def test_free_worker_takes_next_chunk_without_sorting_results(self) -> None:
        barrier = threading.Barrier(2)
        extractor = ParallelExtractor(
            connection_factory=lambda: FakeConnection(barrier),
            execution_config=ExecutionConfig(workers=2),
        )
        request = ExtractionRequest(
            query=(
                "SELECT * FROM t "
                "WHERE created_at >= {start_date} AND created_at < {end_date}"
            ),
            start_date=datetime(2026, 1, 1),
            end_date=datetime(2026, 1, 5),
            chunk_type=TimeChunkType.DAY,
        )

        result = extractor.extract(request)

        self.assertEqual(FakeConnection.max_active_connections, 2)
        self.assertEqual(
            [value.day for value in result["chunk_start"].tolist()],
            [2, 3, 4, 1],
        )
        self.assertNotEqual(
            FakeConnection.executed_by_day[1],
            FakeConnection.executed_by_day[2],
        )
        self.assertEqual(
            FakeConnection.executed_by_day[2],
            FakeConnection.executed_by_day[3],
        )
        self.assertEqual(
            FakeConnection.executed_by_day[3],
            FakeConnection.executed_by_day[4],
        )
        self.assertEqual(FakeConnection.active_connections, 0)

    def test_collector_preserves_completion_order(self) -> None:
        result_queue = ResultQueue()
        result_queue.put(pd.DataFrame({"value": [2]}))
        result_queue.put(pd.DataFrame({"value": [1]}))

        result = ResultCollector(result_queue).collect(2)

        self.assertEqual(result["value"].tolist(), [2, 1])

    def test_keyboard_interrupt_stops_new_chunks_and_closes_connection(self) -> None:
        stop_signal = threading.Event()
        query_started = threading.Event()
        executed_days = []
        connection = Mock()
        connection.label = "test-server:8123"

        def execute(query, params):
            day = params["start_date"].day
            executed_days.append(day)
            if day == 1:
                query_started.set()
                # Без обработки KeyboardInterrupt очередь продолжит работу
                # после истечения этого короткого ожидания.
                stop_signal.wait(timeout=0.3)
            return pd.DataFrame({"day": [day]})

        def interrupt_wait(futures):
            if not query_started.wait(timeout=2):
                raise AssertionError("Первый запрос не запустился")
            raise KeyboardInterrupt

        connection.execute.side_effect = execute
        extractor = ParallelExtractor(
            connection_factory=lambda: connection,
            execution_config=ExecutionConfig(workers=1),
        )
        request = ExtractionRequest(
            query="SELECT * FROM t WHERE ts >= {start_date} AND ts < {end_date}",
            start_date=datetime(2026, 1, 1),
            end_date=datetime(2026, 1, 5),
            chunk_type=TimeChunkType.DAY,
        )

        with (
            patch("extractor.ParallelExtractor.Event", return_value=stop_signal),
            patch("extractor.ParallelExtractor.as_completed", side_effect=interrupt_wait),
            redirect_stdout(StringIO()),
            redirect_stderr(StringIO()),
        ):
            with self.assertRaises(KeyboardInterrupt):
                extractor.extract(request)

        self.assertTrue(stop_signal.is_set())
        self.assertEqual(executed_days, [1])
        connection.close.assert_called_once()

    def test_failed_chunk_is_highlighted_and_not_reported_as_completed(self) -> None:
        extractor = ParallelExtractor(
            connection_factory=FailingConnection,
            execution_config=ExecutionConfig(workers=1),
        )
        request = ExtractionRequest(
            query=(
                "SELECT * FROM t "
                "WHERE created_at >= {start_date} AND created_at < {end_date}"
            ),
            start_date=datetime(2026, 1, 1),
            end_date=datetime(2026, 1, 4),
            chunk_type=TimeChunkType.DAY,
        )
        stdout = StringIO()
        stderr = StringIO()

        with redirect_stdout(stdout), redirect_stderr(stderr):
            with self.assertRaisesRegex(RuntimeError, "server-side test error"):
                extractor.extract(request)

        self.assertIn("ОШИБКА В CHUNK #2", stderr.getvalue())
        self.assertIn("RuntimeError: server-side test error", stderr.getvalue())
        self.assertIn("Worker #1 | test-server:8123", stderr.getvalue())
        self.assertNotIn("Chunk #2 завершён", stdout.getvalue())
        self.assertNotIn("Chunk #3 завершён", stdout.getvalue())


class ExecutionConfigTests(unittest.TestCase):
    def test_rejects_invalid_worker_counts(self) -> None:
        for workers in (0, -1, True, False, 1.5, "3", None):
            with self.subTest(workers=workers):
                with self.assertRaisesRegex(ValueError, "Количество воркеров"):
                    ExecutionConfig(workers=workers)


class MultiServerTests(unittest.TestCase):
    def setUp(self) -> None:
        # Любая неожиданная попытка загрузить настоящий .env ломает тест.
        dotenv_guard = patch(
            "database_connector.DatabaseConfig.load_dotenv",
            side_effect=AssertionError("Тест не должен читать .env"),
        )
        dotenv_guard.start()
        self.addCleanup(dotenv_guard.stop)
        self.config = DatabaseConfig(
            database_type="clickhouse",
            host="unused.example",
            port=8123,
            database="BookingFormAnalyticsV2",
            username="test-reader",
            password="test-password",
        )

    def make_extractor(self, **options) -> DataExtractor:
        arguments = dict(
            query="SELECT * FROM t WHERE ts >= {start_date} AND ts < {end_date}",
            start_date=datetime(2026, 1, 1),
            end_date=datetime(2026, 1, 9),
            chunk_type=TimeChunkType.DAY,
            workers=3,
            database_type="clickhouse",
        )
        arguments.update(options)
        with patch("DataExtractor.DatabaseConfig.from_env", return_value=self.config):
            return DataExtractor(**arguments)

    def test_each_worker_gets_its_own_client_on_the_next_server(self) -> None:
        extractor = self.make_extractor(workers=6)
        connections = [extractor._create_connection() for _ in range(6)]

        self.assertEqual(
            [connection.config.host for connection in connections],
            list(THEATRE_RU_HOSTS) * 2,
        )
        self.assertEqual(len({id(connection) for connection in connections}), 6)
        for connection in connections:
            self.assertIsInstance(connection, ClickHouseConnection)
            self.assertEqual(connection.config.port, 8123)
            self.assertEqual(connection.config.database, self.config.database)
            self.assertEqual(connection.config.username, self.config.username)
            self.assertEqual(connection.config.password, self.config.password)
            self.assertNotIn(self.config.password, connection.label)

    def test_can_select_a_single_server(self) -> None:
        extractor = self.make_extractor(clickhouse_hosts=[" mv2-bfa-ch.travelline.wan "])
        self.assertEqual(
            [extractor._create_connection().config.host for _ in range(3)],
            ["mv2-bfa-ch.travelline.wan"] * 3,
        )

    def test_rejects_invalid_server_lists(self) -> None:
        for hosts in (
            [], "mv1-bfa-ch.travelline.wan", [""], [None],
            ["https://db.example"], ["db.example:8123"],
            ["user:password@db.example"], ["db example"],
            ["db.example", "DB.EXAMPLE"],
        ):
            with self.subTest(hosts=hosts):
                with self.assertRaises(ValueError):
                    self.make_extractor(clickhouse_hosts=hosts)

    def test_mssql_keeps_single_server_configuration(self) -> None:
        extractor = self.make_extractor(database_type="mssql")
        connection = extractor._create_connection()
        self.assertIsInstance(connection, MSSQLConnection)
        self.assertIs(connection.config, self.config)
        with self.assertRaisesRegex(ValueError, "только для ClickHouse"):
            self.make_extractor(database_type="mssql", clickhouse_hosts=["db.example"])

    def test_clickhouse_connect_uses_each_assigned_endpoint(self) -> None:
        extractor = self.make_extractor()
        connections = [extractor._create_connection() for _ in range(3)]
        with patch("database_connector.ClickHouseConnection.clickhouse_connect") as driver:
            driver.get_client.side_effect = [Mock(), Mock(), Mock()]
            for connection in connections:
                connection.connect()
            self.assertEqual(
                [call.kwargs["host"] for call in driver.get_client.call_args_list],
                list(THEATRE_RU_HOSTS),
            )
            for call in driver.get_client.call_args_list:
                self.assertEqual(call.kwargs["port"], 8123)
                self.assertEqual(call.kwargs["username"], self.config.username)
                self.assertEqual(call.kwargs["password"], self.config.password)
            clients = [connection.connection for connection in connections]
            for connection in connections:
                connection.close()
            for client in clients:
                client.close.assert_called_once_with()

    def test_three_servers_share_queue_without_duplicate_chunks(self) -> None:
        # Все три сервера начинают первый чанк. Один занят до тех пор,
        # пока два других не разберут оставшуюся очередь.
        first_queries = threading.Barrier(3)
        fast_finished = threading.Event()
        lock = threading.Lock()
        executed: list[tuple[int, str]] = []
        connections = []

        class RoutedConnection:
            def __init__(self, config) -> None:
                self.config = config
                self.label = f"{config.host}:{config.port}"
                self.connected = False
                self.first_query = True
                connections.append(self)

            def connect(self) -> None:
                self.connected = True

            def execute(self, query, params):
                assert self.connected
                day = params["start_date"].day
                if self.first_query:
                    self.first_query = False
                    first_queries.wait(timeout=3)
                with lock:
                    executed.append((day, self.config.host))
                if day == 1:
                    if not fast_finished.wait(timeout=3):
                        raise AssertionError("Свободные воркеры не взяли следующие чанки")
                elif day == 8:
                    fast_finished.set()
                return pd.DataFrame({"day": [day], "host": [self.config.host]})

            def close(self) -> None:
                self.connected = False

        extractor = self.make_extractor()
        output = StringIO()
        with (
            patch("DataExtractor.ClickHouseConnection", side_effect=RoutedConnection),
            redirect_stdout(output),
        ):
            result = extractor.extract()

        self.assertEqual(len(connections), 3)
        self.assertTrue(all(not connection.connected for connection in connections))
        self.assertEqual(sorted(result["day"].tolist()), list(range(1, 9)))
        self.assertEqual(sorted(day for day, host in executed), list(range(1, 9)))
        self.assertEqual({host for day, host in executed}, set(THEATRE_RU_HOSTS))
        slow_host = next(host for day, host in executed if day == 1)
        self.assertTrue(all(host != slow_host for day, host in executed if day >= 4))
        for host in THEATRE_RU_HOSTS:
            self.assertIn(f"{host}:8123", output.getvalue())
        self.assertNotIn(self.config.password, output.getvalue())

    def test_one_month_chunk_opens_only_one_connection(self) -> None:
        extractor = self.make_extractor(workers=6, chunk_type=TimeChunkType.MONTH)
        client = Mock()
        client.label = "sv1-bfa-ch.travelline.wan:8123"
        client.execute.return_value = pd.DataFrame({"value": [1]})
        with (
            patch("DataExtractor.ClickHouseConnection", return_value=client) as factory,
            redirect_stdout(StringIO()),
        ):
            result = extractor.extract()
        factory.assert_called_once()
        client.execute.assert_called_once()
        self.assertEqual(
            client.execute.call_args.kwargs["params"],
            {"start_date": datetime(2026, 1, 1), "end_date": datetime(2026, 1, 9)},
        )
        client.close.assert_called_once()
        self.assertEqual(result["value"].tolist(), [1])

    def test_connection_failure_identifies_server_and_propagates(self) -> None:
        extractor = self.make_extractor(workers=1)
        client = Mock()
        client.label = "sv1-bfa-ch.travelline.wan:8123"
        client.connect.side_effect = RuntimeError("test connection refused")
        errors = StringIO()
        with (
            patch("DataExtractor.ClickHouseConnection", return_value=client),
            redirect_stdout(StringIO()),
            redirect_stderr(errors),
        ):
            with self.assertRaisesRegex(RuntimeError, "test connection refused"):
                extractor.extract()
        self.assertIn("ОШИБКА ПОДКЛЮЧЕНИЯ К БАЗЕ", errors.getvalue())
        self.assertIn("Worker #1 | sv1-bfa-ch.travelline.wan:8123", errors.getvalue())
        client.execute.assert_not_called()
        client.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
