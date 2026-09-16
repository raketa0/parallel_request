import importlib.util
import json
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("benchmark_runner", ROOT / "benchmarks/run_benchmarks.py")
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class LengthOnlyResult:
    def __len__(self):
        return 123

    def __iter__(self):
        raise AssertionError("Result contents must not be inspected")


class BenchmarkTests(unittest.TestCase):
    def test_preserved_historical_logs_are_consistent(self):
        history = json.loads((ROOT / "benchmarks/historical.json").read_text(encoding="utf-8"))
        self.assertEqual(len(history["runs"]), 7)
        self.assertEqual(sum(history["daily_rows"]), 281310)
        self.assertEqual([r["range_seconds"] for r in history["runs"]],
                         [353.19, 324.13, 306.94, 273.57, 268.03, 93.97, 52.57])
        for run in history["runs"]:
            self.assertEqual(sum(run.get("chunk_rows", history["daily_rows"])), run["rows"])
        archive = history["other_period_runs"][0]
        self.assertNotEqual(archive["period"], history["period"])
        self.assertIsNone(archive["range_seconds"])
        self.assertEqual(len(archive["durations"]), 14)

    def test_every_case_covers_only_fixed_period_without_gaps(self):
        for case_id, kind, workers, hosts in runner.CASES:
            with self.subTest(case=case_id):
                chunks = runner.ChunkGenerator().generate(runner.START, runner.END, runner.TimeChunkType[kind])
                self.assertEqual(chunks[0].start_date.isoformat(), "2026-05-01T00:00:00")
                self.assertEqual(chunks[-1].end_date.isoformat(), "2026-05-17T00:00:00")
                for first, second in zip(chunks, chunks[1:]):
                    self.assertEqual(first.end_date, second.start_date)
                self.assertLessEqual(workers, 6)
                self.assertTrue(set(hosts).issubset(runner.THEATRE_RU_HOSTS))

    def test_query_is_read_without_importing_main_or_loading_dotenv(self):
        with patch("dotenv.load_dotenv", side_effect=AssertionError("Forbidden .env access")):
            query = runner.load_query()
        self.assertIn("{start_date}", query)
        self.assertIn("{end_date}", query)
        self.assertIn("BookingFormAnalyticsV2.EventRaw", query)

    def test_success_records_only_length_and_metadata(self):
        with patch.object(runner.ParallelExtractor, "extract", return_value=LengthOnlyResult()):
            result = runner.run_case(runner.CASES[0], "SELECT fixture", None, 1)
        self.assertEqual(result["rows"], 123)
        self.assertEqual(result["status"], "success")
        self.assertFalse(result["csv_written"])
        self.assertGreaterEqual(result["range_seconds"], 0)
        self.assertNotIn("data", result)

    def test_error_text_is_not_saved(self):
        with patch.object(runner.ParallelExtractor, "extract", side_effect=RuntimeError("sensitive driver text")):
            result = runner.run_case(runner.CASES[0], "SELECT fixture", None, 1)
        self.assertEqual(result["status"], "error")
        self.assertEqual(result["error_type"], "RuntimeError")
        self.assertNotIn("sensitive", str(result))
        self.assertIsNone(result["range_seconds"])


if __name__ == "__main__":
    unittest.main()
