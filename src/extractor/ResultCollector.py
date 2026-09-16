import pandas as pd

from extractor.ResultQueue import ResultQueue


class ResultCollector:
    def __init__(self, result_queue: ResultQueue) -> None:
        self.result_queue = result_queue

    def collect(self, result_count: int) -> pd.DataFrame:
        dataframes: list[pd.DataFrame] = []

        for _ in range(result_count):
            data = self.result_queue.get()

            try:
                dataframes.append(data)

            finally:
                self.result_queue.task_done()

        if not dataframes:
            return pd.DataFrame()

        return pd.concat(
            dataframes,
            ignore_index=True,
        )
