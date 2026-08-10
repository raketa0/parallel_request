from queue import Queue
import pandas as pd

class ResultQueue:
    def __init__(self) -> None:

        self._queue: Queue[pd.DataFrame] = Queue()

    def put(
        self,
        data: pd.DataFrame,
    ) -> None:

        self._queue.put(data)

    def get(self) -> pd.DataFrame:

        return self._queue.get()

    def task_done(self) -> None:

        self._queue.task_done()

    def join(self) -> None:

        self._queue.join()