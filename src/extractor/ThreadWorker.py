from datetime import datetime

from database_connector.DatabaseConnection import DatabaseConnection
from extractor.QueryChunk import QueryChunk
from extractor.ResultQueue import ResultQueue


class ThreadWorker:
    def __init__(self, connection: DatabaseConnection, result_queue: ResultQueue) -> None:
        self.connection = connection
        self.result_queue = result_queue

    def execute(self, query_chunks: list[QueryChunk]) -> None:

        try:
            self.connection.connect()

            for query_chunk in query_chunks:
                self._execute_chunk(
                    query_chunk,
                )

        finally:
            self.connection.close()

    def _execute_chunk(self, query_chunk: QueryChunk) -> None:
        start_time = datetime.now()

        data = self.connection.execute(
            query=query_chunk.query,
            params=query_chunk.parameters.to_dict(),
        )

        duration = datetime.now() - start_time

        print(
            f"Chunk #{query_chunk.chunk.number} "
            f"завершён за {duration}. "
            f"Строк: {len(data)}"
        )

        self.result_queue.put(data)