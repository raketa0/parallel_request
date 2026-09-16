import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from threading import Event

import pandas as pd

from database_connector.DatabaseConnection import DatabaseConnection
from extractor.ExecutionConfig import ExecutionConfig
from extractor.QueryChunk import QueryChunk
from extractor.ResultCollector import ResultCollector
from extractor.ResultQueue import ResultQueue
from extractor.ThreadWorker import ThreadWorker
from query.QueryBuilder import QueryBuilder
from request.ExtractionRequest import ExtractionRequest
from time_chunks.ChunkGenerator import ChunkGenerator


class ParallelExtractor:
    def __init__(
        self,
        connection_factory,
        execution_config: ExecutionConfig | None = None,
        chunk_generator: ChunkGenerator | None = None,
        query_builder: QueryBuilder | None = None,
    ) -> None:

        self.connection_factory = connection_factory

        self.execution_config = (
            execution_config
            if execution_config is not None
            else ExecutionConfig()
        )

        self.chunk_generator = (
            chunk_generator
            if chunk_generator is not None
            else ChunkGenerator()
        )

        self.query_builder = (
            query_builder
            if query_builder is not None
            else QueryBuilder()
        )

    def extract(
        self,
        request: ExtractionRequest,
    ) -> pd.DataFrame:

        chunks = self.chunk_generator.generate(
            start_date=request.start_date,
            end_date=request.end_date,
            chunk_type=request.chunk_type,
        )

        if not chunks:
            return pd.DataFrame()

        query_chunks = self._build_query_chunks(
            query=request.query,
            chunks=chunks,
        )

        worker_count = min(
            self.execution_config.workers,
            len(query_chunks),
        )
        query_queue: Queue[QueryChunk] = Queue()
        result_queue = ResultQueue()
        stop_event = Event()

        for query_chunk in query_chunks:
            query_queue.put(query_chunk)

        print(
            f"Чанков: {len(query_chunks)}. Воркеров: {worker_count} "
            f"(общий лимит: {self.execution_config.workers}).",
            flush=True,
        )

        with ThreadPoolExecutor(
            max_workers=worker_count,
            thread_name_prefix="db-worker",
        ) as executor:
            futures = []

            try:
                for worker_index in range(worker_count):
                    connection: DatabaseConnection = self.connection_factory()
                    worker = ThreadWorker(
                        connection=connection,
                        result_queue=result_queue,
                        worker_id=worker_index + 1,
                    )
                    futures.append(
                        executor.submit(
                            worker.execute,
                            query_queue,
                            stop_event,
                        )
                    )

                for future in as_completed(futures):
                    future.result()

            except BaseException as error:
                stop_event.set()

                for future in futures:
                    future.cancel()

                if isinstance(error, KeyboardInterrupt):
                    print(
                        "\nПолучено прерывание (KeyboardInterrupt). Выдача новых чанков остановлена. "
                        "Ожидаю завершения уже запущенных запросов и закрытия соединений...",
                        file=sys.stderr,
                        flush=True,
                    )

                raise

        print(
            f"Все {worker_count} воркеров завершены. "
            f"Объединяю результаты {len(query_chunks)} чанков в DataFrame...",
            flush=True,
        )
        collector = ResultCollector(
            result_queue=result_queue,
        )

        return collector.collect(
            result_count=len(query_chunks),
        )

    def _build_query_chunks(
        self,
        query: str,
        chunks,
    ) -> list[QueryChunk]:

        query_chunks: list[QueryChunk] = []

        for chunk in chunks:

            sql, parameters = self.query_builder.build(
                query=query,
                chunk=chunk,
            )

            query_chunks.append(
                QueryChunk(
                    chunk=chunk,
                    query=sql,
                    parameters=parameters,
                )
            )

        return query_chunks
