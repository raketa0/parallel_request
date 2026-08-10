from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from chunk.ChunkGenerator import ChunkGenerator
from database_connector.DatabaseConnection import DatabaseConnection
from extractor.ChunkDistributor import ChunkDistributor
from extractor.ExecutionConfig import ExecutionConfig
from extractor.QueryChunk import QueryChunk
from extractor.ResultCollector import ResultCollector
from extractor.ResultQueue import ResultQueue
from extractor.ThreadWorker import ThreadWorker
from query.QueryBuilder import QueryBuilder
from request.ExtractionRequest import ExtractionRequest


class ParallelExtractor:
    def __init__(
        self,
        connection_factory,
        execution_config: ExecutionConfig | None = None,
        chunk_generator: ChunkGenerator | None = None,
        query_builder: QueryBuilder | None = None,
        chunk_distributor: ChunkDistributor | None = None,
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

        self.chunk_distributor = (
            chunk_distributor
            if chunk_distributor is not None
            else ChunkDistributor()
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

        distributed_queries = (
            self.chunk_distributor.distribute_queries(
                query_chunks=query_chunks,
                workers=self.execution_config.max_workers,
            )
        )

        result_queue = ResultQueue()

        with ThreadPoolExecutor(
            max_workers=len(distributed_queries),
        ) as executor:

            futures = []

            for worker_queries in distributed_queries:

                connection: DatabaseConnection = (
                    self.connection_factory()
                )

                worker = ThreadWorker(
                    connection=connection,
                    result_queue=result_queue,
                )

                future = executor.submit(
                    worker.execute,
                    worker_queries,
                )

                futures.append(future)

            for future in futures:

                future.result()

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