from extractor.QueryChunk import QueryChunk
from time_chunks.TimeChunk import TimeChunk


class ChunkDistributor:
    def distribute(self, chunks: list[TimeChunk], workers: int) -> list[list[TimeChunk]]:

        self._validate_workers(workers)

        if not chunks:
            return []

        workers = min(workers, len(chunks))

        distributed_chunks: list[list[TimeChunk]] = [
            []
            for _ in range(workers)
        ]

        for index, chunk in enumerate(chunks):

            worker_index = index % workers

            distributed_chunks[
                worker_index
            ].append(chunk)

        return distributed_chunks

    def distribute_queries(
        self,
        query_chunks: list[QueryChunk],
        workers: int,
    ) -> list[list[QueryChunk]]:

        self._validate_workers(workers)

        if not query_chunks:
            return []

        workers = min(workers, len(query_chunks))

        distributed_queries: list[list[QueryChunk]] = [
            []
            for _ in range(workers)
        ]

        for index, query_chunk in enumerate(
            query_chunks
        ):

            worker_index = index % workers

            distributed_queries[
                worker_index
            ].append(query_chunk)

        return distributed_queries

    @staticmethod
    def _validate_workers(workers: int) -> None:
        if workers <= 0:
            raise ValueError("Количество потоков должно быть больше 0.")
