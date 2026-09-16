import sys
from datetime import timedelta
from queue import Empty, Queue
from threading import Event
from time import perf_counter

from database_connector.DatabaseConnection import DatabaseConnection
from extractor.QueryChunk import QueryChunk
from extractor.ResultQueue import ResultQueue


class ThreadWorker:
    def __init__(
        self,
        connection: DatabaseConnection,
        result_queue: ResultQueue,
        worker_id: int = 1,
    ) -> None:
        self.connection = connection
        self.result_queue = result_queue
        self.worker_label = (
            f"Worker #{worker_id} | "
            f"{getattr(connection, 'label', type(connection).__name__)}"
        )

    def execute(
        self,
        query_queue: Queue[QueryChunk],
        stop_event: Event,
    ) -> None:
        worker_error: Exception | None = None

        try:
            if stop_event.is_set():
                return
            try:
                self.connection.connect()
            except Exception as error:
                stop_event.set()
                self._print_error(
                    title="ОШИБКА ПОДКЛЮЧЕНИЯ К БАЗЕ",
                    error=error,
                )
                raise

            print(f"{self.worker_label} | подключён", flush=True)

            while not stop_event.is_set():
                try:
                    query_chunk = query_queue.get_nowait()
                except Empty:
                    break

                try:
                    if stop_event.is_set():
                        break
                    self._execute_chunk(query_chunk)
                except Exception:
                    stop_event.set()
                    raise
                finally:
                    query_queue.task_done()

        except Exception as error:
            worker_error = error
            raise

        finally:
            try:
                print(f"{self.worker_label} | закрываю соединение...", flush=True)
                self.connection.close()
            except Exception as error:
                stop_event.set()
                self._print_error(
                    title="ОШИБКА ЗАКРЫТИЯ СОЕДИНЕНИЯ",
                    error=error,
                )

                if worker_error is None:
                    raise
            else:
                print(f"{self.worker_label} | соединение закрыто, воркер завершён", flush=True)

    def _execute_chunk(self, query_chunk: QueryChunk) -> None:
        started_at = perf_counter()

        print(
            f"Chunk #{query_chunk.chunk.number} начат | {self.worker_label} | "
            f"Период: [{query_chunk.chunk.start_date}, {query_chunk.chunk.end_date})",
            flush=True,
        )

        try:
            data = self.connection.execute(
                query=query_chunk.query,
                params=query_chunk.parameters.to_dict(),
            )
            self.result_queue.put(data)

            duration = timedelta(seconds=perf_counter() - started_at)
            print(
                f"Chunk #{query_chunk.chunk.number} "
                f"завершён за {duration}. "
                f"Строк: {len(data)} | {self.worker_label}",
                flush=True,
            )

        except Exception as error:
            duration = timedelta(seconds=perf_counter() - started_at)
            self._print_error(
                title=f"ОШИБКА В CHUNK #{query_chunk.chunk.number}",
                error=error,
                details=(
                    f"Период: {query_chunk.chunk.start_date} — "
                    f"{query_chunk.chunk.end_date}\n"
                    f"До ошибки прошло: {duration}"
                ),
            )
            raise

    def _print_error(
        self,
        title: str,
        error: Exception,
        details: str | None = None,
    ) -> None:
        message_parts = [
            "\n" + "!" * 80,
            f"!!! {title} !!!",
            self.worker_label,
        ]

        if details:
            message_parts.append(details)

        message_parts.extend(
            [
                f"{type(error).__name__}: {error}",
                "!" * 80,
            ]
        )

        print("\n".join(message_parts), file=sys.stderr, flush=True)
