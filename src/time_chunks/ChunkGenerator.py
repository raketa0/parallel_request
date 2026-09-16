from datetime import datetime, timedelta

from time_chunks.TimeChunk import TimeChunk
from time_chunks.TimeChunkType import TimeChunkType


class ChunkGenerator:
    def generate(
        self,
        start_date: datetime,
        end_date: datetime,
        chunk_type: TimeChunkType,
    ) -> list[TimeChunk]:
        if start_date >= end_date:
            raise ValueError("start_date должен быть меньше end_date.")

        chunks: list[TimeChunk] = []
        current = start_date
        while current < end_date:
            chunk_end = min(self._next_boundary(current, chunk_type), end_date)
            chunks.append(
                TimeChunk(
                    number=len(chunks) + 1,
                    start_date=current,
                    end_date=chunk_end,
                )
            )
            current = chunk_end
        return chunks

    @staticmethod
    def _next_boundary(current: datetime, chunk_type: TimeChunkType) -> datetime:
        match chunk_type:
            case TimeChunkType.HOUR:
                return current + timedelta(hours=1)
            case TimeChunkType.DAY:
                return current + timedelta(days=1)
            case TimeChunkType.WEEK:
                return current + timedelta(days=7)
            case TimeChunkType.MONTH:
                next_month = current.month + 1
            case TimeChunkType.QUARTER:
                next_month = (current.month - 1) // 3 * 3 + 4
            case TimeChunkType.YEAR:
                next_month = 13
            case _:
                raise ValueError(f"Неизвестный тип временного разбиения: {chunk_type}")

        return datetime(
            year=current.year + (next_month - 1) // 12,
            month=(next_month - 1) % 12 + 1,
            day=1,
            tzinfo=current.tzinfo,
        )
