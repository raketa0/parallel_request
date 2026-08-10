from datetime import datetime, timedelta

from chunk.TimeChunk import TimeChunk
from chunk.TimeChunkType import TimeChunkType


class ChunkGenerator:
    def generate(self, start_date: datetime, end_date: datetime, chunk_type: TimeChunkType) -> list[TimeChunk]:
        self._validate_dates(start_date, end_date)

        match chunk_type:
            case TimeChunkType.YEAR:
                return self._generate_year_chunks(
                    start_date,
                    end_date,
                )
            case TimeChunkType.QUARTER:
                return self._generate_quarter_chunks(
                    start_date,
                    end_date,
                )
            case TimeChunkType.MONTH:
                return self._generate_month_chunks(
                    start_date,
                    end_date,
                )
            case TimeChunkType.DAY:
                return self._generate_day_chunks(
                    start_date,
                    end_date,
                )
            case TimeChunkType.HOUR:
                return self._generate_hour_chunks(
                    start_date,
                    end_date,
                )
            case _:
                raise ValueError(f"Неизвестный тип временного разбиения: {chunk_type}")

    @staticmethod
    def _validate_dates(start_date: datetime, end_date: datetime) -> None:
        
        if start_date >= end_date:
            raise ValueError("start_date должен быть меньше end_date.")

    @staticmethod
    def _generate_year_chunks(start_date: datetime,end_date: datetime) -> list[TimeChunk]:
        chunks: list[TimeChunk] = []

        current = start_date
        number = 1

        while current < end_date:

            next_date = datetime(
                year=current.year + 1,
                month=1,
                day=1,
                tzinfo=current.tzinfo,
            )

            chunk_end = min(
                next_date,
                end_date,
            )

            chunks.append(
                TimeChunk(
                    number=number,
                    start_date=current,
                    end_date=chunk_end,
                )
            )

            current = chunk_end
            number += 1

        return chunks

    @staticmethod
    def _generate_quarter_chunks(start_date: datetime,end_date: datetime) -> list[TimeChunk]:

        chunks: list[TimeChunk] = []

        current = start_date
        number = 1

        while current < end_date:
            current_quarter = (current.month - 1) // 3

            next_quarter_month = (current_quarter * 3 + 4)

            if next_quarter_month > 12:
                next_date = datetime(year=current.year + 1, month=1, day=1, tzinfo=current.tzinfo,)
            else:
                next_date = datetime(year=current.year, month=next_quarter_month, day=1, tzinfo=current.tzinfo,
                )

            chunk_end = min(next_date, end_date)

            chunks.append(TimeChunk(number=number, start_date=current, end_date=chunk_end))

            current = chunk_end
            number += 1

        return chunks

    @staticmethod
    def _generate_month_chunks(start_date: datetime, end_date: datetime) -> list[TimeChunk]:
        chunks: list[TimeChunk] = []

        current = start_date
        number = 1

        while current < end_date:
            if current.month == 12:
                next_date = datetime(year=current.year + 1, month=1, day=1, tzinfo=current.tzinfo)
            else:
                next_date = datetime(year=current.year, month=current.month + 1, day=1, tzinfo=current.tzinfo)

            chunk_end = min(next_date, end_date)

            chunks.append(TimeChunk(number=number, start_date=current, end_date=chunk_end))

            current = chunk_end
            number += 1

        return chunks

    @staticmethod
    def _generate_day_chunks(start_date: datetime, end_date: datetime) -> list[TimeChunk]:
        chunks: list[TimeChunk] = []

        current = start_date
        number = 1

        while current < end_date:
            next_date = current + timedelta(days=1)

            chunk_end = min(next_date, end_date)

            chunks.append(TimeChunk(number=number, start_date=current, end_date=chunk_end))

            current = chunk_end
            number += 1

        return chunks

    @staticmethod
    def _generate_hour_chunks(start_date: datetime, end_date: datetime) -> list[TimeChunk]:
        chunks: list[TimeChunk] = []

        current = start_date
        number = 1

        while current < end_date:
            next_date = current + timedelta(hours=1)

            chunk_end = min(next_date, end_date)

            chunks.append(TimeChunk(number=number, start_date=current, end_date=chunk_end))

            current = chunk_end
            number += 1

        return chunks