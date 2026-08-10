from dataclasses import dataclass
from datetime import datetime

from chunk.TimeChunkType import TimeChunkType


@dataclass(frozen=True)
class ExtractionRequest:
    query: str
    date_column: str
    start_date: datetime
    end_date: datetime
    chunk_type: TimeChunkType

    def __post_init__(self) -> None:

        if not self.query or not self.query.strip():
            raise ValueError(
                "SQL-запрос не может быть пустым."
            )

        if not self.date_column or not self.date_column.strip():
            raise ValueError(
                "date_column не может быть пустым."
            )

        if self.start_date >= self.end_date:
            raise ValueError(
                "start_date должен быть меньше end_date."
            )