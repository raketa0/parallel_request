from dataclasses import dataclass
from datetime import datetime

@dataclass(frozen=True)
class TimeChunk:
    number: int
    start_date: datetime
    end_date: datetime

    def __post_init__(self) -> None:
        if self.number < 1:
            raise ValueError("Номер временного куска должен быть больше 0.")

        if self.start_date >= self.end_date:
            raise ValueError("start_date должен быть меньше end_date.")
