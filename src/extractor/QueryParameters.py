from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class QueryParameters:
    start_date: datetime
    end_date: datetime

    def __post_init__(self) -> None:

        if self.start_date >= self.end_date:
            raise ValueError("start_date должен быть меньше end_date.")

    def to_dict(self) -> dict[str, datetime]:
        return {
            "start_date": self.start_date,
            "end_date": self.end_date,
        }