from enum import Enum

class TimeChunkType(str, Enum):
    YEAR = "year"
    MONTH = "month"
    DAY = "day"
    QUARTER = "quarter"
    HOUR = "hour"
