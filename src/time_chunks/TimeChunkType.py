from enum import Enum


class TimeChunkType(str, Enum):
    YEAR = "year"
    MONTH = "month"
    WEEK = "week"
    DAY = "day"
    QUARTER = "quarter"
    HOUR = "hour"
