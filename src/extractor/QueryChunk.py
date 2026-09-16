from dataclasses import dataclass

from query.QueryParameters import QueryParameters
from time_chunks.TimeChunk import TimeChunk


@dataclass(frozen=True)
class QueryChunk:
    chunk: TimeChunk
    query: str
    parameters: QueryParameters
