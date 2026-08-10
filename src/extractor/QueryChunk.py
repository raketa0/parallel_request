from dataclasses import dataclass

from chunk.TimeChunk import TimeChunk
from query.QueryParameters import QueryParameters

@dataclass(frozen=True)
class QueryChunk:
    chunk: TimeChunk
    query: str
    parameters: QueryParameters