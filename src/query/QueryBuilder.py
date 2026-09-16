from query.QueryParameters import QueryParameters
from time_chunks.TimeChunk import TimeChunk


class QueryBuilder:
    def build(self, query: str, chunk: TimeChunk) -> tuple[str, QueryParameters]:

        self._validate_query(query)

        parameters = QueryParameters(start_date=chunk.start_date, end_date=chunk.end_date)

        return query, parameters

    @staticmethod
    def _validate_query(query: str) -> None:

        if not query or not query.strip():
            raise ValueError("SQL-запрос не может быть пустым.")

        if "{start_date}" not in query:
            raise ValueError("SQL-запрос должен содержать параметр {start_date}.")

        if "{end_date}" not in query:
            raise ValueError("SQL-запрос должен содержать параметр {end_date}.")

        for parameter in ("{start_date}", "{end_date}"):
            if f"'{parameter}'" in query:
                raise ValueError(
                    f"Параметр {parameter} должен использоваться без кавычек."
                )
