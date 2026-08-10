from datetime import datetime

from chunk.ChunkGenerator import ChunkGenerator
from chunk.TimeChunkType import TimeChunkType
from request.ExtractionRequest import ExtractionRequest


request = ExtractionRequest(
    query="""
        SELECT *
        FROM booking
        WHERE booking_date >= :start_date
          AND booking_date < :end_date
    """,
    date_column="booking_date",
    start_date=datetime(2025, 1, 1),
    end_date=datetime(2025, 4, 1),
    chunk_type=TimeChunkType.MONTH,
    workers=4,
)


generator = ChunkGenerator()

chunks = generator.generate(
    start_date=request.start_date,
    end_date=request.end_date,
    chunk_type=request.chunk_type,
)


print(f"Количество частей: {len(chunks)}")
print()


for chunk in chunks:
    print(
        f"Chunk #{chunk.number}: "
        f"{chunk.start_date} -> {chunk.end_date}"
    )