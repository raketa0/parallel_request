from dataclasses import dataclass


@dataclass(frozen=True)
class ExecutionConfig:
    workers: int = 8

    def __post_init__(self) -> None:

        if (
            isinstance(self.workers, bool)
            or not isinstance(self.workers, int)
            or self.workers <= 0
        ):
            raise ValueError("Количество воркеров должно быть целым числом больше 0.")
