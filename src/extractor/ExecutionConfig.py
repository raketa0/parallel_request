from dataclasses import dataclass


@dataclass(frozen=True)
class ExecutionConfig:
    cpu_cores: int = 4
    threads_per_core: int = 2

    @property
    def max_workers(self) -> int:
        return self.cpu_cores * self.threads_per_core

    def __post_init__(self) -> None:

        if self.cpu_cores <= 0:
            raise ValueError("Количество ядер должно быть больше 0.")

        if self.threads_per_core <= 0:
            raise ValueError("Количество потоков на ядро должно быть больше 0.")