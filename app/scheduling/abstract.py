import uuid
from abc import ABC, abstractmethod
from typing import Any


class TaskScheduler(ABC):
    @abstractmethod
    async def add_job(
        self,
        task_id: uuid.UUID,
        workflow: Any | None = None,
        trigger_type: str = "immediate",
        queue: str = "default",
        max_retries: int = 0,
        retry_interval: int = 60,
        timeout: int = 300,
        **kwargs
    ) -> None:
        pass

    @abstractmethod
    async def remove_job(self, job_id: str) -> bool:
        pass

    @abstractmethod
    async def pause_job(self, job_id: str) -> bool:
        pass

    @abstractmethod
    async def resume_job(self, job_id: str) -> bool:
        pass
