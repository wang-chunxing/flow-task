import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, List
from uuid import UUID

from app.builders.workflow_builder import WorkflowBuilder
from app.models.enum import ScheduleType, TaskStatus
from app.models.models import (
    SchedulerPolicy,
    Task,
    TaskQueue,
    Workflow,  # 确保导入Workflow类型
)


class TaskBuilder:
    def __init__(self, name: str):
        self.task_id: UUID = uuid.uuid4()
        self.name: str = name
        self._scheduler_type: str = "interval"
        self._scheduler_args: Dict[str, Any] = {}
        self._queue: TaskQueue | None = None  # 修正类型注解
        self._workflow: Workflow | None = None
        self.version: int = 0  # 提供默认值
        self._success_handlers: List[Callable] = []
        self._failure_handlers: List[Callable] = []

    def set_scheduler(self, scheduler_type: str, scheduler_args: Dict[str, Any]) -> "TaskBuilder":
        self._scheduler_type = scheduler_type
        self._scheduler_args = scheduler_args
        return self

    def set_queue(self, queue: TaskQueue) -> "TaskBuilder":
        self._queue = queue
        return self

    def build_workflow(self, workflow_builder: WorkflowBuilder) -> "TaskBuilder":
        self._workflow = workflow_builder.build()
        return self

    def on_success(self, *handlers: Callable) -> "TaskBuilder":
        self._success_handlers.extend(handlers)
        return self

    def on_failure(self, *handlers: Callable) -> "TaskBuilder":
        self._failure_handlers.extend(handlers)
        return self

    def build(self) -> Task:
        # 参数验证
        if not self._queue:
            raise ValueError("Task queue must be specified")
        if not self._workflow:
            raise ValueError("Workflow must be built")

        return Task(
            id=self.task_id,
            name=self.name,
            scheduler_type=ScheduleType(self._scheduler_type),
            scheduler_config=SchedulerPolicy(
                scheduler_type=self._scheduler_type,
                timeout=self._scheduler_args.get("timeout", 300),
                retry_interval=self._scheduler_args.get("retry_interval", 60),
                max_retries=self._scheduler_args.get("retries", 3),
                cron_expression=self._scheduler_args["cron_expression"]
                    if "cron_expression" in self._scheduler_args else None
            ),
            queue_name=self._queue.queue_name,  # 根据实际模型字段名调整
            workflow=self._workflow,
            version=self.version,
            status=TaskStatus.PENDING,
            success_handlers=self._success_handlers,
            failure_handlers=self._failure_handlers
        )


@dataclass
class Defaults:
    TASK_QUEUE = TaskQueue(
        max_concurrency=10,
        queue_name="default",
        user_concurrency={"default": 1}
    )
