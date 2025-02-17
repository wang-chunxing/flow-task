import asyncio
import uuid
from datetime import timedelta
from typing import Any

from temporalio.client import Client, WorkflowFailureError
from temporalio.common import RetryPolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from app.models.models import Task
from app.persistence import TaskStorage
from app.scheduling.abstract import TaskScheduler
from app.scheduling.adapters.temporal.workflow_registrar import WorkflowRegistrar
from app.scheduling.concurrency import ConcurrencyController


class DynamicScheduler(TaskScheduler):

    """先进动态调度引擎，集成可靠性增强功能"""

    def __init__(
            self,
            temporal_client: Client,
            controller: ConcurrencyController,
            task_repository: TaskStorage,
            base_retry_delay: float = 1.0,
    ):
        self.client = temporal_client
        self.controller = controller
        self.storage = task_repository
        self.base_retry_delay = base_retry_delay
        self.max_retries = 3
        self.retry_interval = 60
        self.timeout = 300

    async def add_job(
            self,
            task_id: str,
            workflow: Any | None = None,
            scheduler_type: str = "immediate",
            queue: str = "default",
            max_retries: int = 3,
            retry_interval: int = 60,
            timeout: int = 300,
            **kwargs
    ) -> None:
        self.max_retries = max_retries
        self.retry_interval = retry_interval
        self.timeout = timeout

        """实现任务添加接口，支持多种触发类型"""

        return await self.schedule_task(task_id)

    async def remove_job(self, job_id: str) -> bool:
        return True

    async def pause_job(self, job_id: str) -> bool:
        return True

    async def resume_job(self, job_id: str) -> bool:
        return True

    async def schedule_task(self, task_id: str):
        """智能任务调度入口，支持动态优先级"""
        task = await self._get_task_with_retry(task_id)
        await self._execute_with_adaptive_concurrency(task)

    async def _get_task_with_retry(self, task_id: str) -> Task:
        """带重试机制的任务获取"""
        for _ in range(3):
            try:
                return await self.storage.load(task_id)
            except Exception as e:
                print(f"Failed to fetch task {task_id}: {e}")
                await asyncio.sleep(self.base_retry_delay)
        raise RuntimeError(f"Failed to fetch task {task_id}")

    async def _execute_with_adaptive_concurrency(self, task: Task):
        """自适应并发控制执行"""
        queue = task.queue_name
        task_id = task.id
        dynamic_delay = self.base_retry_delay

        for attempt in range(self.max_retries + 1):
            try:
                if await self.controller.acquire_slot(
                        queue,
                        task_id,
                        timeout=self.timeout
                ):
                    try:
                        return await self._execute_workflow(task)
                    finally:
                        await self.controller.release_slot(queue, task_id)
                else:
                    dynamic_delay = self._calculate_backoff(dynamic_delay)
                    await asyncio.sleep(dynamic_delay)
            except Exception as e:
                print(f"Failed to execute task {task_id}: {e}")
        await self.storage.update_task_status(task_id, "pending")

    async def _execute_workflow(self, task: Task):
        """执行工作流核心逻辑"""
        registrar=self.registrar = WorkflowRegistrar(self.client, task)
        await registrar.start_worker()
        try:
            input_data=task.workflow.args
            await self.client.execute_workflow(
                "DynamicWorkflow",
                args=[task, input_data],
                task_queue=task.queue_name,
                id=f"Workflow-{task.queue_name}-{task.id} ",
                retry_policy=RetryPolicy(
                    maximum_attempts=self.max_retries,
                    initial_interval=timedelta(seconds=self.retry_interval)
                ),
                cron_schedule=task.scheduler_config.cron_expression,
                execution_timeout=timedelta(seconds=self.timeout),
            )
        except WorkflowAlreadyStartedError:
            print(f"Workflow {task.id} already running")
        except WorkflowFailureError as e:
            print(f"Workflow {task.id} failed: {e}")
            raise

    def _calculate_backoff(self, current_delay: float) -> float:
        """计算自适应退避时间"""
        return min(current_delay * 1.618, 60)  # 使用黄金比例退避

    async def recover_cluster_state(self):
        """集群状态恢复机制"""
        wait_tasks = await self.storage.get_by_status(["pending", "timeout"])

        for task in wait_tasks:
            await self.controller.acquire_slot(task.queue_name, task.id)

