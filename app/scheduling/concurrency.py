import asyncio
import uuid
from collections import defaultdict
from datetime import datetime, timedelta

from app.persistence.adapters import TaskStorage
from app.persistence.adapters.my_queue import QueueStorage


class ConcurrencyController:
    def __init__(self, task_storage: TaskStorage, queue_storage: QueueStorage):
        self.locks = defaultdict(asyncio.Lock)  # type: ignore
        self.task = task_storage  # type: ignore
        self.queue = queue_storage  # type: ignore
        self.pending_tasks = defaultdict(set)  # type: ignore

    async def acquire_slot(self, queue_name: str, task_id: str, timeout: int = 300) -> bool:
        """带队列等待的槽位获取"""
        async with self.locks[queue_name]:
            # 检查当前并发数
            current = await self.task.get_concurrency(queue_name)
            config = await self.queue.load(queue_name)

            if current < config.max_concurrent:
                await self._acquire_immediate(task_id)
                return True

            # 进入等待队列
            wait_event = asyncio.Event()
            self.pending_tasks[queue_name].add((task_id, wait_event))

            try:
                await asyncio.wait_for(wait_event.wait(), timeout=timeout)
                return await self._retry_acquire(queue_name, task_id)
            except asyncio.TimeoutError:
                await self._handle_timeout(queue_name, task_id)
                return False

    async def _acquire_immediate(self, task_id: str):
        """立即获取槽位"""
        await self.task.update_task_status(task_id, "running")

    async def _retry_acquire(self, queue_name: str, task_id: str) -> bool:
        """重试获取槽位"""
        async with self.locks[queue_name]:
            current = await self.task.get_concurrency(queue_name)
            config = await self.queue.load(queue_name)

            if current < config.max_concurrent:
                await self._acquire_immediate(task_id)
                return True
            return False

    async def release_slot(self, queue_name: str, task_id: str):
        """释放槽位并唤醒等待任务"""
        async with self.locks[queue_name]:
            await self.task.update_task_status(task_id, "released")

            # 唤醒下一个等待任务
            if self.pending_tasks[queue_name]:
                _next_task_id, event = self.pending_tasks[queue_name].pop()
                event.set()

    async def _handle_timeout(self, queue_name: str, task_id: str):
        """处理获取超时"""
        await self.task.update_task_status(task_id, "timeout")
        self.pending_tasks[queue_name].discard(task_id)
        await self._schedule_retry(task_id)

    async def _schedule_retry(self, task_id: str):
        """安排指数退避重试"""
        retries = await self.task.get_retry_count(task_id)
        delay = min(2 ** retries, 300)  # 最大等待5分钟

        await self.task.record_retry_attempt(
            task_id,
            scheduled_time=datetime.now() + timedelta(seconds=delay)
        )


