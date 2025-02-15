import asyncio
import inspect
import signal
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from typing import Any, Callable, Dict, Type

from temporalio.client import Client
from temporalio.worker import Worker


class WorkflowRegistrar:
    """动态工作流注册管理器，支持多任务队列及资源隔离"""

    def __init__(
            self,
            client: Client,
            default_worker_options: Dict | None = None
    ):
        self.client = client
        self.default_worker_options = default_worker_options or {
            "max_concurrent_workflow_tasks": 100,
            "max_concurrent_activities": 100,
            "graceful_shutdown_timeout": 30
        }

        # 注册表数据结构
        self.workflow_registry = defaultdict(set)  # type: ignore
        self.activity_registry = defaultdict(set)  # type: ignore

        # Worker运行时状态
        self.workers: Dict[str, Dict] = {}  # task_queue -> {worker, task, executor}

        # 信号处理
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(self.shutdown()))
        loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.create_task(self.shutdown()))

    async def register(
            self,
            workflow_cls: Any,
            override: bool = False

    ) -> str:
        """注册工作流定义并自动管理Worker"""
        # workflow_cls = translator.translate(task)
        task_queue = workflow_cls.__workflow_config__["task_queue"]
        activities = workflow_cls.__temporal_activities__

        if task_queue in self.workers and not override:
            raise Exception(f"Worker for queue {task_queue} already exists")

        # 更新注册表
        self.workflow_registry[task_queue].add(workflow_cls)
        self.activity_registry[task_queue].update(activities)

        # 重启Worker以应用变更
        await self._restart_worker(task_queue)
        return task_queue

    async def _restart_worker(self, task_queue: str):
        """安全重启Worker实例"""
        if task_queue in self.workers:
            await self._shutdown_worker(task_queue, f"Restarting worker for {task_queue}")

        if not self.workflow_registry[task_queue] and not self.activity_registry[task_queue]:
            return

        # 创建新的执行器实例
        max_activities = self.default_worker_options.get("max_concurrent_activities", 100)
        activity_executor = ThreadPoolExecutor(
            max_workers=max_activities,
            thread_name_prefix=f"temporal_act_{task_queue}_"
        )

        worker = Worker(
            self.client,
            task_queue=task_queue,
            workflows=list(self.workflow_registry[task_queue]),
            activities=list(self.activity_registry[task_queue]),
            activity_executor=activity_executor,
            max_concurrent_workflow_tasks=self.default_worker_options["max_concurrent_workflow_tasks"],
            max_concurrent_activities=self.default_worker_options["max_concurrent_activities"],
            graceful_shutdown_timeout=timedelta(self.default_worker_options["graceful_shutdown_timeout"])
        )

        # 创建并启动Worker
        task = asyncio.create_task(worker.run(), name=f"worker_{task_queue}")
        self.workers[task_queue] = {
            "worker": worker,
            "task": task,
            "executor": activity_executor
        }

    # 下面的方法暂时不用
    async def _shutdown_worker(self, task_queue: str, reason: str = ""):
        """安全停止指定Worker"""
        if worker_info := self.workers.pop(task_queue, None):
            print(f"Shutting down {task_queue} worker: {reason}")
            await worker_info["worker"].shutdown()
            worker_info["task"].cancel()
            worker_info["executor"].shutdown(wait=False)

    async def deregister(self, task_queue: str, remove_assets: bool = False):
        """注销整个任务队列"""
        if remove_assets:
            self.workflow_registry.pop(task_queue, None)
            self.activity_registry.pop(task_queue, None)
        await self._shutdown_worker(task_queue, f"Deregistering {task_queue}")

    async def shutdown(self):
        """完全关闭所有Worker"""
        print("\nInitiating graceful shutdown...")
        await asyncio.gather(*[
            self._shutdown_worker(tq, "Full shutdown")
            for tq in list(self.workers.keys())
        ])
        print("All workers terminated")

    async def health_check(self) -> Dict:
        """系统健康状态报告"""
        return {
            "total_queues": len(self.workers),
            "active_workers": [
                {
                    "task_queue": tq,
                    "workflows": len(self.workflow_registry[tq]),
                    "activities": len(self.activity_registry[tq]),
                    "running": not worker_info["task"].done()
                }
                for tq, worker_info in self.workers.items()
            ]
        }

    async def auto_scale_workers(self):
        """预留的自动扩展接口"""
        # 示例：基于队列深度的自动扩展逻辑
        # 实际实现需要与Temporal服务端API集成
        pass

    def register_workflow(self, workflow_cls: Type, task_queue: str):
        """直接注册工作流类到指定队列"""
        self.workflow_registry[task_queue].add(workflow_cls)
        asyncio.create_task(self._restart_worker(task_queue))

    def register_activity(self, activity_fn: Callable, task_queue: str):
        """注册活动函数到指定队列"""
        self.activity_registry[task_queue].add(activity_fn)
        asyncio.create_task(self._restart_worker(task_queue))

    def register_module(self, module: Any, task_queue: str):
        """自动注册模块中的工作流和活动"""
        for attr in dir(module):
            obj = getattr(module, attr)
            if inspect.isclass(obj) and hasattr(obj, "run"):
                self.register_workflow(obj, task_queue)
            elif inspect.isfunction(obj) and attr.startswith("activity_"):
                self.register_activity(obj, task_queue)
