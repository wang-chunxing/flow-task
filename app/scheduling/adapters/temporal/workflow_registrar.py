import asyncio
from datetime import timedelta
from typing import Any, Callable, Dict, Type
import logging
from temporalio.client import Client
from temporalio.worker import Worker

from app.models.models import Task
from app.scheduling.adapters.temporal.workflow_define import DynamicWorkflow
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner, SandboxRestrictions, SandboxMatcher

DEFAULT_SANDBOX_MODULES = {
    # 标准库
    "json",
    "logging",
    "datetime",
    "http.client",
    "importlib",

    # 第三方库
    "requests",
    "urllib3",
    "aiohttp",
    "google.protobuf",
    "grpc",

    # Temporal 相关
    "temporalio",
    "temporalio.service",

    # 自定义模块
    "app.models"
}

class WorkflowRegistrar:
    """动态工作流注册管理器，支持多任务队列及资源隔离"""
    def __init__(
            self,
            client: Client,
            task: Task,
            default_worker_options: Dict | None = None
    ):
        self.client = client
        self.task = task
        self.default_worker_options = default_worker_options or {
            "max_concurrent_workflow_tasks": 100,
            "max_concurrent_activities": 100,
            "graceful_shutdown_timeout": 30
        }

    async def start_worker(self):

        workflow_instance = DynamicWorkflow()
        workflow_instance.task = self.task
        workflow_instance.load_activities()
        workflow_instance.load_dependencies()

        worker = Worker(
            self.client,
            task_queue=self.task.queue_name ,
            workflows=[DynamicWorkflow],
            activities=workflow_instance.__temporal_activities__,
            workflow_runner=SandboxedWorkflowRunner(
                restrictions=SandboxRestrictions(
                    passthrough_modules=DEFAULT_SANDBOX_MODULES,
                    invalid_modules=SandboxMatcher(),
                    invalid_module_members=SandboxMatcher()
                )
            ),
            max_concurrent_workflow_tasks = self.default_worker_options["max_concurrent_workflow_tasks"],
            max_concurrent_activities = self.default_worker_options["max_concurrent_activities"],
            graceful_shutdown_timeout=timedelta(self.default_worker_options["graceful_shutdown_timeout"])
        )

        # 创建并启动Worker
        asyncio.create_task(worker.run(), name=f"worker_{self.task.queue_name}")


    # 下面的方法暂时不用

    async def shutdown(self):
        """完全关闭Worker"""
        pass


    async def deregister(self, task_queue: str, remove_assets: bool = False):
        """注销任务队列"""
        pass

    async def health_check(self) -> Dict:
        """系统健康状态报告"""
        pass

    async def auto_scale_workers(self):
        """预留的自动扩展接口"""
        pass
