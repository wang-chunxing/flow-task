import unittest
from unittest import IsolatedAsyncioTestCase  # 修改导入

from app.builders import TaskBuilder, WorkflowBuilder
from app.core.clients import logger
from app.core.containers.container import Container
from app.models.enum import ScheduleType
from app.models.models import TaskQueue


class TestAsyncUploadOperator(IsolatedAsyncioTestCase):  # 修改基类
    async def asyncSetUp(self):
        # 初始化核心容器

        self.container = Container()
        settings = self.container.config_module.container.settings()
        logger.info(f"database url: {settings.DATABASE_URL}")
        # 初始化数据库
        db = self.container.database_module.container.db()
        await db.init_db()
        logger.info("Database initialized")

    async def build_rag_workflow(self, builder: WorkflowBuilder) -> WorkflowBuilder:
        # 注册异步API算子

        await self.container.operator_registry.register_api(
            name="async_split",
            run_config={
                "url": "http://192.168.131.98:7861/v1/merlin/resources/upload",
                "method": "POST",
                "headers": {"X-Top-Account-Id": "{account_id}"},
                "id_field": "resource_id",
                "file_field": "file",
            },
            sync_config={
                "url": "http://192.168.131.98:7861/v1/merlin/resources/{resource_id}",
                "method": "GET",
                "terminal_statuses": ["SUCCEEDED", "FAILED"],
                "status_field": "status"  # 明确指定状态字段路径
            },
            timeout=30,
            poll_interval=3
        )
        return (
            builder
            .stage(name="document_load", description="文档加载")
                .add_operator(
                    self.container.operator_registry.get_operator("async_parser"),
                    depends_on=[]
                )
            .stage(name="document_split", description="文档切分")
                .add_operator(
                    self.container.operator_registry.get_operator("async_split"),
                    depends_on=["async_parser"],
                    sync_policy={"mode": "wait"}
                )
        )

    async def test_async_upload_workflow(self):
        workflow_builder = WorkflowBuilder()
        await self.build_rag_workflow(workflow_builder)
        engine = await self.container.services.task_engine()

        # 构建工作流
        upload_task = (
            TaskBuilder("arg_doc_upload")
            .set_scheduler(ScheduleType.IMMEDIATE, {
                'timeout': 300,
                'retry_interval': 100,
                'retries': 3
             })
            .set_queue(TaskQueue(
                max_concurrency=1,
                queue_name="default",
                user_concurrency={"default": 1}
            ))
            .build_workflow(workflow_builder)
            .on_success()
            .on_failure()
            .build()
        )

        # 提交任务并等待结果
        result = await engine.submit_task(upload_task)
        self.assertIsNotNone(result)  # 添加断言验证结果
        print("Task submitted:", result)


if __name__ == "__main__":
    unittest.main()

