import logging
import asyncio
from contextlib import asynccontextmanager
from app.builders import TaskBuilder, WorkflowBuilder
from app.core.containers.container import Container
from app.models.models import Queue

logger = logging.getLogger(__name__)


class AsyncUploadOperator:
    def __init__(self, container: Container):
        self.container = container

    async def build_rag_workflow(self, builder: WorkflowBuilder) -> WorkflowBuilder:
        # 注册异步API算子
        registry = self.container.services.operator_registry()
        await registry.register_api(
            name="async_parser",
            run_config={
                "url": "http://192.168.131.98:7861/v1/merlin/resources/upload",
                "method": "POST",
                "headers": {"X-Top-Account-Id": "{account_id}"},
                "task_id_path": "resource_id",
                "file_field": "file",
            },
            sync_config={
                "url": "http://192.168.131.98:7861/v1/merlin/resources/{resource_id}",
                "method": "GET",
                "terminal_statuses": ["SUCCEEDED", "FAILED"],
                "status_field": "status"
            },
            sync_policy="true",
            timeout=30,
            poll_interval=3
        )
        await registry.register_api(
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
                "status_field": "status"
            },
            timeout=30,
            poll_interval=3
        )
        return (
            builder
            .stage(name="document_load", description="文档加载")
            .add_operator(
                registry.get_operator("async_parser"),
                depends_on=[]
            )
            .stage(name="document_split", description="文档切分")
            .add_operator(
                registry.get_operator("async_split"),
                depends_on=["async_parser"],
            )
        )

    async def execute_workflow(self):
        workflow_builder = WorkflowBuilder()
        await self.build_rag_workflow(workflow_builder)
        engine = await self.container.services.task_engine()

        upload_task = (
            TaskBuilder("arg_doc_upload")
            .set_scheduler("immediate",{
                'timeout': 300,
                'retry_interval': 100,
                'max_retries': 3
            })
            .set_queue(Queue(
                max_concurrency=1,
                queue_name="default",
                user_concurrency={"default": 1}
            ))
            .build_workflow(workflow_builder)
            .on_success()
            .on_failure()
            .build()
        )

        result = await engine.submit_task(upload_task)
        logger.info(f"Task submitted: {result}")
        return result


@asynccontextmanager
async def init_app():
    """应用上下文管理器"""
    container = Container()
    try:
        # 初始化配置
        settings = container.settings()
        logger.info(f"Database URL: {settings.DATABASE.URL}")

        # 初始化数据库
        db = container.database.container.db()
        await db.init_db()
        logger.info("Database initialized")

        yield container
    finally:
        # 清理资源
        logger.info("Closing application resources...")



async def main():
    async with init_app() as container:
        operator = AsyncUploadOperator(container)
        result = await operator.execute_workflow()
        logger.info(f"Task execution result: {result}")
        return result


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application shutdown by user")