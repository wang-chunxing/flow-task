import json
import logging
import asyncio
import sys
from contextlib import asynccontextmanager
from datetime import timedelta, datetime
from types import ModuleType

import numpy as np

from app.builders import TaskBuilder, WorkflowBuilder
from app.core.containers.container import Container
from app.models.models import Queue

logger = logging.getLogger(__name__)



def _create_complex_function():
    """创建包含多种功能的复杂测试函数模块"""
    module_name = "complex_operators_module"
    if module_name not in sys.modules:
        sys.modules[module_name] = ModuleType(module_name)

    module = sys.modules[module_name]

    # 复杂函数1：带类型验证和异常处理的处理函数
    def data_processor(input_data, multiplier=2, log_errors=True):
        """
        数据处理函数：
        - 支持多种输入类型（dict, list, int）
        - 异常处理机制
        - 默认参数使用
        """
        try:
            if isinstance(input_data, dict):
                return {k: v * multiplier for k, v in input_data.items()}
            elif isinstance(input_data, list):
                return [x * multiplier for x in input_data]
            elif isinstance(input_data, (int, float)):
                return input_data * multiplier
            else:
                raise ValueError("Unsupported input type")
        except Exception as e:
            if log_errors:
                print(f"Error processing data: {str(e)}")
            raise

    # 复杂函数2：包含第三方库使用的函数
    def calculate_bmi(weight_kg, height_cm):
        """BMI计算函数"""
        height_m = height_cm / 100
        return np.round(weight_kg / (height_m ** 2), 2)

    # 复杂函数3：时间序列处理函数
    def generate_time_series(start_date, days=7):
        """生成时间序列"""
        date_format = "%Y-%m-%d"
        start = datetime.strptime(start_date, date_format)
        return [{
            "date": (start + timedelta(days=i)).strftime(date_format),
            "value": i * 100
        } for i in range(days)]

    # 复杂函数4：带依赖注入的配置处理
    def configurable_processor(data, config_json):
        """可配置的数据处理器"""
        config = json.loads(config_json)
        if config.get("reverse") and isinstance(data, list):
            return data[::-1]
        return data

    # 将函数附加到模块
    for func in [data_processor, calculate_bmi, generate_time_series, configurable_processor]:
        func.__module__ = module_name
        setattr(module, func.__name__, func)

    return data_processor, calculate_bmi, generate_time_series, configurable_processor


class AsyncUploadOperator:
    def __init__(self, container: Container):
        self.container = container

    async def build_rag_workflow(self, builder: WorkflowBuilder) -> WorkflowBuilder:
        # 注册异步API算子
        registry = self.container.services.operator_registry()
        # data_processor, calculate_bmi, generate_series, config_processor = _create_complex_function()
        data_processor, _, _, _ = _create_complex_function()
        await registry.register_function("complex_processor", data_processor)
        # await registry.register_function("bmi_calculator", calculate_bmi)
        # await registry.register_function("time_series", generate_series)
        # await registry.register_function("config_processor", config_processor)
        # await registry.register_api(
        #     name="async_parser",
        #     run_config={
        #         "url": "http://192.168.131.98:7861/v1/merlin/resources",
        #         "method": "POST",
        #         "headers": {"Content-Type": "application/json"},
        #         "task_id_path": "data.id",
        #     },
        #     sync_config={
        #         "url": "http://192.168.131.98:7861/v1/merlin/resources/{id}",
        #         "method": "GET",
        #         "status_field": "data.status",
        #         "terminal_statuses": [0,10],
        #     },
        #     sync_policy="true",
        #     timeout=30,
        #     poll_interval=3
        # )
        # await registry.register_api(
        #     name="async_split",
        #     run_config={
        #         "url": "http://192.168.131.98:7861/v1/merlin/resources/upload",
        #         "method": "POST",
        #         "headers": {"X-Top-Account-Id": "{account_id}"},
        #         "id_field": "resource_id",
        #         "file_field": "file",
        #     },
        #     sync_config={
        #         "url": "http://192.168.131.98:7861/v1/merlin/resources/{resource_id}",
        #         "method": "GET",
        #         "terminal_statuses": ["SUCCEEDED", "FAILED"],
        #         "status_field": "status"
        #     },
        #     timeout=30,
        #     poll_interval=3
        # )
        return (
            builder.stage(name="document_load", description="文档加载")
            .add_operator(registry.get_operator("complex_processor"))
            .set_args({"a": 1, "b": 2})
            # .add_operator(
            #     registry.get_operator("async_parser"),
            #     depends_on=[]
            # )
            # .set_args(
            #     {
            #         "name": "docling",
            #         "extension": "md",
            #         "storage_url": "merlin/0194f3d1908576b398f033a4d4683621/text.md",
            #         "type": "md"
            #     }
            # )
            # .stage(name="document_split", description="文档切分")
            # .add_operator(
            #     registry.get_operator("async_split"),
            #     depends_on=["async_parser"],
            # )
            .build()
        )

    async def execute_workflow(self):
        workflow_builder = WorkflowBuilder()
        await self.build_rag_workflow(workflow_builder("临时测试工作流"))
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