import asyncio
import importlib
import time
import uuid
from datetime import timedelta
from textwrap import dedent
from typing import Any, Callable, Dict, List, Type

from temporalio import activity, workflow
from temporalio.activity import info as activity_info
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio.workflow import execute_activity

from app.models.models import Operator, Task


class WorkflowExecutionContext:
    def __init__(self):
        self.results = {}

    def store_result(self, op_name: str, result: Any):
        self.results[op_name] = result

    def get_result(self, op_name: str) -> Any:
        return self.results.get(op_name)

class WorkflowTranslator:
    def __init__(self):
        self._activity_registry = {}

    def translate(self, task: Task) -> Any:
        """生成符合 Temporal 1.8+ 规范的工作流类"""
        class_name = f"DynamicWorkflow_{task.name}_{uuid.uuid4().hex}"
        dependencies = self._process_dependencies(task)

        # 准备正确的全局命名空间
        global_namespace = {
            "workflow": workflow,  # 关键：必须包含完整的 workflow 模块
            "asyncio": asyncio,
            "timedelta": timedelta,
            "RetryPolicy": RetryPolicy,
            "ApplicationError": ApplicationError,
            "execute_activity": execute_activity,
            "WorkflowExecutionContext": WorkflowExecutionContext,
            "__import__": __import__,
            "Any": Any
        }

        # 注册活动函数
        for layer in task.workflow.execution_layers:
            for op in layer:
                activity_name = f"op_{op.name}"
                if activity_name not in self._activity_registry:
                    self._activity_registry[activity_name] = self._create_activity(op)
                global_namespace[activity_name] = self._activity_registry[activity_name]

        # 生成完全合规的类模板
        class_template = dedent(f'''
            from temporalio import workflow
            from typing import Any

            @workflow.defn(name="{class_name}")  # 正确使用装饰器
            class {class_name}:  # 不要继承任何基类
                _dependencies = {repr(dependencies)}

                @workflow.run  # 直接应用装饰器
                async def run(self, input_data: Any) -> Any:
                    ctx = WorkflowExecutionContext()
                    layers = {repr(task.workflow.execution_layers)}
                    return await self._execute_layers(layers, input_data, ctx)

                async def _execute_layers(self, layers: list, input_data: Any, ctx: WorkflowExecutionContext) -> Any:
                    current_data = input_data
                    for layer in layers:
                        futures = [
                            self._execute_operator(op, current_data, ctx)
                            for op in layer
                        ]
                        results = await asyncio.gather(*futures)
                        current_data = self._merge_results(results)
                    return current_data

                async def _execute_operator(self, operator: Any, input_data: Any, ctx: WorkflowExecutionContext) -> Any:
                    # 依赖检查
                    for dep in self._dependencies.get(operator.name, []):
                        if ctx.get_result(dep) is None:
                            raise ApplicationError(f"Missing dependency {{dep}}")

                    # 获取活动函数
                    activity_name = f"op_{{operator.name}}"
                    activity_func = globals().get(activity_name)
                    if not activity_func:
                        raise ApplicationError(f"Activity {{activity_name}} not registered")

                    # 执行活动
                    result = await execute_activity(
                        activity_func,
                        args=[input_data],
                        start_to_close_timeout=timedelta(seconds=int({task.scheduler_config.timeout})),
                        task_queue="{task.queue_name or 'default'}",
                        retry_policy=RetryPolicy(
                            maximum_attempts=int({task.scheduler_config.max_retries}),
                            initial_interval=timedelta(seconds=10)
                        )
                    )
                    ctx.store_result(operator.name, result)
                    return result

                @staticmethod
                def _merge_results(results: list) -> Any:
                    if not results:
                        return None
                    if len(results) == 1:
                        return results[0]
                    if all(isinstance(r, dict) for r in results):
                        merged = dict()
                        for r in results:
                            merged.update(r)
                        return merged
                    return results
        ''')

        # 执行代码生成
        exec(class_template, global_namespace)
        workflow_class = global_namespace[class_name]

        # 验证装饰器属性
        if not hasattr(workflow_class.run, "__temporal_workflow_run"):
            raise TypeError("生成的工作流类未通过 Temporal 装饰器验证")

        return workflow_class
    # 以下保持不变...
    def _process_dependencies(self, task: Task) -> Dict[str, List[str]]:
        dependencies = {}
        for stage in task.workflow.stages:
            for op_name, deps in stage.dependencies.items():
                dependencies.setdefault(op_name, []).extend(deps)
                dependencies[op_name] = list(set(dependencies[op_name]))
        return dependencies

    def _create_activity(self, operator: Operator) -> Callable:
        if operator.operator_type == "function":
            return self._create_function_activity(operator)
        elif operator.operator_type == "async_api":
            return self._create_api_activity(operator)
        else:
            raise ValueError(f"Unsupported operator type: {operator.operator_type}")

    def _create_function_activity(self, operator: Operator) -> Callable:
        @activity.defn(name=f"op_{operator.name}")
        async def wrapper(input_data: Any) -> Any:
            try:
                module = importlib.import_module(operator.spec["module"])
                func = getattr(module, operator.spec["func_name"])
                return await func(input_data) if asyncio.iscoroutinefunction(func) else func(input_data)
            except Exception as e:
                raise ApplicationError(f"Function execution failed: {str(e)}") from e

        return wrapper

    def _create_api_activity(self, operator: Operator) -> Callable:
        @activity.defn(name=f"op_{operator.name}")
        async def wrapper(input_data: Any) -> Any:
            activity_info()
            import aiohttp

            async def poll_task(session: aiohttp.ClientSession, task_id: str):
                start = time.monotonic()
                while True:
                    if time.monotonic() - start > operator.total_timeout:
                        raise ApplicationError("Async task timeout")

                    activity.heartbeat({"task_id": task_id})
                    try:
                        async with session.get(
                                operator.format_sync_url(task_id),
                                headers=operator.sync_config.get("headers", {})
                        ) as resp:
                            resp.raise_for_status()
                            data = await resp.json()

                            status = operator.extract_status(data)
                            if status in operator.sync_config.get("success_status", []):
                                return operator.parse_response(data)
                            if status in operator.sync_config.get("failure_status", []):
                                raise ApplicationError(f"Task failed with status: {status}")

                            await asyncio.sleep(operator.poll_interval)
                    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                        activity.logger.warning(f"Polling error: {str(e)}")
                        await asyncio.sleep(operator.poll_interval * 2)

            async with aiohttp.ClientSession() as session:
                try:
                    async with session.request(
                            method=operator.run_config.get("method", "POST"),
                            url=operator.run_config["url"],
                            json=input_data,
                            headers=operator.run_config.get("headers", {}),
                            timeout=operator.run_config.get("timeout", 30)
                    ) as resp:
                        resp.raise_for_status()
                        response = await resp.json()

                        if operator.requires_async_polling():
                            task_id = operator.extract_task_id(response)
                            return await poll_task(session, task_id)
                        return operator.parse_response(response)
                except aiohttp.ClientError as e:
                    raise ApplicationError(f"API request failed: {str(e)}") from e

        return wrapper