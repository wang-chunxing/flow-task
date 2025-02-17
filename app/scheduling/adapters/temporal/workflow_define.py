import asyncio
import importlib
import time
from datetime import timedelta
from shutil import ExecError
from typing import Any, List, Callable
from temporalio import workflow, activity
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio.workflow import execute_activity
from temporalio.activity import info as activity_info
from app.models.models import Operator, Task


class WorkflowExecutionContext:
    """工作流执行上下文管理器"""

    def __init__(self):
        self.results = {}
        self.dependencies = {}

    def store_result(self, op_name: str, result: Any):
        self.results[op_name] = result

    def get_result(self, op_name: str) -> Any:
        return self.results.get(op_name)

# 定义工作流
@workflow.defn
class DynamicWorkflow:
    def __init__(self):
        self.__temporal_activities__ = []
        self.__temporal_activities_dependencies__ = {}
        self.task = None

    def get_dependencies(self):
        for stage in self.task.workflow.stages:
            for op_name, op_deps in stage.dependencies.items():
                if op_name in self.__temporal_activities_dependencies__:
                    existing = self.__temporal_activities_dependencies__[op_name]
                    merged = list(set(existing + op_deps))
                    self.__temporal_activities_dependencies__[op_name] = merged
                else:
                    self.__temporal_activities_dependencies__[op_name] = op_deps

            for op_name in stage.operators:
                if op_name not in self.__temporal_activities_dependencies__:
                    self.__temporal_activities_dependencies__[op_name] = []

    def get_activities(self):
        workflow_def = self.task.workflow
        activities = []
        for layer in workflow_def.execution_layers:
            for op in layer:
                activity_def = self._create_activity(op)
                activities.append(activity_def)
        self.__temporal_activities__ = activities
        print(f"Generated {len(activities)} activities:")
        for act in activities:
            meta = getattr(act, '__temporal_activity_definition__', {})
            print(f"- {meta.get('name')}")

    @workflow.run
    async def run(self, task: Task, input_data: dict) -> Any:  # 改为接收字典类型
        execution_ctx = WorkflowExecutionContext()
        self.task = task
        self.get_activities()
        self.get_dependencies()

        for op_name, op_deps in self.__temporal_activities_dependencies__.items():
            print(f"Operator: {op_name}, Dependencies: {op_deps}")

        for op in self.__temporal_activities__:
            print(f"Operator: {op}")

        return await self._execute_layer(
            layers=self.task.workflow.execution_layers,
            initial_input=input_data,
            ctx=execution_ctx
        )

    async def _execute_layer(self, layers: List[List[Operator]],
                             initial_input: Any, ctx: WorkflowExecutionContext) -> Any:
        """执行层级结构（修复依赖检查）"""
        current_data = initial_input
        for layer in layers:
            # 并行执行当前层算子
            results = await asyncio.gather(*[
                self._execute_operator(operator, current_data, ctx)
                for operator in layer
            ])
            current_data = self._merge_results(results)
        return current_data

    async def _execute_operator(self, operator: Operator, input_data: Any,
                                 ctx: WorkflowExecutionContext) -> Any:
        """执行单个算子（修复Activity获取）"""
        # 检查依赖满足情况

        for dep in self.__temporal_activities_dependencies__.get(operator.name, []):
            if ctx.get_result(dep) is None:
                raise ApplicationError(
                    f"Unsatisfied dependency: {dep} for operator {operator.name}"
                )

        try:
            # 通过名称直接获取Activity定义
            # activity_name = f"op_{operator.name}"
            activity_name = operator.name
            act_def = next(
                a for a in self.__temporal_activities__
                if getattr(a, '__temporal_activity_definition__', {}).get('name') == activity_name
            )
        except StopIteration:
            raise ApplicationError(f"Activity for {operator.name} not found")

        # 执行Activity
        result: Any = await execute_activity(
            act_def,
            args=[input_data],
            start_to_close_timeout=timedelta(seconds=self.task.scheduler_config.timeout),
            task_queue=self.task.queue_name or "default",
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=10),
                maximum_attempts=self.task.scheduler_config.max_retries
            )
        )

        # 存储结果
        ctx.store_result(operator.name, result)
        return result

    @staticmethod
    def _merge_results(results: List[Any]) -> Any:
        """智能结果合并（增强异常处理）"""
        try:
            if not results:
                return None
            if len(results) == 1:
                return results[0]
            if all(isinstance(r, dict) for r in results):
                merged = {}
                for r in results:
                    merged.update(r)
                return merged
            return results
        except Exception as e:
            workflow.logger.error(f"Result merge failed: {str(e)}")
            raise ApplicationError("Result merge failed") from e

    def _create_activity(self, operator: Operator) -> Callable:
        """动态生成Activity（修复类型判断逻辑）"""
        # activity_name = f"op_{operator.name}"
        activity_name = operator.name

        if operator.operator_type == "function":
            activity_def = self._create_function_activity(operator, activity_name)
        elif operator.operator_type == "async_api":
            activity_def = self._create_api_activity(operator, activity_name)
        else:
            raise ValueError(f"Unsupported operator type: {type(operator).__name__}")

        return activity_def

    def _create_function_activity(self, operator: Operator, name: str) -> Callable:
        """生成函数型Activity（修复模块加载）"""

        @activity.defn(name=name)
        async def _activity_wrapper(input_data: Any) -> Any:
            try:
                # 动态加载函数模块
                module = importlib.import_module(operator.spec["module"])
                func = getattr(module, operator.spec["func_name"])

                # 执行并处理异步函数
                if asyncio.iscoroutinefunction(func):
                    return await func(input_data)
                else:
                    return func(input_data)
            except Exception as e:
                raise ExecError(f"Function execution failed: {str(e)}")

        # 添加活动定义元数据以便后续查找
        setattr(_activity_wrapper, '__temporal_activity_definition__', {'name': name})
        return _activity_wrapper

    def _create_api_activity(self, operator: Operator, name: str) -> Callable:
        """修复后的API型Activity"""
        @activity.defn(name=name)
        async def _activity_wrapper(input_data: Any) -> Any:
            activity_info()
            # 从operator.spec中获取配置
            run_config = operator.spec.get("run_config", {})
            sync_config = operator.spec.get("sync_config", {})
            poll_interval = operator.spec.get("poll_interval", 3)
            total_timeout = operator.spec.get("total_timeout", 30)

            import aiohttp  # 使用异步HTTP客户端

            async def poll_async_task(session: aiohttp.ClientSession, api_task_id: str):
                """优化后的异步轮询函数"""
                start_time = time.time()
                poll_count = 0

                while True:
                    # 心跳报告当前状态
                    activity.heartbeat(f"Polling {api_task_id} - Attempt {poll_count}")
                    poll_count += 1

                    # 超时检查
                    if time.time() - start_time > operator.total_timeout:
                        raise ApplicationError(f"Async task timeout after {operator.total_timeout}s")

                    try:
                        sync_url = sync_config.get("url", "").replace("{resource_id}", api_task_id)

                        # 发送异步状态查询请求
                        async with session.request(
                                method=sync_config.get("method", "GET"),
                                url=sync_url,
                                headers=sync_config.get("headers", {}),
                                params=sync_config.get("params", {}),
                                timeout=aiohttp.ClientTimeout(total=poll_interval)
                        ) as resp:
                            resp.raise_for_status()
                            status_data = await resp.json()

                    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                        activity.logger.warning(f"Polling error: {str(e)}")
                        await asyncio.sleep(operator.poll_interval)
                        continue

                    status = status_data.get(sync_config.get("status_field", "status"), "")
                    if status in sync_config.get("success_status", []):
                        return status_data
                    if status in sync_config.get("failure_status", []):
                        raise ExecError(f"Async task failed with status: {status}")

                    await asyncio.sleep(poll_interval)

            # 主请求逻辑
            async with aiohttp.ClientSession() as session:
                try:
                    async with session.request(
                            method=run_config.get("method", "POST"),
                            url=run_config.get("url", ""),
                            json=input_data,
                            headers=run_config.get("headers", {}),
                            params=run_config.get("params", {}),
                            timeout=aiohttp.ClientTimeout(total=run_config.get("timeout", 10))
                    ) as response:
                        response.raise_for_status()
                        response_data = await response.json()

                except aiohttp.ClientError as e:
                    raise ExecError(f"Initial API request failed: {str(e)}")

                # 处理异步轮询
                if operator.requires_async_polling():
                    task_id = operator.extract_task_id(response_data)
                    return await poll_async_task(session, task_id)

                return operator.parse_response(response_data)

        # 添加活动定义元数据以便后续查找
        setattr(_activity_wrapper, '__temporal_activity_definition__', {'name': name})
        return _activity_wrapper
