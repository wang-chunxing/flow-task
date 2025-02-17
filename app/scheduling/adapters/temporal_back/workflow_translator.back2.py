import asyncio
import importlib
import time
import uuid
from datetime import timedelta
from shutil import ExecError
from typing import Any, Callable, Dict, List

from temporalio import activity, workflow
from temporalio.activity import info as activity_info
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio.workflow import execute_activity

from app.models.models import Operator, Task
from app.plugins.operators.registry import APIOperator, FunctionOperator


class WorkflowExecutionContext:
    """工作流执行上下文管理器"""

    def __init__(self):
        self.results = {}
        self.dependencies = {}

    def store_result(self, op_name: str, result: Any):
        self.results[op_name] = result

    def get_result(self, op_name: str) -> Any:
        return self.results.get(op_name)


class WorkflowTranslator:
    # def __init__(self, operator_registry: Dict[str, Type[Operator]]):
    #     # self.operator_registry = operator_registry
    def __init__(self):
        self._activity_cache: Dict[str, Callable] = {}
        self._dependencies: Dict[str, List[str]] = {}

    def translate(self, task: Task) -> workflow.defn:   # type: ignore
        # 遍历所有stage的dependencies字典
        for stage in task.workflow.stages:
            # 每个stage的dependencies是字典类型
            for op_name, op_deps in stage.dependencies.items():
                # 合并依赖（保留所有阶段的依赖定义）
                if op_name in self._dependencies:
                    # 合并去重逻辑（如果需要）
                    existing = self._dependencies[op_name]
                    merged = list(set(existing + op_deps))  # 合并并去重
                    self._dependencies[op_name] = merged
                else:
                    self._dependencies[op_name] = op_deps

        workflow_def = task.workflow
        activities = []
        for layer in workflow_def.execution_layers:
            for op in layer:
                activity_def = self._create_activity(op)
                activities.append(activity_def)
        @workflow.defn(name=f"DynamicWorkflow_{task.name}_{uuid.uuid4()}")
        class DynamicWorkflow:

            __temporal_activities__ = activities
            __temporal_activities_dependencies__ = self._dependencies

            @workflow.run
            async def run(self, input_data: Any) -> Any:

                execution_ctx = WorkflowExecutionContext()

                return await self._execute_layer(
                    layers=workflow_def.execution_layers,
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

                for dep in self.__temporal_activities_dependencies__[operator.name]:
                    if ctx.get_result(dep) is None:
                        raise ApplicationError(
                            f"Unsatisfied dependency: {dep} for operator {operator.name}"
                        )

                try:
                    # 通过名称直接获取Activity定义
                    activity_name = f"op_{operator.name}"
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
                    start_to_close_timeout=timedelta(seconds=task.scheduler_config.timeout),
                    task_queue=task.queue_name or "default",
                    retry_policy=RetryPolicy(
                        initial_interval=timedelta(seconds=10),
                        maximum_attempts=task.scheduler_config.max_retries
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

        return DynamicWorkflow

    def _create_activity(self, operator: Operator) -> Callable:
        """动态生成Activity（修复类型判断逻辑）"""
        activity_name = f"op_{operator.name}"

        # 检查缓存
        if activity_name in self._activity_cache:
            return self._activity_cache[activity_name]

        # 根据算子类型创建对应Activity
        if operator.operator_type == "function":
            activity_def = self._create_function_activity(operator, activity_name)
        elif operator.operator_type == "async_api":
            activity_def = self._create_api_activity(operator, activity_name)
        else:
            raise ValueError(f"Unsupported operator type: {type(operator).__name__}")

        # 加入缓存
        self._activity_cache[activity_name] = activity_def
        return activity_def

    def _create_function_activity(self, operator: FunctionOperator, name: str) -> Callable:
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

    def _create_api_activity(self, operator: APIOperator, name: str) -> Callable:
        """修复后的API型Activity"""

        @activity.defn(name=name)
        async def _activity_wrapper(input_data: Any) -> Any:
            activity_info()

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
                        # 发送异步状态查询请求
                        async with session.request(
                                method=operator.sync_config.get("method", "GET"),
                                url=operator.format_sync_url(api_task_id),
                                headers=operator.sync_config.get("headers", {}),
                                params=operator.sync_config.get("params", {}),
                                timeout=aiohttp.ClientTimeout(total=operator.poll_interval)
                        ) as resp:
                            resp.raise_for_status()
                            status_data = await resp.json()

                    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                        activity.logger.warning(f"Polling error: {str(e)}")
                        await asyncio.sleep(operator.poll_interval)
                        continue

                    # 状态处理
                    status = operator.extract_status(status_data)
                    if status in operator.sync_config.get("success_status", []):
                        return operator.parse_response(status_data)
                    if status in operator.sync_config.get("failure_status", []):
                        raise ExecError(f"Async task failed with status: {status}")

                    await asyncio.sleep(operator.poll_interval)

            # 主请求逻辑
            async with aiohttp.ClientSession() as session:
                try:
                    # 发送主请求
                    async with session.request(
                            method=operator.run_config.get("method", "POST"),
                            url=operator.run_config["url"],
                            json=input_data,
                            headers=operator.run_config.get("headers", {}),
                            params=operator.run_config.get("params", {}),
                            timeout=aiohttp.ClientTimeout(total=operator.run_config.get("timeout", 10))
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
