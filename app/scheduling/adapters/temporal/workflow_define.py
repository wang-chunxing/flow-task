import asyncio
import importlib
import json
import time
import logging
from datetime import timedelta
from shutil import ExecError
from typing import Any, List, Callable
from temporalio import workflow, activity
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio.workflow import execute_activity
from temporalio.activity import info as activity_info
from app.models.models import Operator, Task

logger = logging.getLogger(__name__)

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

    def load_dependencies(self):
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

    def load_activities(self):
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
    async def run(self, task: Task, input_data: dict) -> dict:
        execution_ctx = WorkflowExecutionContext()
        self.task = task
        self.load_activities()
        self.load_dependencies()

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
                                 ctx: WorkflowExecutionContext) -> dict:
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
        async def _activity_wrapper(input_data: Any) -> dict:
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
        async def _activity_wrapper(input_data: Any) -> dict:

            activity_info()
            # 从operator.spec中获取配置
            run_config = operator.spec.get("run_config", {})
            sync_config = operator.spec.get("sync_config", {})
            poll_interval = operator.spec.get("poll_interval", 3)
            total_timeout = operator.spec.get("total_timeout", 30)
            sync_policy = operator.spec.get("sync_policy", "false")

            import aiohttp

            def extract_task_id(run_config, response):
                """从主响应中提取任务ID"""
                try:
                    return nested_get(
                        response,
                        run_config.get("task_id_path", "id").split(".")
                    )
                except (KeyError, ValueError) as e:
                    logger.error(f"Failed to extract task ID: {str(e)}")
                    raise RuntimeError("Failed to extract async task ID") from e

            def extract_status(sync_config, response):
                """从响应中提取状态"""
                try:
                    return nested_get(
                        response,
                        sync_config.get("status_field", "status").split(".")
                    )
                except (KeyError, ValueError) as e:
                    logger.error(f"Failed to extract status: {str(e)}")
                    raise RuntimeError("Invalid status response format") from e

            def nested_get(data, path):
                """从嵌套字典中获取值"""
                for key in path:
                    data = data.get(key,{})
                return data

            def build_curl_command(method, url, headers, params, data=None):
                """构建curl格式的请求命令"""
                cmd = [f"curl -X {method}"]

                # 添加headers
                for k, v in headers.items():
                    cmd.append(f"-H '{k}: {v}'")

                # 添加query参数
                if params:
                    url += "?" + "&".join([f"{k}={v}" for k, v in params.items()])
                cmd.append(f"'{url}'")

                # 添加请求体
                if data and method.upper() in ["POST", "PUT", "PATCH"]:
                    json_data = json.dumps(data)
                    cmd.append(f"--data-raw '{json_data}'")

                return " \\\n  ".join(cmd)

            async def poll_async_task(session: aiohttp.ClientSession, api_task_id: str):
                """优化后的异步轮询函数"""
                start_time = time.time()
                poll_count = 0

                while True:
                    activity.heartbeat(f"Polling {api_task_id} - Attempt {poll_count}")
                    poll_count += 1

                    if time.time() - start_time > total_timeout:
                        raise ApplicationError(f"Async task timeout after {total_timeout}s")

                    try:
                        sync_url = sync_config.get("url", "").replace("{id}", api_task_id)

                        # 构建轮询请求日志
                        logger.info("\n" + "#" * 80)
                        logger.info(f"[Polling Request #{poll_count}]")
                        curl_cmd = build_curl_command(
                            method=sync_config.get("method", "GET"),
                            url=sync_url,
                            headers=sync_config.get("headers", {}),
                            params=sync_config.get("params", {}),
                        )
                        logger.info("CURL command:\n%s", curl_cmd)

                        async with session.request(
                                method=sync_config.get("method", "GET"),
                                url=sync_url,
                                headers=sync_config.get("headers", {}),
                                params=sync_config.get("params", {}),
                                timeout=aiohttp.ClientTimeout(total=poll_interval)
                        ) as resp:
                            status_data = await resp.json()
                            logger.info(f"[Polling Response #{poll_count}] Status: {resp.status}")
                            logger.debug("Response body: %s", json.dumps(status_data, indent=2))

                    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                        logger.error("Polling error: %s", str(e))
                        await asyncio.sleep(operator.poll_interval)
                        continue

                    status = extract_status(sync_config, status_data)
                    print("############")
                    print(f"Status: {status}")
                    print(sync_config.get("terminal_statuses", []))
                    print("############")
                    if status in sync_config.get("terminal_statuses", []):
                        return status_data
                    await asyncio.sleep(poll_interval)

            async with aiohttp.ClientSession() as session:
                try:
                    # 记录主请求日志
                    logger.info("\n" + "=" * 80)
                    logger.info("[Main API Request]")
                    curl_cmd = build_curl_command(
                        method=run_config.get("method", "POST"),
                        url=run_config.get("url", ""),
                        headers=run_config.get("headers", {}),
                        params=run_config.get("params", {}),
                        data=input_data
                    )
                    logger.info("CURL command:\n%s", curl_cmd)

                    async with session.request(
                            method=run_config.get("method", "POST"),
                            url=run_config.get("url", ""),
                            json=input_data,
                            headers=run_config.get("headers", {}),
                            params=run_config.get("params", {}),
                            auth=sync_config.get("auth"),
                            timeout=aiohttp.ClientTimeout(total=run_config.get("timeout", 10))
                    ) as response:
                        response_data = await response.json()
                        logger.info(f"[Main Response] Status: {response.status}")
                        logger.debug("Response body: %s", json.dumps(response_data, indent=2))

                except aiohttp.ClientError as e:
                    logger.error("Initial request failed: %s", str(e))
                    raise ExecError(f"Initial API request failed: {str(e)}")

                if bool(sync_policy == "true"):
                    task_id = extract_task_id(run_config, response_data)
                    logger.info("Starting async polling for task: %s", task_id)
                    return await poll_async_task(session, task_id)

                return response_data

        setattr(_activity_wrapper, '__temporal_activity_definition__', {'name': name})
        return _activity_wrapper
