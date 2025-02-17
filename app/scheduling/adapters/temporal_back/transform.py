# import asyncio
# import importlib
# import time
# import uuid
# from datetime import timedelta
# from shutil import ExecError
# from typing import Any, Dict, Type, List, Set
# from temporalio import workflow, activity
# from temporalio.common import RetryPolicy
# from temporalio.exceptions import ApplicationError, ActivityError
# from temporalio.workflow import execute_activity
# from app.core.models import Workflow, Operator, Task
# from app.plugins.operators.registry import FunctionOperator, APIOperator
# from temporalio.activity import info as activity_info
#
#
# class WorkflowExecutionContext:
#     """工作流执行上下文管理器"""
#
#     def __init__(self):
#         self.results = {}
#         self.dependencies = {}
#
#     def store_result(self, op_name: str, result: Any):
#         self.results[op_name] = result
#
#     def get_result(self, op_name: str) -> Any:
#         return self.results.get(op_name)
#
#
# class WorkflowTranslator:
#
#     def __init__(self, operator_registry: Dict[str, Type[Operator]]):
#         self.operator_registry = operator_registry
#         self._activity_cache = {}
#
#     def translate(self, workflow_def: Workflow,task: Task) -> workflow.defn:
#
#         activities = []
#         for layer in workflow_def.execution_layers:
#             for op in layer:
#                 activity_def = self._create_activity(op)
#                 activities.append(activity_def)
#
#
#         @workflow.defn(name=f"DynamicWorkflow_{task.name}_{uuid.uuid4()}")
#         class DynamicWorkflow:
#
#             __temporal_activities__ = activities
#
#             @workflow.run
#             async def run(self, input_data: Any) -> Any:
#
#                 execution_ctx = WorkflowExecutionContext()
#
#                 return await self._execute_layer(
#                     layers=workflow_def.execution_layers,
#                     initial_input=input_data,
#                     ctx=execution_ctx
#                 )
#
#             async def _execute_layer(self, layers: List[List[Operator]],
#                                    initial_input: Any, ctx: WorkflowExecutionContext) -> Any:
#                 """执行层级结构（修复依赖检查）"""
#                 current_data = initial_input
#                 for layer in layers:
#                     # 并行执行当前层算子
#                     results = await asyncio.gather(*[
#                         self._execute_operator(operator, current_data, ctx)
#                         for operator in layer
#                     ])
#                     current_data = self._merge_results(results)
#                 return current_data
#
#             async def _execute_operator(self, operator: Operator, input_data: Any,
#                                         ctx: WorkflowExecutionContext) -> Any:
#                 """执行单个算子（修复Activity获取）"""
#                 # 检查依赖满足情况
#                 for dep in operator.dependencies:
#                     if ctx.get_result(dep) is None:
#                         raise ApplicationError(
#                             f"Unsatisfied dependency: {dep} for operator {operator.name}"
#                         )
#
#                 try:
#                     # 通过名称直接获取Activity定义
#                     act_def = next(
#                         a for a in self.__temporal_activities__
#                         if a.name == f"op_{operator.name}"
#                     )
#                 except StopIteration:
#                     raise ApplicationError(f"Activity for {operator.name} not found")
#
#                 # 执行Activity
#                 result = await execute_activity(
#                     act_def,
#                     args=[input_data],
#                     start_to_close_timeout=timedelta(seconds=task.scheduler_config.timeout),
#                     task_queue=task.queue_name or "default",
#                     retry_policy=RetryPolicy(
#                         initial_interval=timedelta(seconds=10),
#                         maximum_attempts=task.scheduler_config.max_retries
#                     )
#                 )
#
#                 # 存储结果
#                 ctx.store_result(operator.name, result)
#                 return result
#
#             @staticmethod
#             def _merge_results(results: List[Any]) -> Any:
#                 """智能结果合并（增强异常处理）"""
#                 try:
#                     if not results:
#                         return None
#                     if len(results) == 1:
#                         return results[0]
#                     if all(isinstance(r, dict) for r in results):
#                         merged = {}
#                         for r in results:
#                             merged.update(r)
#                         return merged
#                     return results
#                 except Exception as e:
#                     workflow.logger.error(f"Result merge failed: {str(e)}")
#
#                     raise ApplicationError("Result merge failed") from e
#
#         return DynamicWorkflow
#
#     def _create_activity(self, operator: Operator) -> activity.defn:
#         """动态生成Activity（修复类型判断逻辑）"""
#         activity_name = f"op_{operator.name}"
#
#         # 检查缓存
#         if activity_name in self._activity_cache:
#             return self._activity_cache[activity_name]
#
#         # 根据算子类型创建对应Activity
#         if isinstance(operator, FunctionOperator):
#             activity_def = self._create_function_activity(operator, activity_name)
#         elif isinstance(operator, APIOperator):
#             activity_def = self._create_api_activity(operator, activity_name)
#         else:
#             raise ValueError(f"Unsupported operator type: {type(operator).__name__}")
#
#         # 加入缓存
#         self._activity_cache[activity_name] = activity_def
#         return activity_def
#
#     def _create_function_activity(self, operator: FunctionOperator, name: str) -> activity.defn:
#         """生成函数型Activity（修复模块加载）"""
#
#         @activity.defn(name=name)
#         async def _activity_wrapper(input_data: Any) -> Any:
#             try:
#                 # 动态加载函数模块
#                 module = importlib.import_module(operator.spec["module"])
#                 func = getattr(module, operator.spec["func_name"])
#
#                 # 执行并处理异步函数
#                 if asyncio.iscoroutinefunction(func):
#                     return await func(input_data)
#                 else:
#                     return func(input_data)
#             except Exception as e:
#                 raise ActivityError(f"Function execution failed: {str(e)}")
#
#         return _activity_wrapper
#
#     def _create_api_activity(self, operator: APIOperator, name: str) -> activity.defn:
#         """修复后的API型Activity"""
#         @activity.defn(name=name)
#         async def _activity_wrapper(input_data: Any) -> Any:
#             ctx = activity_info()
#
#             import aiohttp  # 使用异步HTTP客户端
#             async def poll_async_task(session: aiohttp.ClientSession, api_task_id: str):
#                 """优化后的异步轮询函数"""
#                 start_time = time.time()
#                 poll_count = 0
#
#                 while True:
#                     # 心跳报告当前状态
#                     activity.heartbeat(f"Polling {api_task_id} - Attempt {poll_count}")
#                     poll_count += 1
#
#                     # 超时检查
#                     if time.time() - start_time > operator.total_timeout:
#                         raise ActivityError(f"Async task timeout after {operator.total_timeout}s")
#
#                     try:
#                         # 发送异步状态查询请求
#                         async with session.request(
#                                 method=operator.sync_config.get("method", "GET"),
#                                 url=operator.format_sync_url(api_task_id),
#                                 headers=operator.sync_config.get("headers", {}),
#                                 params=operator.sync_config.get("params", {}),
#                                 timeout=aiohttp.ClientTimeout(total=operator.poll_interval)
#                         ) as resp:
#                             resp.raise_for_status()
#                             status_data = await resp.json()
#
#                     except (aiohttp.ClientError, asyncio.TimeoutError) as e:
#                         activity.logger.warning(f"Polling error: {str(e)}")
#                         await asyncio.sleep(operator.poll_interval)
#                         continue
#
#                     # 状态处理
#                     status = operator.extract_status(status_data)
#                     if status in operator.sync_config.get("success_status", []):
#                         return operator.parse_response(status_data)
#                     if status in operator.sync_config.get("failure_status", []):
#                         raise ExecError(f"Async task failed with status: {status}")
#
#                     await asyncio.sleep(operator.poll_interval)
#
#             # 主请求逻辑
#             async with aiohttp.ClientSession() as session:
#                 try:
#                     # 发送主请求
#                     async with session.request(
#                             method=operator.run_config.get("method", "POST"),
#                             url=operator.run_config["url"],
#                             json=input_data,
#                             headers=operator.run_config.get("headers", {}),
#                             params=operator.run_config.get("params", {}),
#                             timeout=aiohttp.ClientTimeout(total=operator.run_config.get("timeout", 10))
#                     ) as response:
#                         response.raise_for_status()
#                         response_data = await response.json()
#
#                 except aiohttp.ClientError as e:
#                     raise ExecError(f"Initial API request failed: {str(e)}")
#
#                 # 处理异步轮询
#                 if operator.requires_async_polling():
#                     task_id = operator.extract_task_id(response_data)
#                     return await poll_async_task(session, task_id)
#
#                 return operator.parse_response(response_data)
#
#         return _activity_wrapper


#
# class WorkflowRegistrar:
#     """增强型工作流注册管理器"""
#
#     def __init__(self, client):
#         self.client = client
#         self.registered_workflows = {}
#         self.worker_tasks = []
#
#     async def register(self, workflow_def: Workflow, translator: WorkflowTranslator):
#         """注册工作流及关联Activities"""
#         workflow_class = translator.translate(workflow_def)
#         if workflow_class.__name__ in self.registered_workflows:
#             return
#
#         # 创建专用Worker
#         worker = Worker(
#             self.client,
#             task_queue=workflow_class.__workflow_config__["task_queue"],
#             workflows=[workflow_class],
#             activities=workflow_class.__temporal_activities__,
#             max_concurrent_activities=MAX_CONCURRENT_ACTIVITIES,
#             activity_executor=self._create_managed_executor(),
#         )
#
#         # 启动后台Worker
#         task = asyncio.create_task(worker.run())
#         self.worker_tasks.append(task)
#         self.registered_workflows[workflow_class.__name__] = {
#             "workflow": workflow_class,
#             "worker": worker,
#             "task": task
#         }
#
#     def _create_managed_executor(self):
#         """创建受控的执行环境"""
#         from concurrent.futures import ThreadPoolExecutor
#         return ThreadPoolExecutor(
#             max_workers=MAX_CONCURRENT_ACTIVITIES,
#             thread_name_prefix="temporal_activity_"
#         )
#
#     async def deregister(self, workflow_name: str):
#         """注销工作流"""
#         if info := self.registered_workflows.get(workflow_name):
#             await info["worker"].shutdown()
#             info["task"].cancel()
#             del self.registered_workflows[workflow_name]
#
#     async def shutdown(self):
#         """关闭所有Worker"""
#         for info in self.registered_workflows.values():
#             await info["worker"].shutdown()
#             info["task"].cancel()
#         self.registered_workflows.clear()
#
#
# class AdvancedDynamicScheduler(TaskScheduler):
#     """增强型调度器实现"""
#
#     async def _execute_workflow(self, task: Task):
#         # 转换工作流定义
#         workflow_class = self.translator.translate(task.workflow_def)
#
#         # 注册到Temporal
#         await self.registrar.register(workflow_class)
#
#         # 启动工作流执行
#         await self.client.start_workflow(
#             workflow=workflow_class.run,
#             args=[task.input_data],
#             id=f"wf-{task.id}",
#             task_queue=workflow_class.__workflow_config__["task_queue"],
#             execution_timeout=timedelta(seconds=task.timeout),
#             retry_policy=RetryPolicy(
#                 maximum_attempts=task.max_retries,
#                 initial_interval=timedelta(seconds=task.retry_interval)
#             )
#         )
#         await self.storage.update_task_status(task.id, TaskStatus.RUNNING)
