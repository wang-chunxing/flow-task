import importlib
import logging
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime
from typing import Callable, Dict, List, Set, Optional, DefaultDict

from pydantic import BaseModel, Field, ConfigDict
from requests.exceptions import Timeout as RequestTimeout
import requests

logger = logging.getLogger(__name__)


class Operator(BaseModel):
    name: str
    operator_type: str
    spec: dict
    version: int
    dependencies: Dict[str, List[str]] = Field(default_factory=dict)

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def as_dict(self, exclude: set = None) -> dict:
        """将对象转换为字典"""
        exclude = exclude or set()
        return self.model_dump(exclude=exclude, by_alias=True)


class Stage(BaseModel):
    name: str
    description: Optional[str] = None
    operators: Dict[str, Operator] = Field(default_factory=dict)
    dependencies: Dict[str, List[str]] = Field(default_factory=dict)

    def add_operator(self, operator: Operator, depends_on: List[str] = None):
        """添加算子并指定依赖"""
        if operator.name in self.operators:
            raise ValueError(f"Operator {operator.name} already exists in stage")

        self.operators[operator.name] = operator
        if depends_on:
            self.dependencies[operator.name] = depends_on
        return self

    @property
    def operator_list(self) -> List[Operator]:
        return list(self.operators.values())


class Workflow(BaseModel):
    name: str
    stages: List[Stage]
    execution_layers: List[List[Operator]]
    args: dict = Field(default_factory=dict)


# ----------- 任务调度相关模型 -----------
class SchedulerPolicy(BaseModel):
    scheduler_type: str
    timeout: int = Field(..., gt=0)
    max_retries: int = Field(..., ge=0)
    retry_interval: int = Field(..., ge=0)
    cron_expression: str
    timezone: str | None = None

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        protected_namespaces=()
    )


class Task(BaseModel):
    id: str
    name: str
    scheduler_type: str
    scheduler_config: SchedulerPolicy
    queue_name: str
    workflow: Workflow
    version: int
    success_handlers: List[Callable] = Field(default_factory=list)
    failure_handlers: List[Callable] = Field(default_factory=list)
    status: str
    current_retry: int = 0

    model_config = ConfigDict(arbitrary_types_allowed=True)


# ----------- DAG 相关模型 -----------
class DAG(BaseModel):
    graph: DefaultDict[str, List[str]] = Field(
        default_factory=lambda: defaultdict(list),
        description="邻接表表示的图结构"
    )
    in_degree: DefaultDict[str, int] = Field(
        default_factory=lambda: defaultdict(int),
        description="节点的入度统计"
    )
    nodes: Set[str] = Field(
        default_factory=set,
        description="节点集合"
    )

    def add_node(self, node: str):
        if node not in self.nodes:
            self.nodes.add(node)
            self.in_degree[node] = 0

    def add_edge(self, from_node: str, to_node: str):
        self.graph[from_node].append(to_node)
        self.in_degree[to_node] += 1

    def layered_topological_sort(self) -> List[List[str]]:
        """返回分层的执行计划"""
        in_degree = self.in_degree.copy()
        queue = deque([n for n in self.nodes if in_degree[n] == 0])
        layers = []

        while queue:
            layer = []
            for _ in range(len(queue)):
                node = queue.popleft()
                layer.append(node)
                for neighbor in self.graph[node]:
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        queue.append(neighbor)
            layers.append(layer)

        if sum(len(layer) for layer in layers) != len(self.nodes):
            raise ValueError("DAG contains cycles")
        return layers


# ----------- 队列相关模型 -----------
class Queue(BaseModel):
    max_concurrent: int = 5
    queue_name: str = Field(default_factory=lambda: str(uuid.uuid4()))
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


# ----------- 具体算子实现 -----------
class FunctionOperator(Operator):
    f: Optional[Callable] = Field(exclude=True, default=None)

    def __init__(self, spec: dict, name: str):
        super().__init__(
            name=name,
            operator_type="function",
            spec=spec,
            version=1
        )

    @property
    def func(self):
        if not self.f:
            module = importlib.import_module(self.spec["module"])
            self.f = getattr(module, self.spec["func_name"])
        return self.f

    def execute(self, *args, **kwargs):
        try:
            return self.func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Function operator failed: {e}", exc_info=True)
            raise


class APIOperator(Operator):
    def __init__(self, spec: dict, name: str):
        super().__init__(
            name=name,
            operator_type="async_api",
            spec=spec,
            version=1
        )
    def execute(self, data=None, params=None, headers=None):
        """执行API调用并根据配置处理异步轮询"""
        # 1. 执行主请求

        main_response = self.execute_main_request(data, params, headers)

        # 2. 处理异步等待逻辑
        if self.requires_async_polling():
            task_id = self.extract_task_id(main_response)
            return self.poll_async_task(task_id)

        return self.parse_response(main_response)

    def execute_main_request(self, data, params, headers):
        """执行主API请求"""
        run_config = self.spec.get("run_config", {})
        try:
            return requests.request(
                method=run_config.get("method", "POST"),
                url=run_config["url"],
                headers=self.merge_headers(headers),
                params=self.merge_params(params),
                json=data,
                auth=run_config.get("auth"),
                timeout=run_config.get("request_timeout", 10)
            )
        except Exception as e:
            logger.error(f"Main request failed: {str(e)}")
            raise RuntimeError(f"API operator failed initial request: {str(e)}") from e

    def requires_async_polling(self):
        """检查是否需要异步轮询"""
        return bool(self.spec.get("sync_policy") == "true")

    def extract_task_id(self, response):
        """从主响应中提取任务ID"""
        try:
            return self.nested_get(
                response.json(),
                self.run_config.get("task_id_path", "id").split(".")
            )
        except (KeyError, ValueError) as e:
            logger.error(f"Failed to extract task ID: {str(e)}")
            raise RuntimeError("Failed to extract async task ID") from e

    def poll_async_task(self, task_id):
        """轮询异步任务状态"""
        sync_config = self.spec.get("sync_config", {})
        poll_interval = self.spec.get("poll_interval", 5)
        start_time = time.time()
        while time.time() - start_time < self.total_timeout:
            try:
                response = self.execute_sync_request(task_id)
                status = self.extract_status(response)

                if status in sync_config.get("success_status", []):
                    return self.parse_response(response)
                if status in sync_config.get("failure_status", []):
                    raise RuntimeError(f"Async task failed with status: {status}")

                time.sleep(poll_interval)
            except RequestTimeout as e:
                logger.warning(f"Polling timeout: {str(e)}")
                continue

        raise TimeoutError(f"Async task timed out after {self.total_timeout}s")

    def execute_sync_request(self, task_id):
        """执行状态检查请求"""
        sync_config = self.spec.get("sync_config", {})
        try:
            return requests.request(
                method=sync_config.get("method", "GET"),
                url=self.format_sync_url(task_id),
                headers=sync_config.get("headers", {}),
                params=sync_config.get("params", {}),
                auth=sync_config.get("auth"),
                timeout=sync_config.get("request_timeout", self.poll_interval)
            )
        except Exception as e:
            logger.error(f"Sync request failed: {str(e)}")
            raise RuntimeError("Failed to poll async task status") from e

    def format_sync_url(self, task_id):
        """格式化包含任务ID的URL"""
        return self.sync_config["url"].replace("{task_id}", str(task_id))

    def extract_status(self, response):
        """从响应中提取状态"""
        try:
            return self.nested_get(
                response.json(),
                self.sync_config.get("status_path", "status").split(".")
            )
        except (KeyError, ValueError) as e:
            logger.error(f"Failed to extract status: {str(e)}")
            raise RuntimeError("Invalid status response format") from e

    @staticmethod
    def nested_get(data, path):
        """从嵌套字典中获取值"""
        for key in path:
            data = data[key]
        return data

    def merge_headers(self, headers):
        return {**self.run_config.get("headers", {}), **(headers or {})}

    def merge_params(self, params):
        return {**self.run_config.get("params", {}), **(params or {})}

    def parse_response(self, response):
        response.raise_for_status()
        return response.json()
