import uuid
from abc import ABC
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Dict, List, Set

from app.models.enum import ScheduleType, TaskStatus


@dataclass
class Operator:
    id: uuid.UUID
    name: str
    operator_type: str
    spec: dict
    version: int
    dependencies: Dict[str, List[str]] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)

    def as_dict(self, exclude: set) -> dict:
        """将对象转换为字典"""
        exclude = exclude or set()
        return {
            key: value
            for key, value in self.__dict__.items()
            if not key.startswith('_') and key not in exclude
        }


@dataclass
class Stage:
    name: str
    description: str | None
    operators: Dict[str, Operator] = field(default_factory=dict)
    dependencies: Dict[str, List[str]] = field(default_factory=dict)

    def describe(self, description: str) -> 'Stage':
        """设置阶段描述"""
        self.description = description
        return self

    def add_operator(self, operator: Operator, depends_on: List[str] | None = None, sync_policy: dict | None = None):
        """添加算子并指定依赖"""
        if operator.name in self.operators:
            raise ValueError(f"Operator {operator.name} already exists in stage")

        self.operators[operator.name] = operator
        if depends_on:
            self.dependencies[operator.name] = depends_on
        return self

    @property
    def operator_list(self) -> List[Operator]:
        """获取算子顺序列表"""
        return list(self.operators.values())


@dataclass
class Workflow:
    name: str
    stages: List[Stage]
    execution_layers: List[List[Operator]]
    args: dict = field(default_factory=dict)


@dataclass
class TaskQueue:
    max_concurrency: int  # 最大并行度
    queue_name: str  # 队列标识
    user_concurrency: Dict[str, int]


@dataclass
class SchedulerPolicy(ABC):
    scheduler_type: str
    timeout: int
    max_retries: int
    retry_interval: int
    cron_expression: str
    timezone = None


@dataclass
class Task:
    id: uuid.UUID
    name: str
    scheduler_type: ScheduleType
    scheduler_config: SchedulerPolicy
    queue_name: str
    workflow: Workflow
    version: int
    success_handlers: List[Callable]
    failure_handlers: List[Callable]
    status: TaskStatus
    current_retry: int = 0
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class DAG:
    """增强的DAG类，支持分层拓扑排序"""
    graph: Dict[str, List[str]] = field(
        default_factory=lambda: defaultdict(list),
    )
    in_degree: Dict[str, int] = field(
        default_factory=lambda: defaultdict(int),
    )
    nodes: Set[str] = field(
        default_factory=set,
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


@dataclass
class Queue:
    max_concurrent: int = 5
    queue_name: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
