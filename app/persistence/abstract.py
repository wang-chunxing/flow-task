from __future__ import annotations
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from uuid import UUID

    from app.models.models import Queue, Task


class TaskRepository(ABC):
    @abstractmethod
    async def save(self, task: Task) -> Task:
        """保存任务对象，返回任务ID"""
        pass

    @abstractmethod
    async def load(self, task_id: UUID) -> Task:
        """根据ID获取任务"""
        pass


class OperatorRepository(ABC):
    """持久化层抽象接口"""
    @abstractmethod
    async def save(self, operator):
        """保存算子对象"""
        pass

    @abstractmethod
    async def load_all(self):
        """获取所有算子对象"""
        pass

    @abstractmethod
    async def delete(self, name):
        """删除算子对象"""
        pass

    @abstractmethod
    async def load(self, name):
        """获取指定版本的算子对象"""
        pass


class QueueRepository(ABC):
    @abstractmethod
    async def save(self, queue_name: str, max_concurrent: int = 5) -> Queue:
        """创建队列配置"""
        pass

    @abstractmethod
    async def load(self, queue_name: str) -> Queue:
        """根据ID获取任务"""
        pass


class PersistenceException(Exception):
    """持久化层基础异常"""
    pass


class TaskNotFound(PersistenceException):
    """任务不存在异常"""
    pass


class ConcurrentUpdate(PersistenceException):
    """并发更新异常"""
    pass


class OperatorVersionError(Exception):
    """基类异常"""


class VersionMismatchError(OperatorVersionError):
    """版本不匹配异常"""


class OperatorNotFoundError(OperatorVersionError):
    """算子不存在异常"""


class OperatorCompatibilityError(OperatorVersionError):
    """接口不兼容异常"""


class ConcurrencyError(Exception):
    """并发控制基类异常"""


class QueueNotFound(ConcurrencyError):
    """队列不存在异常"""


class InvalidStateTransition(ConcurrencyError):
    """非法状态转移异常"""


class MaxRetriesExceeded(ConcurrencyError):
    """超过最大重试次数异常"""


class ConcurrencyLimitError(ConcurrencyError):
    """并发限制冲突异常"""
