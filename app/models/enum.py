from enum import Enum


class TaskStatus(str, Enum):
    """优化后的任务状态枚举"""
    PENDING = "pending"        # 任务等待调度
    RUNNING = "running"        # 任务正在执行
    CANCELLED = "cancelled"    # 任务被取消
    PAUSED = "paused"          # 任务被暂停
    TIMEOUT = "timeout"
    RELEASED = "released"


class ScheduleType(str, Enum):
    SCHEDULED = "scheduled"
    IMMEDIATE = "immediate"
