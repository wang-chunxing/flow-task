from typing import Callable, Coroutine, List

from sqlalchemy import Column, Integer, String, UniqueConstraint, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.models import Queue
from app.persistence.abstract import QueueRepository
from app.persistence.base import BaseModel


class QueueModel(BaseModel):
    __tablename__ = "queue"
    __table_args__ = (
        # 唯一约束（队列名称必须唯一）
        UniqueConstraint('queue_name', name='uq_queue_name'),
        # 可添加其他表级参数
        {'comment': '系统队列配置表'}
    )

    # 队列名称字段（唯一约束）
    queue_name = Column(
        String(64),
        nullable=False,
        unique=True,  # 直接添加唯一约束
        comment="队列唯一名称"
    )

    # 最大并发数
    max_concurrent = Column(
        Integer,
        nullable=False,
        default=5,  # 默认值
        server_default='5',  # 数据库端默认值
        comment="最大并发数"
    )

    def __repr__(self):
        return f"<Queue(queue_name='{self.queue_name}', max_concurrent={self.max_concurrent})>"


class QueueStorage(QueueRepository):
    def __init__(self, session_factory):
        self.session_factory = session_factory

    async def save(self, queue_name: str, max_concurrent: int = 5) -> Queue:
        """创建队列配置"""
        async def _create(session: AsyncSession):
            config = Queue(
                queue_name=queue_name,
                max_concurrent=max_concurrent
            )
            session.add(config)
            return config

        return await self._execute_in_transaction([_create])

    async def load(self, queue_name: str) -> Queue:
        """获取队列配置"""
        async with self.session_factory() as session:
            result = await session.execute(
                select(QueueModel)
                .where(QueueModel.queue_name == queue_name)  # type: ignore
            )
            return result.scalar_one_or_none()

    async def _execute_in_transaction(self, operations: List[Callable[[AsyncSession], Coroutine]]):
        """事务执行模板方法"""
        async with self.session_factory() as session:
            try:
                for op in operations:
                    await op(session)
                await session.commit()
            except SQLAlchemyError as e:
                await session.rollback()
                raise RuntimeError(f"Database operation failed: {str(e)}")







