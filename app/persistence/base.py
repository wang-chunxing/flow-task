from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import Column, DateTime, String
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


def now_utc():
    """生成当前UTC时间"""
    return datetime.now(timezone.utc)


class BaseModel(Base):
    """所有模型的抽象基类"""
    __abstract__ = True

    id = Column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid4()),  # 使用 lambda 生成 UUID 字符串
        comment="主键UUID"
    )

    # 创建时间（带时区）
    created_at = Column(
        DateTime(timezone=True),
        default=now_utc,
        nullable=False,
        index=True,
        comment="创建时间"
    )

    # 更新时间（带时区，自动更新）
    updated_at = Column(
        DateTime(timezone=True),
        default=now_utc,
        onupdate=now_utc,  # 更新时自动刷新
        nullable=False,
        index=True,
        comment="最后更新时间"
    )

    def __repr__(self):
        """统一显示格式"""
        return f"<{self.__class__.__name__}(id={self.id})>"

    def __init__(self, **kwargs):
        if 'created_at' not in kwargs:
            kwargs['created_at'] = now_utc()
        if 'updated_at' not in kwargs:
            kwargs['updated_at'] = now_utc()
        super().__init__(**kwargs)
