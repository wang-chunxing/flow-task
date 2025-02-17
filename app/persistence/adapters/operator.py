import uuid
from datetime import datetime

from sqlalchemy import (
    JSON,
    UUID,
    Column,
    Integer,
    String,
    UniqueConstraint,
    and_,
    select,
    update,
)
from sqlalchemy.exc import IntegrityError, NoResultFound

from app.models.models import Operator
from app.persistence.abstract import (
    OperatorCompatibilityError,
    OperatorNotFoundError,
    OperatorRepository,
    VersionMismatchError,
)
from app.persistence.base import BaseModel


class OperatorModel(BaseModel):
    """Operator 配置表（纯SQLAlchemy版本）"""
    __tablename__ = 'operators'
    __table_args__ = (
        # 名称+版本号的唯一约束
        UniqueConstraint('name', 'version', name='uq_operator_name_version'),
        {'comment': '系统算子配置表'}
    )

    # 算子名称（带索引）
    name = Column(
        String(255),
        nullable=False,
        index=True,
        comment="算子名称"
    )

    # 算子类型
    operator_type = Column(
        'operator_type',  # 显式指定列名
        String(50),
        nullable=False,
        comment="算子类型"
    )

    spec = Column(
        JSON,
        nullable=False,
        comment="算子配置规范"
    )

    # 版本号
    version = Column(
        Integer,
        nullable=False,
        comment="版本号"
    )

    def __repr__(self):
        return f"<Operator(name='{self.name}', version={self.version})>"


class OperatorStorage(OperatorRepository):
    def __init__(self, session_factory):
        self.session_factory = session_factory

    async def save(self, operator: Operator) -> Operator:
        if not operator.version:
            operator.version = 1

        async with self.session_factory() as session:
            try:
                # 查询现有记录（按name查询）
                existing_result = await session.execute(
                    select(OperatorModel)
                    .where(OperatorModel.name == operator.name)  # type: ignore
                )
                existing = existing_result.scalar_one_or_none()

                if existing:
                    # 版本检查（乐观锁）
                    if existing.version != operator.version:
                        raise VersionMismatchError(
                            f"Operator '{operator.name}' version conflict. "
                            f"Current version: {existing.version}, "
                            f"Submitted version: {operator.version}"
                        )

                    # 执行原子更新（基于name和version）
                    update_data = operator.as_dict(exclude={'created_at', 'version'})
                    update_data.update({
                        "version": existing.version + 1,
                        "updated_at": datetime.utcnow()
                    })

                    # 使用组合条件保证原子性
                    result = await session.execute(
                        update(OperatorModel)
                        .where(
                            and_(
                                OperatorModel.name == operator.name,  # type: ignore
                                OperatorModel.version == operator.version  # type: ignore
                            )
                        )
                        .values(**update_data)
                    )

                    if result.rowcount == 0:
                        raise VersionMismatchError("Concurrent modification detected")

                    await session.execute(
                        select(OperatorModel)
                        .where(OperatorModel.name == operator.name)  # type: ignore
                    )

                else:

                    new_model = OperatorModel(
                        name=operator.name,
                        operator_type=operator.operator_type,
                        spec=operator.spec,
                        version=operator.version,
                    )

                    session.add(new_model)
                    await session.flush()
                await session.commit()
            except IntegrityError as e:
                await session.rollback()
                self._handle_integrity_error(e, operator)
        return operator

    def _handle_integrity_error(self, e: IntegrityError, operator: Operator):
        """处理唯一性约束异常"""
        error_msg = str(e.orig).lower()

        if "uq_operator_name_version" in error_msg:
            raise OperatorCompatibilityError(
                f"Operator '{operator.name}' version {operator.version} already exists"
            ) from e

        if "uq_operator_name" in error_msg:
            raise OperatorCompatibilityError(
                f"Operator name '{operator.name}' already exists"
            ) from e

        raise  # 其他类型异常继续抛出

    async def load_all(self) -> list[Operator]:
        async with self.session_factory() as session:
            result = await session.execute(select(OperatorModel))
            return [model for model in result.scalars()]

    async def load(self, name: str) -> Operator:
        async with self.session_factory() as session:
            result = await session.execute(
                select(OperatorModel).filter_by(name=name)
            )
            try:
                model = result.scalar_one()
                return Operator(
                    id=UUID(model.id),
                    name=model.name,
                    operator_type=model.operator_type,
                    spec=model.spec,
                    version=model.version,
                    created_at=model.created_at,
                    updated_at=model.updated_at
                )
            except NoResultFound:
                raise OperatorNotFoundError(f"Operator '{name}' not found")

    async def delete(self, name: str) -> None:
        async with self.session_factory() as session:
            result = await session.execute(
                select(OperatorModel).filter_by(name=name)
            )
            try:
                model = result.scalar_one()
                await session.delete(model)
                await session.commit()
            except NoResultFound:
                raise OperatorNotFoundError(f"Operator '{name}' not found")
