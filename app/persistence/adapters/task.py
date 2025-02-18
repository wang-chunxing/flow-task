
from datetime import datetime
from typing import List
from uuid import UUID

from sqlalchemy import (
    JSON,
    Column,
    Enum,
    Integer,
    String,
    and_,
    func,
    select,
    update as sql_update,
)
from sqlalchemy.exc import SQLAlchemyError

from app.models.models import (
    Operator,
    SchedulerPolicy,
    Stage,
    Task,
    Workflow,
)
from app.persistence.abstract import (
    TaskNotFound,
    TaskRepository,
)
from app.persistence.adapters import OperatorStorage
from app.persistence.base import BaseModel
from app.plugins.operators.registry import APIOperator, FunctionOperator


class TaskModel(BaseModel):
    """任务主表（纯SQLAlchemy版本）"""
    __tablename__ = 'tasks'
    __table_args__ = {
        'comment': '任务主表'
    }

    # 基础信息
    name = Column(
        String(255),
        nullable=False,
        comment='任务名称'
    )

    scheduler_type = Column(
        String(20),
        nullable=False,
        comment='调度类型'
    )

    scheduler_config = Column(
        JSON,
        nullable=False,
        comment='调度参数'
    )

    workflow_definition = Column(
        JSON,
        nullable=False,
        comment='工作流定义'
    )

    # 版本控制
    version = Column(
        Integer,
        nullable=False,
        default=0,
        server_default='0',
        comment='版本号'
    )

    # 队列信息
    queue_name = Column(
        String(32),
        nullable=False,
        comment='任务队列'
    )

    # 状态管理
    status = Column(
        String(32),
        nullable=False,
        default="pending",
        index=True,
        comment='任务状态'
    )

    retries = Column(
        Integer,
        default=0,
        server_default='0',
        comment='重试次数'
    )

    # 处理器配置
    success_handlers = Column(
        JSON,
        comment='成功处理器列表'
    )

    failure_handlers = Column(
        JSON,
        comment='失败处理器列表'
    )

    # 业务数据
    payload = Column(
        JSON,
        comment='任务元数据'
    )

    def __repr__(self):
        return f"<Task(name='{self.name}', status={self.status})>"


class TaskStorage(TaskRepository):
    """基于SQL数据库的实现（修复版）"""

    def __init__(self, session_factory, operator_repository: OperatorStorage):
        self.session_factory = session_factory
        self.operator_repository = operator_repository

    async def save(self, task: Task) -> Task:
        async with self.session_factory() as session:
            model = self._convert_to_model(task)
            session.add(model)
            await session.commit()
            return await self._convert_from_model(model)

            # try:
            #     model = self._convert_to_model(task)
            #
            #     # 如果 task.id 为空，表示是新增任务
            #     if not task.id:
            #         session.add(model)
            #         await session.commit()
            #         return await self._convert_from_model(model)
            #
            #     # 如果 task.id 不为空，表示是更新任务
            #     result = await session.execute(
            #         select(TaskModel)
            #         .where(TaskModel.id == str(task.id))
            #         .with_for_update()
            #     )
            #     existing = result.scalar_one_or_none()
            #
            #     if existing:
            #         if existing.version != task.version:
            #             raise ConcurrencyError(f"Task {task.id} version conflict")
            #         model.version += 1
            #         await session.merge(model)
            #     else:
            #         # 如果任务不存在，则抛出异常
            #         raise ValueError(f"Task with id {task.id} not found")
            #
            #     await session.commit()
            #     return await self._convert_from_model(model)
            # except IntegrityError as e:
            #     await session.rollback()
            #     raise ConcurrentUpdate("版本冲突或数据不一致") from e
            # except SQLAlchemyError as e:
            #     await session.rollback()
            #     raise RuntimeError(f"数据库操作失败: {str(e)}") from e

    async def load(self, task_id: str) -> Task:
        async with self.session_factory() as session:
            result = await session.execute(
                select(TaskModel)
                .where(TaskModel.id == task_id)  # type: ignore
            )
            model = result.scalar_one_or_none()
            return await self._convert_from_model(model)

    async def update_task_status(self, task_id: str, new_status: str,
                                 expected_current: str | None = None) -> bool:
        async with self.session_factory() as session:
            try:
                # 使用统一查询方式
                result = await session.execute(
                    select(TaskModel)
                    .where(TaskModel.id == task_id)  # type: ignore
                    .with_for_update()
                )
                task_model = result.scalar_one_or_none()

                if not task_model:
                    raise TaskNotFound(f"Task {task_id} not found")

                if expected_current and task_model.status != expected_current:
                    return False

                # 使用ORM更新方式
                await session.execute(
                    sql_update(TaskModel)
                    .where(TaskModel.id == task_id)  # type: ignore
                    .values(
                        status=new_status,
                        version=TaskModel.version + 1,
                        updated_at=func.now()
                    )
                )
                await session.commit()
                return True
            except SQLAlchemyError as e:
                await session.rollback()
                raise RuntimeError(f"状态更新失败: {str(e)}") from e

    async def get_concurrency(self, queue_name: str) -> int:
        async with self.session_factory() as session:
            result = await session.execute(
                select(func.count())
                .select_from(TaskModel)
                .where(
                    and_(
                        TaskModel.queue_name == queue_name,  # type: ignore
                        TaskModel.status == "running"  # type: ignore
                    )
                )
            )
            return result.scalar() or 0

    async def get_retry_count(self, task_id: str) -> int:
        async with self.session_factory() as session:
            stmt = select(TaskModel.retries).where(TaskModel.id == task_id)  # type: ignore
            result = await session.scalars(stmt)
            return result.one_or_none() or 0

    async def get_by_status(self, status: List[str]) -> List[Task]:
        async with self.session_factory() as session:
            result = await session.execute(
                select(TaskModel)
                .where(TaskModel.status.in_([s.value for s in status]))  # type: ignore
            )
            return [await self._convert_from_model(m) for m in result.scalars()]

    async def record_retry_attempt(self, task_id: str, scheduled_time: datetime):
        async with self.session_factory() as session:
            try:
                # 使用ORM更新方式
                await session.execute(
                    sql_update(TaskModel)
                    .where(TaskModel.id == task_id)  # type: ignore
                    .values(
                        retries=TaskModel.retries + 1,
                        scheduler_config=func.jsonb_set(
                            TaskModel.scheduler_config,
                            '{scheduled_retry_time}',
                            f'"{scheduled_time.isoformat()}"'
                        )
                    )
                )
                await session.commit()
            except SQLAlchemyError as e:
                await session.rollback()
                raise RuntimeError(f"重试记录失败: {str(e)}") from e

    def _convert_to_model(self, task: Task) -> TaskModel:
        scheduler_config_dict = {
            "scheduler_type": task.scheduler_type,
            'timeout': task.scheduler_config.timeout,
            'retry_interval': task.scheduler_config.retry_interval,
            'retries': task.scheduler_config.max_retries
        }

        """转换业务对象到持久化模型"""
        return TaskModel(
            name=task.name,
            scheduler_type=task.scheduler_config.scheduler_type,
            scheduler_config=scheduler_config_dict,
            queue_name=task.queue_name,
            workflow_definition=self._serialize_workflow(task.workflow),
            version=task.version,
            # 下面两个实现有问题，先忽略
            # success_handlers=self._serialize_handlers(task.success_handlers),
            # failure_handlers=self._serialize_handlers(task.failure_handlers)
        )

    async def _convert_from_model(self, model: TaskModel) -> Task:
        """数据库模型转领域对象"""
        return Task(
            id=model.id,
            name=model.name,
            scheduler_type=model.scheduler_type,
            scheduler_config=SchedulerPolicy(
                scheduler_type=model.scheduler_type,
                timeout=model.scheduler_config["timeout"],
                retry_interval=model.scheduler_config["retry_interval"],
                max_retries=model.scheduler_config.get("max_retries",3),
                cron_expression=model.scheduler_config["cron_expression"]
                    if "cron_expression" in model.scheduler_config else ""
            ),
            queue_name=model.queue_name,
            workflow=await self._deserialize_workflow(model.workflow_definition),
            version=model.version,
            success_handlers=await self._deserialize_handlers(model.success_handlers),  # type: ignore
            failure_handlers=await self._deserialize_handlers(model.failure_handlers),  # type: ignore
            created_at=model.created_at,  # type: ignore
            updated_at=model.updated_at,  # type: ignore
            status=model.status
        )

    def _serialize_workflow(self, workflow: Workflow) -> dict:
        """序列化保持Stage对象结构"""
        return {
            "name": workflow.name,
            "stages": [
                {
                    "name": stage.name,
                    "operators": [self._serialize_operator(op) for op in stage.operator_list],
                    "dependencies": stage.dependencies,
                    "description": stage.description
                }
                for stage in workflow.stages
            ],
            "execution_layers": [
                [op.name for op in layer]
                for layer in workflow.execution_layers
            ],
            "args": workflow.args
        }

    def _serialize_operator(self, operator: Operator) -> dict:
        """序列化单个算子"""
        if isinstance(operator, FunctionOperator):
            return {
                "name": operator.name,
                "type": "function",
                "module": operator.spec["module"],
                "function": operator.spec["func_name"]
            }
        elif isinstance(operator, APIOperator):
            return {
                "name": operator.name,
                "type": "api",
                "spec": operator.spec
            }
        raise ValueError(f"Unknown operator type: {type(operator)}")

    async def _deserialize_workflow(self, data: dict) -> Workflow:
        """反序列化重建Stage对象"""
        # 从数据中提取工作流名称和参数
        workflow_name = data.get("name", "")
        workflow_args = data.get("args", {})

        # 先加载所有算子
        all_operators = {}
        for stage_data in data["stages"]:
            for op_data in stage_data["operators"]:
                op = await self._deserialize_operator(op_data)
                all_operators[op.name] = op

        # 重建Stage对象
        stages = []
        for stage_data in data["stages"]:
            stage = Stage(name=stage_data["name"], description=stage_data["description"])
            dependencies = stage_data.get("dependencies", {})

            # 按原始顺序添加算子
            for op_data in stage_data["operators"]:
                op = all_operators[op_data["name"]]
                stage.add_operator(op, depends_on=dependencies.get(op.name, []))

            stages.append(stage)

        # 重建执行层
        execution_layers = []
        for layer_names in data["execution_layers"]:
            layer = [all_operators[name] for name in layer_names]
            execution_layers.append(layer)

        return Workflow(
            name=workflow_name,
            stages=stages,
            execution_layers=execution_layers,
            args=workflow_args
        )

    async def _deserialize_operator(self, data: dict) -> Operator:
        return await self.operator_repository.load(data["name"])

    async def _deserialize_handlers(self, handler_names: List[str]) -> list[Operator]:
        """反序列化处理器"""
        return [await self.operator_repository.load(name) for name in (handler_names or [])]
