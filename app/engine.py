from uuid import UUID

from app.models.models import Task, TaskStatus
from app.persistence.adapters import TaskStorage
from app.scheduling.abstract import TaskScheduler


class TaskEngine:
    def __init__(self, task_repository: TaskStorage, task_scheduler: TaskScheduler):
        """
        :param repository: 任务存储仓库
        :param scheduler: 任务调度器
        """
        self.repository = task_repository
        self.scheduler = task_scheduler

    async def submit_task(self, task: Task) -> Task:

        saved_task = await self.repository.save(task)  # 添加await

        # 提交到调度系统
        await self.scheduler.add_job(  # 假设调度器也需异步操作
            task_id=saved_task.id,
            workflow=saved_task.workflow,
            trigger_type=task.scheduler_config.scheduler_type,
            max_retries=task.scheduler_config.max_retries,
            retry_interval=task.scheduler_config.retry_interval,
            timeout=task.scheduler_config.timeout,
            queue=saved_task.queue_name,
        )
        return saved_task

    async def cancel_task(self, task_id: UUID) -> bool:
        """
        取消已调度的任务
        1. 停止调度
        2. 更新任务状态
        """
        # 停止调度任务
        await self.scheduler.remove_job(str(task_id))

        # 获取并更新任务
        if task := await self.repository.load(task_id):
            task.status = TaskStatus.CANCELLED
            await self.repository.save(task)
            return True
        return False

    async def pause_task(self, task_id: UUID) -> bool:
        """暂停任务调度"""
        await self.scheduler.pause_job(str(task_id))
        if task := await self.repository.load(task_id):
            task.status = TaskStatus.PAUSED
            await self.repository.save(task)
            return True
        return False

    async def resume_task(self, task_id: UUID) -> bool:
        """恢复任务调度"""
        await self.scheduler.resume_job(str(task_id))
        if task := await self.repository.load(task_id):
            task.status = TaskStatus.PENDING
            await self.repository.save(task)
            return True
        return False


# import uuid
# from uuid import UUID
# from app.core.models import Task, SchedulerPolicy, Workflow
# from app.execution.executor import WorkflowExecutor
# from app.monitoring.dispatcher import EventDispatcher
# from app.persistence.adapters import TaskStorage
# from app.scheduling.abstract import TaskScheduler
#
#
# class TaskEngine:
#     """任务引擎，负责任务的提交、调度和执行协调"""
#
#     def __init__(
#             self,
#             repository: TaskStorage,
#             scheduler: TaskScheduler = None,
#             executor: WorkflowExecutor = None,
#             dispatcher: EventDispatcher = None
#     ):
#         """
#         :param repository: 任务存储仓库
#         :param scheduler: 任务调度器
#         :param executor: 工作流执行器
#         :param dispatcher: 事件分发器
#         """
#         self.repository = repository
#         self.scheduler = scheduler
#         self.executor = executor
#         self.dispatcher = dispatcher
#
#     def submit_task(self, task: Task) -> Task:
#         """
#         提交任务到引擎的主要入口方法
#         1. 持久化任务配置
#         2. 初始化任务调度
#         3. 返回最终任务对象
#         """
#         # 持久化任务配置
#         saved_task = self.repository.save(task)
#
#         # 定义任务执行包装函数
#         def job_wrapper():
#             """实际执行任务的包装函数"""
#             context = {
#                 "task_id": saved_task.id,
#                 "execution_id": uuid.uuid4(),
#                 "queue": saved_task.queue
#             }
#
#             try:
#                 # 执行前事件
#                 self.dispatcher.dispatch(
#                     "task_started",
#                     task_id=saved_task.id,
#                     context=context
#                 )
#
#
#                 # 执行工作流
#                 result = self.executor.execute(
#                     workflow=saved_task.workflow,
#                     context=context
#                 )
#
#                 # 成功处理
#                 for handler in saved_task.success_handlers:
#                     handler(saved_task, result)
#
#                 # 成功事件
#                 self.dispatcher.dispatch(
#                     "task_succeeded",
#                     task_id=saved_task.id,
#                     result=result,
#                     context=context
#                 )
#
#             except Exception as e:
#                 # 失败处理
#                 for handler in saved_task.failure_handlers:
#                     handler(saved_task, e)
#
#                 # 失败事件
#                 self.dispatcher.dispatch(
#                     "task_failed",
#                     task_id=saved_task.id,
#                     error=e,
#                     context=context
#                 )
#
#         # 配置调度参数
#         scheduler_config = saved_task.scheduler_config
#
#         # 注册调度任务 todo 待实现
#         # self.scheduler.add_job(
#         #     job_func=job_wrapper,
#         #     trigger_type=scheduler_config.scheduler_type,
#         #     trigger_args=scheduler_config.config,
#         #     job_id=str(saved_task.id),
#         #     queue=saved_task.queue.value
#         # )
#
#         return saved_task
#
#     # todo 待实现
#     def cancel_task(self, task_id: UUID) -> bool:
#         """
#         取消已调度的任务
#         1. 停止调度
#         2. 更新任务状态
#         """
#         # 停止调度任务
#         self.scheduler.remove_job(str(task_id))
#
#         # 获取并更新任务
#         if task := self.repository.load(task_id):
#             # 此处假设Task对象有状态字段
#             task.status = "CANCELLED"
#             self.repository.save(task)
#             return True
#         return False
#
#     # todo 待实现
#     def pause_task(self, task_id: UUID) -> bool:
#         """暂停任务调度"""
#         self.scheduler.pause_job(str(task_id))
#         if task := self.repository.load(task_id):
#             task.status = "PAUSED"
#             self.repository.save(task)
#             return True
#         return False
#
#     # todo 待实现
#     def resume_task(self, task_id: UUID) -> bool:
#         """恢复任务调度"""
#         self.scheduler.resume_job(str(task_id))
#         if task := self.repository.load(task_id):
#             task.status = "ACTIVE"
#             self.repository.save(task)
#             return True
#         return False

# def _retry_task(self, task: Task, retry_interval: int):
#     task.current_retry += 1
#     self.repository.save(task)
#     self.scheduler.add_job(
#         job_func=self.submit_task,
#         trigger_type="interval",
#         trigger_args={"seconds": retry_interval},
#         job_id=f"{task.id}_retry_{task.current_retry}",
#         queue=task.queue_name
#     )
