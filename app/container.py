# import os
#
# from dependency_injector import containers, providers
# from temporalio.client import Client
#
# from app.engine import TaskEngine
# from app.persistence.adapters import OperatorStorage, TaskStorage
# from app.persistence.adapters.queue import QueueStorage
# from app.plugins.operators.registry import OperatorRegistry
# from app.scheduling.adapters.temporal.workflow_registrar import WorkflowRegistrar
# from app.scheduling.adapters.temporal.workflow_translator import WorkflowTranslator
# from app.scheduling.concurrency import ConcurrencyController
# from app.scheduling.dynamic_scheduler import DynamicScheduler
# from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
#
#
# class CoreContainer(containers.DeclarativeContainer):
#     """基础设施层容器（仅包含纯技术组件）"""
#
#     config = providers.Configuration()
#     settings = providers.Singleton(get_settings)
#
#     db_engine = providers.Singleton(
#         create_async_engine,
#         url=settings.provided.DATABASE.provided.url,
#         echo=True,
#     )
#
#     temporal_config = providers.Singleton(
#         TemporalSettings,
#         host=config.temporal_host,
#         port=config.temporal_port,
#         namespace=config.temporal_namespace,
#         identity=config.temporal_identity,
#         tls_enabled=config.temporal_tls_enabled,
#         server_cert=config.temporal_server_cert,
#         client_key=config.temporal_client_key
#     )
#     # settings = providers.Singleton(Settings)
#     temporal_target = providers.Callable(
#         lambda host, port: f"{host}:{port}",
#         temporal_config.provided.host,
#         temporal_config.provided.port
#     )
#
#     # 使用配置化的 Temporal 客户端
#     temporal_client = providers.Resource(
#         Client.connect,
#         target_host=temporal_target,
#         namespace=temporal_config.provided.namespace,
#         identity=temporal_config.provided.identity,
#         tls=temporal_config.provided.tls,
#     )
#
# class BusinessContainer(containers.DeclarativeContainer):
#     """业务逻辑层容器（需要显式声明所有依赖）"""
#     core = providers.DependenciesContainer()
#
#     # 显式声明会话工厂
#     db_session_factory = providers.Factory(
#         async_sessionmaker,
#         bind=core.db_engine,
#         autocommit=False,
#         autoflush=False
#     )
#     # 显式声明operator_storage的依赖链
#     operator_storage = providers.Singleton(
#         OperatorStorage,
#         session_factory=db_session_factory
#     )
#     operator_registry = providers.Singleton(
#         OperatorRegistry,
#         storage=operator_storage
#     )
#     task_storage = providers.Singleton(
#         TaskStorage,
#         session_factory=db_session_factory,
#         operator_registry=operator_registry
#     )
#     queue_storage = providers.Singleton(
#         QueueStorage,
#         session_factory=db_session_factory
#     )
#     concurrency_controller = providers.Singleton(
#         ConcurrencyController,
#         task_storage=task_storage,
#         queue_storage=queue_storage
#     )
#
#     workflow_registrar = providers.Singleton(
#         WorkflowRegistrar,
#         client=core.temporal_client,
#         worker_options=providers.Dict(
#             max_concurrent_workflow_tasks=core.settings.provided.temporal.max_concurrent_workflows,
#             max_concurrent_activities=core.settings.provided.temporal.max_concurrent_activities
#         )
#     )
#
#     workflow_translator = providers.Factory(
#         WorkflowTranslator,
#         operator_registry=operator_registry
#     )
#     # 调度器组件
#     dynamic_scheduler = providers.Factory(
#         DynamicScheduler,
#         client=core.temporal_client,
#         translator=workflow_translator,
#         controller=concurrency_controller,
#         storage=task_storage
#     )
#
#
# class ApplicationContainer(containers.DeclarativeContainer):
#     """应用层容器（组合所有组件）"""
#     core = providers.Container(CoreContainer)
#     business = providers.Container(BusinessContainer, core=core)
#
#
#     task_engine = providers.Factory(
#         TaskEngine,
#         repository=business.task_storage,
#         scheduler=business.dynamic_scheduler
#     )
#
#     # 分层架构清晰可见
#     # +----------------+      +-----------------+      +-------------------+
#     # | CoreContainer | ---> | BusinessContainer | ---> | ApplicationContainer |
#     # +----------------+      +-----------------+      +-------------------+
#
#     # graph
#     # TD
#     # A[TaskEngine] --> | 依赖 | B[TaskStorage]
#     # A --> | 依赖 | C[APScheduler]
#     # A --> | 依赖 | D[WorkflowExecutor]
#     # D --> | 使用 | E[OperatorRegistry]
#     # E --> | 持久化 | F[OperatorStorage]
#     # F --> | 共享连接 | G[DatabaseSession]
#     # B --> | 共享连接 | G
