from dependency_injector import containers, providers

from app.core.database import Database
from app.engine import TaskEngine
from app.plugins.operators.registry import OperatorRegistry
from app.scheduling.adapters.temporal.workflow_registrar import WorkflowRegistrar
from app.scheduling.adapters.temporal.workflow_translator import WorkflowTranslator
from app.scheduling.concurrency import ConcurrencyController
from app.scheduling.dynamic_scheduler import DynamicScheduler
from app.settings import Settings


class ServicesContainer(containers.DeclarativeContainer):
    """Services related dependencies container."""

    repositories = providers.DependenciesContainer()
    db = providers.Dependency(instance_of=Database)
    settings = providers.Dependency(instance_of=Settings)
    clients = providers.DependenciesContainer()

    operator_registry = providers.Singleton(
        OperatorRegistry,
        operator_repository=repositories.operator_repository,
    )

    concurrency_controller = providers.Singleton(
        ConcurrencyController,
        task_storage=repositories.task_repository,
        queue_storage=repositories.queue_repository
    )

    workflow_registrar = providers.Singleton(
        WorkflowRegistrar,
        client=clients.temporal_client,
        worker_options=providers.Dict(
            max_concurrent_workflow_tasks=settings.provided.temporal.max_concurrent_workflows,
            max_concurrent_activities=settings.provided.temporal.max_concurrent_activities
        )
    )
    workflow_translator = providers.Factory(
        WorkflowTranslator,
        # operator_registry=operator_registry
    )
    task_scheduler = providers.Factory(
        DynamicScheduler,
        temporal_client=clients.temporal_client,
        translator=workflow_translator,
        controller=concurrency_controller,
        task_repository=repositories.task_repository,
    )
    task_engine = providers.Factory(
        TaskEngine,
        task_repository=repositories.task_repository,
        task_scheduler=task_scheduler
    )
