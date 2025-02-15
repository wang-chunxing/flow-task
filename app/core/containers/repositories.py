from dependency_injector import containers, providers

from app.core.database import Database
from app.persistence import TaskStorage
from app.persistence.adapters import OperatorStorage
from app.persistence.adapters.my_queue import QueueStorage


class RepositoriesContainer(containers.DeclarativeContainer):
    """Repository related dependencies container."""

    db = providers.Dependency(instance_of=Database)

    operator_repository = providers.Factory(
        OperatorStorage,
        session_factory=db.provided.get_session,
    )

    queue_repository = providers.Factory(
        QueueStorage,
        session_factory=db.provided.get_session,
    )

    task_repository = providers.Factory(
        TaskStorage,
        session_factory=db.provided.get_session,
        operator_repository=operator_repository
    )
