from dependency_injector import containers, providers

from app.core.database import Database
from app.settings.db import DatabaseSettings


class DatabaseContainer(containers.DeclarativeContainer):
    """Database  dependencies container."""

    settings = providers.Dependency(instance_of=DatabaseSettings)

    db = providers.Singleton(
        Database,
        db_url=settings.provided.URL,
    )
