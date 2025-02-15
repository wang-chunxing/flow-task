from dependency_injector import containers, providers

from app.core.containers.clients import ClientsContainer
from app.core.containers.database import DatabaseContainer
from app.core.containers.repositories import RepositoriesContainer
from app.core.containers.services import ServicesContainer
from app.settings import get_settings


class Container(containers.DeclarativeContainer):
    """Main container."""

    wiring_config = containers.WiringConfiguration(
        modules=[
        ]
    )

    # Configuration
    config = providers.Configuration()
    settings = providers.Singleton(get_settings)

    database = providers.Container(
        DatabaseContainer,
        settings=settings.provided.DATABASE,
    )
    clients = providers.Container(
        ClientsContainer,
        settings=settings,
    )
    repositories = providers.Container(
        RepositoriesContainer,
        db=database.db,
    )
    services = providers.Container(
        ServicesContainer,
        settings=settings,
        db=database.db,
        clients=clients,
        repositories=repositories,
    )


