import asyncio

from subscriptions_service.config import Settings
from subscriptions_service.consumers.app import create_app
from subscriptions_service.logs import configure_logging


def main() -> None:
    settings = Settings()
    configure_logging(settings.log_level)
    asyncio.run(create_app(settings).run())


main()
