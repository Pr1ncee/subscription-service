from pathlib import Path

from alembic.config import Config

ROOT = Path(__file__).resolve().parent.parent


def alembic_config(database_url: str) -> Config:
    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(ROOT / "src/subscriptions_service/db/migrations"))
    cfg.attributes["database_url"] = database_url
    return cfg
