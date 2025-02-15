import logging
import os
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict

from app.settings.db import DatabaseSettings
from app.settings.temporal import TemporalSettings


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
BASE_DIR = Path(__file__).resolve().parent.parent.parent


@lru_cache
def get_env_path() -> Path | None:
    import importlib

    try:
        importlib.import_module("dotenv")

        load_dotenv()
        env_path = BASE_DIR / ".env"
        print(os.environ)
        if env_path.exists():
            logger.info(f"Loading environment from: {env_path}")
            return env_path
        else:
            logger.warning(f"Environment file not found at: {env_path}")
            return None
    except ImportError:
        logger.warning("python-dotenv not installed, skipping .env file loading")
        return None


class Settings(BaseSettings):
    TEMPORAL: TemporalSettings = TemporalSettings()
    DATABASE: DatabaseSettings = DatabaseSettings()

    model_config = SettingsConfigDict(
        env_file=".env",  # 确保路径正确（建议用绝对路径）
        env_file_encoding="utf-8",
        env_nested_delimiter="__",  # 分隔符为双下划线
        extra="allow",
    )


@lru_cache()
def get_settings() -> Settings:
    env_path = get_env_path()
    print(f"env path: {env_path}")
    if env_path:
        logger.info(f"Using environment file: {env_path}")
        settings = Settings(_env_file=env_path)

    else:
        logger.warning("No environment file found, using environment variables only")
        settings = Settings()

    # 打印关键配置信息
    logger.info("Settings loaded with:")
    logger.info(f"Environment file: {env_path or 'Not found'}")
    logger.info(f"Database URL: {settings.DATABASE.URL}")
    logger.info(f"Database Engine: {settings.DATABASE.ENGINE}")

    return settings


# 创建全局设置实例
settings = get_settings()
