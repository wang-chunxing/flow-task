from pydantic import ValidationInfo, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional

class DatabaseSettings(BaseSettings):
    USER: str = ""
    PASSWORD: str = ""
    HOST: str = "127.0.0.1"
    PORT: str = "3306"
    ENGINE: str = "mysql"
    DATABASE: str = "mydb"
    URL: Optional[str] = None

    @field_validator("URL", mode="before")
    def assemble_db_connection(cls, v: str, info: ValidationInfo):
        if v is None:
            db_engine = info.data.get('ENGINE', '')
            db_user = info.data.get('USER', '')
            db_password = info.data.get('PASSWORD', '')
            db_host = info.data.get('HOST', '')
            db_port = info.data.get('PORT', '')
            db_database = info.data.get('DATABASE', '')

            if not all([db_engine, db_user, db_host, db_port, db_database]):
                print("Warning: Some database configuration values are missing")

            if db_engine == "mysql":
                return f"mysql+aiomysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_database}?charset=utf8mb4"
            elif db_engine == "postgresql":
                return f"postgresql+asyncpg://{db_user}:{db_password}@{db_host}:{db_port}/{db_database}"
            elif db_engine == "sqlite":
                return f"sqlite+aiosqlite:///{db_database}"
            else:
                raise ValueError(f"Unsupported DB_ENGINE:{db_engine}. Choose from 'mysql', 'postgresql', or 'sqlite'.")
        return v

    def get_sync_url(self):
        if self.ENGINE == "mysql":
            return f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4"
        elif self.ENGINE == "postgresql":
            return f"postgresql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}"
        elif self.ENGINE == "sqlite":
            return f"sqlite:///{self.DATABASE}"
        return self.URL

    model_config = SettingsConfigDict(extra="allow")