from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    postgres_host: str = "localhost"
    postgres_database_name: str = "search_db"
    postgres_password: str = "postgres"
    postgres_port: int = 5435
    postgres_username: str = "postgres"

    log_level: str = "INFO"

    kafka_bootstrap_servers: str = Field("localhost:9092", alias="KAFKA_BROKERS")
    kafka_topic_ads: str = "ads"
    kafka_consumer_group: str = "search-service"
    ad_service_url: str = "http://localhost:8002"

    @property
    def database_url(self) -> str:
        return f"postgresql+asyncpg://{self.postgres_username}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_database_name}"
