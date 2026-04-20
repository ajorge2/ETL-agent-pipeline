from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    anthropic_api_key: str

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "user-signups"
    kafka_group_id: str = "etl-pipeline"

    postgres_dsn: str = "postgresql://etl_user:etl_pass@localhost:5433/etl_pipeline"

    approval_api_url: str = "http://localhost:8003"

    poll_interval_seconds: int = 2
    approval_timeout_seconds: int = 60

    class Config:
        env_file = ".env"


settings = Settings()
