from typing import Annotated

from pydantic import UrlConstraints, RedisDsn
from pydantic_core import MultiHostUrl
from pydantic_settings import BaseSettings

TemporalDsn = Annotated[
    MultiHostUrl,
    UrlConstraints(
        allowed_schemes=["temporalio"],
        default_host="127.0.0.1",
        default_port=7233,
    ),
]


class SchedulerConfig(BaseSettings):
    temporal_dsn: TemporalDsn = "temporalio://127.0.0.1:7233"
    redis_dsn: RedisDsn = "redis://localhost:6379/0"

    def get_temporal_connection_string(self) -> str:
        return str(self.temporal_dsn).split("://")[1]
