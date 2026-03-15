from __future__ import annotations

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


# Enviroment variables to set:
#
# export TRINO_TRINO__HOST=trino.trino.svc.cluster.local
# export TRINO_TRINO__PORT=8080
# export TRINO_TRINO__HTTP_SCHEME=http
# export TRINO_TRINO__USER=admin
# export TRINO_TRINO__CATALOG=iceberg

class TrinoSettings(BaseModel):
    host: str = "trino.trino.svc.cluster.local"
    port: int = 8080
    user: str = "admin"
    catalog: str = "iceberg"

    schema_name: str = Field(default="default", alias="schema")

    http_scheme: str = "http"  # or "https"

    model_config = {"populate_by_name": True}


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TRINO_", env_nested_delimiter="__")

    engine: str = Field(default="trino", description="Execution engine (trino, ...)")
    trino: TrinoSettings = Field(default_factory=TrinoSettings)
