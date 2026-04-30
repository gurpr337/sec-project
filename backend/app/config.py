from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional

class Settings(BaseSettings):
    database_url: str
    sec_api_key: str
    sec_extractor_api_key: str
    pinecone_api_key: str
    vertex_project_id: str
    vertex_location: str
    vertex_service_account_json: Optional[str] = None

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()
