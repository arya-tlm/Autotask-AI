"""
Configuration management for the Autotask API
"""
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""
    
    # API Configuration
    app_name: str = "Autotask AI"
    app_version: str = "1.0.0"
    debug: bool = False
    
    # Server Configuration
    host: str = "0.0.0.0"
    port: int = 8800
    
    # Supabase Configuration
    supabase_url: str
    supabase_key: str
    
    # OpenAI Configuration
    openai_api_key: str
    openai_model: str = "gpt-4o"
    openai_mini_model: str = "gpt-4o-mini"
    
    # Autotask Configuration
    autotask_username: str
    autotask_password: str
    autotask_integration_code: str
    autotask_zone_url: str = "https://webservices15.autotask.net"
    
    # API Limits
    max_tickets_per_request: int = 500
    max_concurrent_requests: int = 5
    max_fetch_limit: int = 1000
    default_search_limit: int = 100
    max_search_limit: int = 1000
    
    # CORS
    cors_origins: list = ["*"]
    
    class Config:
        env_file = ".env"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance"""
    return Settings()
