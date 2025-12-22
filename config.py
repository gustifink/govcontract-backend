"""
GovContract-Alpha Configuration
"""
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # Database
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/govcontract"
    
    # SAM.gov API
    sam_gov_api_key: str = ""
    sam_gov_base_url: str = "https://api.sam.gov"
    
    # App Settings
    debug: bool = True
    pipeline_interval_minutes: int = 60  # Auto-fetch every hour
    
    # Filtering Thresholds
    min_impact_ratio: float = 5.0
    max_market_cap: int = 50_000_000_000  # $50B
    min_award_amount: int = 1_000_000  # $1M
    fuzzy_match_threshold: int = 90

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    return Settings()
