"""Configuration settings for the application."""

import os
from typing import List, Optional
from dotenv import load_dotenv

load_dotenv()

class Settings:
    """Application settings with validation."""
    
    def __init__(self):
        # Application settings
        self.app_name: str = "Logical Data Modeling Assistant API"
        self.app_version: str = "1.0.0"
        self.debug: bool = self._get_bool_env("DEBUG", False)
        
        # Server settings
        self.host: str = os.getenv("HOST", "0.0.0.0")
        self.port: int = int(os.getenv("PORT", "8000"))
        
        # Default user ID for demo purposes
        self.default_user_id: str = os.getenv("DEFAULT_USER_ID", "demo-user")
        
        # LLM API Configuration
        self.llm_base_url: str = os.getenv(
            "LLM_BASE_URL",
            "https://language-model-service.mangobeach-c18b898d.switzerlandnorth.azurecontainerapps.io/api/v2/openai/text/"
        )
        self.llm_api_key: str = os.getenv("LLM_API_KEY", "LMS_API_KEY")
        self.llm_model: str = os.getenv("LLM_MODEL", "gpt-4o")
        self.llm_max_tokens: int = int(os.getenv("LLM_MAX_TOKENS", "1000"))
        self.llm_timeout: int = int(os.getenv("LLM_TIMEOUT", "30"))
        
        # CORS Configuration
        cors_origins_str = os.getenv("CORS_ORIGINS", "*")
        self.cors_origins: List[str] = [cors_origins_str] if cors_origins_str != "*" else ["*"]
        self.cors_credentials: bool = self._get_bool_env("CORS_CREDENTIALS", True)
        self.cors_methods: List[str] = ["*"]  # Simplified for now
        self.cors_headers: List[str] = ["*"]  # Simplified for now
        
        # Logging Configuration
        self.log_level: str = os.getenv("LOG_LEVEL", "INFO")
        self.log_file_size: int = int(os.getenv("LOG_FILE_SIZE", str(10*1024*1024)))  # 10MB
        self.log_backup_count: int = int(os.getenv("LOG_BACKUP_COUNT", "5"))
        
        # Security settings
        self.max_request_size: int = int(os.getenv("MAX_REQUEST_SIZE", str(1024*1024)))  # 1MB
        self.rate_limit_requests: int = int(os.getenv("RATE_LIMIT_REQUESTS", "100"))
        self.rate_limit_window: int = int(os.getenv("RATE_LIMIT_WINDOW", "3600"))  # 1 hour
    
    def _get_bool_env(self, key: str, default: bool) -> bool:
        """Get boolean environment variable."""
        value = os.getenv(key, str(default)).lower()
        return value in ("true", "1", "yes", "on")

# Create settings instance
settings = Settings()

# Backward compatibility
DEFAULT_USER_ID = settings.default_user_id
LLM_BASE_URL = settings.llm_base_url
LLM_API_KEY = settings.llm_api_key
LLM_MODEL = settings.llm_model
LLM_MAX_TOKENS = settings.llm_max_tokens
CORS_ORIGINS = settings.cors_origins
CORS_CREDENTIALS = settings.cors_credentials
CORS_METHODS = settings.cors_methods
CORS_HEADERS = settings.cors_headers 