import os

class Settings:
    PROJECT_NAME: str = "RedAlert"
    VERSION: str = "5.0.0"
    DESCRIPTION: str = "Real-time ambulance dispatch system"
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_PASSWORD: str = os.getenv("REDIS_PASSWORD", None)

settings = Settings()
