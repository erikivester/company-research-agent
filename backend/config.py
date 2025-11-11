import os
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class Config:
    """Central configuration management for the research agent."""
    
    def __init__(self):
        # Load settings from environment variables
        self.USE_MOCK_DATA = os.getenv("USE_MOCK_DATA", "false").lower() == "true"
        self.TAVILY_API_KEY = os.getenv("TAVILY_API_KEY")
        self.API_KEY = os.getenv("API_KEY", "default-dev-key-please-change")

        # Local context settings (to reduce Tavily calls)
        self.USE_LOCAL_FILES = os.getenv("USE_LOCAL_FILES", "false").lower() == "true"
        self.USE_LOCAL_ONLY = os.getenv("USE_LOCAL_ONLY", "false").lower() == "true"
        # Comma-separated list of directories relative to project root
        self.LOCAL_CONTEXT_DIRS_RAW = os.getenv(
            "LOCAL_CONTEXT_DIRS",
            "archive/reports,archive/pdfs,pdfs,archive/docs"
        )
        
        # Security settings
        self.JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-secret-key-for-development")
        self.ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")
        self.RATE_LIMIT_REQUESTS = int(os.getenv("RATE_LIMIT_REQUESTS", "100"))
        self.RATE_LIMIT_PERIOD = int(os.getenv("RATE_LIMIT_PERIOD", "3600"))  # 1 hour in seconds
        
        # Log configuration state
        if self.USE_MOCK_DATA:
            logger.info("🔧 Running in MOCK mode - using sample data for research")
        else:
            if not self.TAVILY_API_KEY:
                logger.warning("⚠️ No Tavily API key found - defaulting to MOCK mode")
                self.USE_MOCK_DATA = True
            else:
                logger.info("🔧 Running in LIVE mode - using Tavily API for research")
        
        # Log local context configuration
        if self.USE_LOCAL_FILES:
            logger.info("📂 Local context enabled - will read existing files before web search")
        if self.USE_LOCAL_ONLY:
            logger.info("🚫 Web search disabled - using only local files for context")
                
        # Log security configuration
        logger.info(f"🔒 Security enabled - Rate limit: {self.RATE_LIMIT_REQUESTS} requests per {self.RATE_LIMIT_PERIOD}s")

    @property
    def is_mock_mode(self) -> bool:
        """Returns whether the system should use mock data."""
        return self.USE_MOCK_DATA

    def get_tavily_client(self):
        """Returns the appropriate Tavily client based on configuration."""
        if self.is_mock_mode:
            from .utils.mock_tavily import MockTavilyClient
            return MockTavilyClient()
        else:
            from tavily import AsyncTavilyClient
            return AsyncTavilyClient(api_key=self.TAVILY_API_KEY)

    def get_local_context_dirs(self):
        """Return absolute paths for configured local context directories."""
        from pathlib import Path
        root = Path(__file__).resolve().parents[1]  # project root
        dirs = [d.strip() for d in self.LOCAL_CONTEXT_DIRS_RAW.split(',') if d.strip()]
        return [str((root / d).resolve()) for d in dirs]

# Create a singleton instance
config = Config()