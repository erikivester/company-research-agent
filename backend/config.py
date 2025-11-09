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
        
        # Log configuration state
        if self.USE_MOCK_DATA:
            logger.info("🔧 Running in MOCK mode - using sample data for research")
        else:
            if not self.TAVILY_API_KEY:
                logger.warning("⚠️ No Tavily API key found - defaulting to MOCK mode")
                self.USE_MOCK_DATA = True
            else:
                logger.info("🔧 Running in LIVE mode - using Tavily API for research")

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

# Create a singleton instance
config = Config()