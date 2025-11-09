"""Mock Tavily client for testing without API calls"""
from typing import Dict, Any
import asyncio
import logging
from .mock_data import get_mock_results, MOCK_DATA_MARKER, ENRICHMENT_MARKER

logger = logging.getLogger(__name__)

class MockTavilyClient:
    """Mock version of AsyncTavilyClient that returns test data instead of making API calls.
    Includes enrichment tracking and detailed mock data."""
    
    def __init__(self, api_key: str = "mock_key"):
        self.api_key = api_key
        self._current_analyst_type = "company_brief"  # Default
        
    def set_analyst_type(self, analyst_type: str):
        """Set the current analyst type for context in mock responses."""
        self._current_analyst_type = analyst_type
        
    async def search(self, query: str, **kwargs) -> Dict[str, Any]:
        """Mock search that returns test data instead of making an API call."""
        # Simulate network delay for realism (0.1-0.3 seconds)
        await asyncio.sleep(0.2)
        
        # Get mock results based on query and analyst type
        results = get_mock_results(query, self._current_analyst_type)
        
        # Log the mock search for visibility
        logger.info(f"{MOCK_DATA_MARKER} Running mock search for {self._current_analyst_type}")
        logger.info(f"Query: {query}")
        logger.info(f"Search parameters: {kwargs}")
        
        # Count enrichment markers to track enhancement
        enrichment_count = str(results).count(ENRICHMENT_MARKER)
        logger.info(f"Found {enrichment_count} enriched sections in mock data")
        
        return results