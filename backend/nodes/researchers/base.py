import asyncio
import logging
import os
from typing import Any, Dict, List

from tavily import AsyncTavilyClient

from ...classes import ResearchState
from ...utils.references import clean_title

logger = logging.getLogger(__name__)

class BaseResearcher:
    def __init__(self):
        tavily_key = os.getenv("TAVILY_API_KEY")
        
        if not tavily_key:
            raise ValueError("Missing TAVILY_API_KEY")
            
        self.tavily_client = AsyncTavilyClient(api_key=tavily_key)
        self.analyst_type = "base_researcher"  # Default type

    @property
    def analyst_type(self) -> str:
        if not hasattr(self, '_analyst_type'):
            raise ValueError("Analyst type not set by subclass")
        return self._analyst_type

    @analyst_type.setter
    def analyst_type(self, value: str):
        self._analyst_type = value

    async def search_documents(self, state: ResearchState) -> Dict[str, Any]:
        """
        Execute all Tavily searches in parallel for the queries assigned
        to this researcher's analyst_type.
        """
        websocket_manager = state.get('websocket_manager')
        job_id = state.get('job_id')
        
        # Get the queries from the state, populated by the QueryGeneratorNode
        try:
            queries = state.get('research_queries', {}).get(self.analyst_type, [])
            if not queries:
                logger.error(f"No queries found in state for analyst: {self.analyst_type}")
                return {}
        except Exception as e:
            logger.error(f"Error accessing queries from state for {self.analyst_type}: {e}")
            return {}

        # Send status update for generated queries
        if websocket_manager and job_id:
            await websocket_manager.send_status_update(
                job_id=job_id,
                status="queries_generated", # This status now means "queries received"
                message=f"Received {len(queries)} queries for {self.analyst_type}",
                result={
                    "step": "Searching",
                    "analyst": self.analyst_type,
                    "queries": queries,
                    "total_queries": len(queries)
                }
            )

        # Prepare all search parameters upfront
        search_params = {
            "search_depth": "basic",
            "include_raw_content": False,
            "max_results": 3 #tweak this knob back to 5 eventually. 3 for testing
        }
        
        # v2: Renamed news_analyst to news_signal
        if self.analyst_type == "news_signal":
            search_params["topic"] = "news"
        # v2: financial_analyst is no longer a node, this can be removed or kept
        # elif self.analyst_type == "financial_analyst":
        #     search_params["topic"] = "finance"

        if websocket_manager and job_id:
            await websocket_manager.send_status_update(
                job_id=job_id,
                status="search_started",
                message=f"Using Tavily to search for {len(queries)} queries ({self.analyst_type})",
                result={
                    "step": "Searching",
                    "total_queries": len(queries)
                }
            )
            
        # Create all API calls upfront - direct Tavily client calls
        search_tasks = [
            self.tavily_client.search(query, **search_params)
            for query in queries
        ]

        # Execute all API calls in parallel
        try:
            results = await asyncio.gather(*search_tasks)
        except Exception as e:
            logger.error(f"Error during parallel search execution for {self.analyst_type}: {e}")
            return {}

        # Process results
        merged_docs = {}
        for query, result in zip(queries, results):
            for item in result.get("results", []):
                if not item.get("content") or not item.get("url"):
                    continue
                    
                url = item.get("url")
                title = item.get("title", "")
                
                if title:
                    title = clean_title(title)
                    if title.lower() == url.lower() or not title.strip():
                        title = ""

                merged_docs[url] = {
                    "title": title,
                    "content": item.get("content", ""),
                    "query": query,
                    "url": url,
                    "source": "web_search",
                    "score": item.get("score", 0.0)
                }

        # Send completion status
        if websocket_manager and job_id:
            await websocket_manager.send_status_update(
                job_id=job_id,
                status="search_complete",
                message=f"Search for {self.analyst_type} completed with {len(merged_docs)} documents",
                result={
                    "step": "Searching",
                    "total_documents": len(merged_docs),
                    "queries_processed": len(queries)
                }
            )

        return merged_docs