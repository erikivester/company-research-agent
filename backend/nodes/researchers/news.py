import logging
from typing import Any, Dict

from langchain_core.messages import AIMessage

from ...classes import ResearchState
from .base import BaseResearcher

logger = logging.getLogger(__name__)


class NewsSignalNode(BaseResearcher):
    def __init__(self) -> None:
        super().__init__()
        # v2: Renamed analyst_type for UI/logging
        self.analyst_type = "news_signal" 

    async def analyze(self, state: ResearchState) -> Dict[str, Any]:
        company = state.get('company', 'Unknown Company')
        msg = [f"📰 News Signal Node analyzing {company}"]
        
        # v2: Updated query generation prompt for ReFED-specific signals
        queries = await self.generate_queries(state, f"""
        You are a research analyst for ReFED, a nonprofit focused on food loss and waste (FLW).
        Generate 4 distinct search queries to find news and "opportunity signals" about "{company}" from the **last 12-18 months**.
        
        Construct queries that include a mix of:
        1.  One general query for major announcements (e.g., '"{company}" major announcements 2024 2025', '"{company}" product launches 2024 2025').
        2.  Three "signal" queries looking for specific ReFED-relevant hooks.
        
        "Signal" queries should hunt for:
        - **FLW/Climate/Methane:** Mentions of "food waste", "food loss", "methane", "ESG report", "sustainability goals". (e.g., '"{company}" food waste initiatives 2024 2025', '"{company}" methane reduction ESG report')
        - **Opportunity Windows:** "new VP of impact", "new sustainability lead", "secured new funding 2024 2025", "new corporate initiatives 2024 2025".
        - **Risk/Financial Health:** "layoffs", "stock price drop", "boycott", "regulatory issues". (e.g., '"{company}" layoffs 2024 2025', '"{company}" consumer boycott')
        """)

        subqueries_msg = "🔍 Subqueries for news & signals:\n" + "\n".join([f"• {query}" for query in queries])
        messages = state.get('messages', [])
        messages.append(AIMessage(content=subqueries_msg))
        state['messages'] = messages
        
        news_signal_data = {}
        
        # Include site_scrape data for news analysis
        if site_scrape := state.get('site_scrape'):
            msg.append(f"\n📊 Including {len(site_scrape)} pages from company website...")
            news_signal_data.update(site_scrape)

        # Perform additional research
        try:
            # Store documents with their respective queries
            for query in queries:
                # Note: We rely on the prompt's time-window (2024 2025)
                # The 'topic="news"' filter is applied in base.py based on analyst_type
                documents = await self.search_documents(state, [query])
                if documents:  # Only process if we got results
                    for url, doc in documents.items():
                        doc['query'] = query  # Associate each document with its query
                        news_signal_data[url] = doc
            
            msg.append(f"\n✓ Found {len(news_signal_data)} documents")
            if websocket_manager := state.get('websocket_manager'):
                if job_id := state.get('job_id'):
                    await websocket_manager.send_status_update(
                        job_id=job_id,
                        status="processing",
                        message=f"Used Tavily Search to find {len(news_signal_data)} documents",
                        result={
                            "step": "Searching",
                            "analyst_type": self.analyst_type, # v2: Uses new type
                            "queries": queries
                        }
                    )
        except Exception as e:
            msg.append(f"\n⚠️ Error during research: {str(e)}")
        
        # Update state with our findings
        messages = state.get('messages', [])
        messages.append(AIMessage(content="\n".join(msg)))
        
        # v2: Update state with the new key
        state['news_signal_data'] = news_signal_data
        
        return {
            'message': msg,
            'news_signal_data': news_signal_data # v2: Uses new key
        }

    async def run(self, state: ResearchState) -> ResearchState:
        """
        Entry point for the LangGraph node execution.
        Calls the analyze method and returns the updated state.
        """
        try:
            await self.analyze(state)
        except Exception as e:
             logger.error(f"NewsSignalNode run failed: {e}")
             # Ensure key exists even on failure
             if 'news_signal_data' not in state:
                state['news_signal_data'] = {}
        return state # Always return the state