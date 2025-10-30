from typing import Any, Dict

from langchain_core.messages import AIMessage

from ...classes import ResearchState
from .base import BaseResearcher


class CompanyBriefNode(BaseResearcher):
    def __init__(self) -> None:
        super().__init__()
        # v2: Renamed analyst_type
        self.analyst_type = "company_brief"

    async def analyze(self, state: ResearchState) -> Dict[str, Any]:
        company = state.get('company', 'Unknown Company')
        msg = [f"🏢 Company Brief Node analyzing {company}"]
        
        # v2: Updated query generation prompt
        queries = await self.generate_queries(state, """
        Generate queries on the company fundamentals of {company} in the {industry} industry such as:
        - Core products and services offered.
        - Primary business lines and revenue streams.
        - Ballpark annual revenue or company size.
        - Recent financial health signals, such as major stock price changes, layoffs, or funding announcements.
        """)

        # Add message to show subqueries with emojis
        subqueries_msg = "🔍 Subqueries for company brief:\n" + "\n".join([f"• {query}" for query in queries])
        messages = state.get('messages', [])
        messages.append(AIMessage(content=subqueries_msg))
        state['messages'] = messages

        # Send queries through WebSocket
        if websocket_manager := state.get('websocket_manager'):
            if job_id := state.get('job_id'):
                await websocket_manager.send_status_update(
                    job_id=job_id,
                    status="processing",
                    message="Company brief queries generated",
                    result={
                        "step": "Company Brief", # v2: Updated step name
                        "analyst_type": self.analyst_type, # v2: Uses new type
                        "queries": queries
                    }
                )
        
        company_brief_data = {}
        
        # If we have site_scrape data, include it first
        if site_scrape := state.get('site_scrape'):
            msg.append(f"\n📊 Including {len(site_scrape)} pages from company website...")
            company_brief_data.update(site_scrape)
        
        # Perform additional research with comprehensive search
        try:
            # Store documents with their respective queries
            for query in queries:
                documents = await self.search_documents(state, [query])
                if documents:  # Only process if we got results
                    for url, doc in documents.items():
                        doc['query'] = query  # Associate each document with its query
                        company_brief_data[url] = doc
            
            msg.append(f"\n✓ Found {len(company_brief_data)} documents")
            if websocket_manager := state.get('websocket_manager'):
                if job_id := state.get('job_id'):
                    await websocket_manager.send_status_update(
                        job_id=job_id,
                        status="processing",
                        message=f"Used Tavily Search to find {len(company_brief_data)} documents",
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
        state['messages'] = messages
        
        # v2: Update state with the new key
        state['company_brief_data'] = company_brief_data
        
        return {
            'message': msg,
            'company_brief_data': company_brief_data # v2: Uses new key
        }

    async def run(self, state: ResearchState) -> ResearchState:
        """
        Entry point for the LangGraph node execution.
        Calls the analyze method and returns the updated state.
        """
        try:
            await self.analyze(state)
        except Exception as e:
             logger.error(f"CompanyBriefNode run failed: {e}")
             # Ensure key exists even on failure
             if 'company_brief_data' not in state:
                state['company_brief_data'] = {}
        return state # Always return the state