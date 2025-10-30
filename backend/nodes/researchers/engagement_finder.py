# backend/nodes/researchers/engagement_finder.py
import logging
from typing import Any, Dict

from langchain_core.messages import AIMessage

# Use relative imports assuming standard project structure
from ...classes import ResearchState
from .base import BaseResearcher

logger = logging.getLogger(__name__)

class EngagementFinderNode(BaseResearcher):
    """
    (v2) A new researcher node dedicated to finding a company's external
    engagements, affiliations, partnerships, and awards, which act as
    strong signals for outreach.
    """
    def __init__(self) -> None:
        super().__init__()
        # Set a specific analyst type for this researcher
        self.analyst_type = "engagement_finder"
        logger.info("Engagement Finder Node initialized.")

    async def analyze(self, state: ResearchState) -> Dict[str, Any]:
        """
        Analyzes public information for signals of external engagement.
        """
        company = state.get('company', 'Unknown Company')
        industry = state.get('industry', 'Unknown Industry') # Get industry for context
        websocket_manager = state.get('websocket_manager')
        job_id = state.get('job_id')

        # Initial message for logging and state update
        msg = [f"🛰️ Engagement Finder Node hunting for signals at {company}"]
        logger.info(f"Starting engagement finding for {company}")

        try:
            # v2: Generate search queries to hunt for "creative signals"
            queries = await self.generate_queries(
                state,
                f"""
                Generate creative search queries to hunt for external signals of engagement for "{company}". 
                We are looking for affiliations, partnerships, memberships, and awards that suggest an interest in sustainability, food waste, or corporate responsibility.

                Focus on finding:
                - **Memberships:** '"{company}" 1% for the Planet', '"{company}" US Food Waste Pact', '"{company}" B Corp certified', '"{company}" Ceres member'.
                - **Event Participation:** '"{company}" speaker ReFED Summit', '"{company}" attended Systems Change Lab', '"{company}" sponsor Aspen Institute'.
                - **Awards & Recognition:** '"{company}" sustainability award 2024', '"{company}" Fast Company most innovative', '"{company}" Dow Jones Sustainability Index'.
                - **Nonprofit Partnerships:** '"{company}" partners with Feeding America', '"{company}" World Wildlife Fund partnership', '"{company}" nonprofit partners'.
                - **Coalition Signatory:** '"{company}" Consumer Goods Forum', '"{company}" Food Marketing Institute FMI'.
                """
            )

            # Add generated queries to state messages for transparency
            subqueries_msg = "🔍 Subqueries for engagement finding:\n" + "\n".join([f"• {query}" for query in queries])
            messages = state.get('messages', [])
            messages.append(AIMessage(content=subqueries_msg))
            state['messages'] = messages

            # Send WebSocket update: Queries generated
            if websocket_manager and job_id:
                await websocket_manager.send_status_update(
                    job_id=job_id,
                    status="processing",
                    message="Engagement finder queries generated",
                    result={
                        "step": "Engagement Finder",
                        "analyst_type": self.analyst_type,
                        "queries": queries
                    }
                )

            # Initialize dictionary to store research results
            engagement_finder_data = {}

            # Include relevant data from the initial website scrape if available
            if site_scrape := state.get('site_scrape'):
                msg.append(f"\n📊 Including {len(site_scrape)} pages from company website...")
                engagement_finder_data.update(site_scrape)
                logger.info(f"Included {len(site_scrape)} site scrape results.")

            # Execute searches for the generated queries
            logger.info(f"Searching documents for {len(queries)} engagement queries.")
            documents_found = await self.search_documents(state, queries)

            if documents_found:
                # Add found documents, associating each with its query
                for url, doc in documents_found.items():
                    doc['query'] = doc.get('query', 'Unknown Query')
                    engagement_finder_data[url] = doc
                msg.append(f"\n✓ Found {len(documents_found)} documents from web search.")
                logger.info(f"Found {len(documents_found)} documents from web search.")
            else:
                 msg.append("\nℹ️ No additional documents found from web search for engagements.")
                 logger.info("No additional documents found from web search.")

            # Send WebSocket update: Search complete
            if websocket_manager and job_id:
                 await websocket_manager.send_status_update(
                     job_id=job_id,
                     status="processing",
                     message=f"Found {len(documents_found)} documents for engagements",
                     result={
                         "step": "Searching",
                         "analyst_type": self.analyst_type,
                         "queries": queries,
                         "documents_found": len(documents_found)
                     }
                 )

            # Update state with findings
            messages = state.get('messages', [])
            messages.append(AIMessage(content="\n".join(msg)))
            state['messages'] = messages
            
            # Use the specific key from our new v2 state.py
            state['engagement_finder_data'] = engagement_finder_data
            logger.info(f"Completed engagement finding. Total documents collected: {len(engagement_finder_data)}")

            return {
                'message': "\n".join(msg),
                'engagement_finder_data': engagement_finder_data
            }

        except Exception as e:
            error_msg = f"Engagement finding failed: {str(e)}"
            logger.error(error_msg, exc_info=True) 

            if websocket_manager and job_id:
                await websocket_manager.send_status_update(
                    job_id=job_id,
                    status="error",
                    message=error_msg,
                    result={
                        "step": "Engagement Finder",
                        "analyst_type": self.analyst_type,
                        "error": str(e)
                    }
                )
            
            messages = state.get('messages', [])
            messages.append(AIMessage(content=f"\n⚠️ {error_msg}"))
            state['messages'] = messages
            state['engagement_finder_data'] = state.get('engagement_finder_data', {}) # Ensure key exists
            raise

    async def run(self, state: ResearchState) -> ResearchState:
        """
        Entry point for the LangGraph node execution.
        Calls the analyze method and returns the updated state.
        """
        try:
            await self.analyze(state)
        except Exception as e:
             logger.error(f"EngagementFinderNode run failed: {e}")
             # Ensure key exists even on failure
             if 'engagement_finder_data' not in state:
                state['engagement_finder_data'] = {}
        return state # Always return the state