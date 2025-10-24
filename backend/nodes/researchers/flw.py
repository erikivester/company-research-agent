# backend/nodes/researchers/flw.py
import logging
from typing import Any, Dict

from langchain_core.messages import AIMessage

# Use relative imports assuming standard project structure
from ...classes import ResearchState
from .base import BaseResearcher

logger = logging.getLogger(__name__)

class FLWAnalyzer(BaseResearcher):
    """
    Researcher focused on Food Loss & Waste (FLW), sustainability,
    and related environmental initiatives for a company.
    """
    def __init__(self) -> None:
        super().__init__()
        # Set a specific analyst type for this researcher
        self.analyst_type = "flw_analyzer"
        logger.info("FLW/Sustainability Analyzer initialized.")

    async def analyze(self, state: ResearchState) -> Dict[str, Any]:
        """
        Analyzes the company's FLW and sustainability efforts.
        """
        company = state.get('company', 'Unknown Company')
        industry = state.get('industry', 'Unknown Industry') # Get industry for context
        websocket_manager = state.get('websocket_manager')
        job_id = state.get('job_id')

        # Initial message for logging and state update
        msg = [f"🌿 FLW/Sustainability Analyzer investigating {company}"]
        logger.info(f"Starting FLW/Sustainability analysis for {company}")

        try:
            # Generate search queries specific to FLW and sustainability
            # Tailored for ReFED's interests (methane, packaging, donation, etc.)
            queries = await self.generate_queries(
                state,
                f"""
                Generate specific search queries to understand '{company}'s efforts related to food loss and waste (FLW) and sustainability. Focus on:
                - Specific food waste reduction initiatives (prevention, rescue, recycling).
                - Sustainability reports or environmental goals (especially regarding climate impact and methane emissions).
                - Packaging details (materials used, optimization efforts, circularity).
                - Food donation programs or partnerships with food rescue organizations.
                - Supply chain practices impacting FLW (e.g., forecasting, cold chain).
                - Statements or data about their environmental footprint in the {industry} industry.
                - Certifications or commitments related to sustainability (e.g., B Corp, UN SDGs).
                - Use of anaerobic digestion or composting for food scraps.
                """
            )

            # Add generated queries to state messages for transparency
            subqueries_msg = "🔍 Subqueries for FLW/Sustainability analysis:\n" + "\n".join([f"• {query}" for query in queries])
            messages = state.get('messages', [])
            messages.append(AIMessage(content=subqueries_msg))
            state['messages'] = messages

            # Send WebSocket update: Queries generated
            if websocket_manager and job_id:
                await websocket_manager.send_status_update(
                    job_id=job_id,
                    status="processing",
                    message="FLW/Sustainability analysis queries generated",
                    result={
                        "step": "FLW/Sustainability Analyst", # Use a descriptive step name
                        "analyst_type": self.analyst_type,
                        "queries": queries
                    }
                )

            # Initialize dictionary to store research results
            flw_data = {}

            # Include relevant data from the initial website scrape if available
            if site_scrape := state.get('site_scrape'):
                msg.append(f"\n📊 Including {len(site_scrape)} pages from company website...")
                # Potentially filter site_scrape for keywords later if needed, but start by including all
                flw_data.update(site_scrape)
                logger.info(f"Included {len(site_scrape)} site scrape results.")


            # Execute searches for the generated queries
            logger.info(f"Searching documents for {len(queries)} FLW/Sustainability queries.")
            documents_found = await self.search_documents(state, queries)

            if documents_found:
                # Add found documents, associating each with its query
                for url, doc in documents_found.items():
                    # Ensure the query is associated, default to 'Unknown Query' if missing
                    doc['query'] = doc.get('query', 'Unknown Query')
                    flw_data[url] = doc
                msg.append(f"\n✓ Found {len(documents_found)} documents from web search.")
                logger.info(f"Found {len(documents_found)} documents from web search.")
            else:
                 msg.append("\nℹ️ No additional documents found from web search for FLW/Sustainability.")
                 logger.info("No additional documents found from web search.")

            # Send WebSocket update: Search complete
            if websocket_manager and job_id:
                 await websocket_manager.send_status_update(
                     job_id=job_id,
                     status="processing",
                     message=f"Found {len(documents_found)} documents for FLW/Sustainability",
                     result={
                         "step": "Searching",
                         "analyst_type": self.analyst_type,
                         "queries": queries,
                         "documents_found": len(documents_found) # Report count found by this node
                     }
                 )

            # Update state with findings
            messages = state.get('messages', [])
            messages.append(AIMessage(content="\n".join(msg)))
            state['messages'] = messages
            # Use a specific key for this node's data
            state['flw_data'] = flw_data
            logger.info(f"Completed FLW/Sustainability analysis. Total documents collected: {len(flw_data)}")

            # Return results (primarily for potential direct use or testing, state update is main goal)
            return {
                'message': "\n".join(msg), # Return the compiled log message
                'flw_data': flw_data
            }

        except Exception as e:
            error_msg = f"FLW/Sustainability analysis failed: {str(e)}"
            logger.error(error_msg, exc_info=True) # Log the full error traceback

            # Send error status via WebSocket
            if websocket_manager and job_id:
                await websocket_manager.send_status_update(
                    job_id=job_id,
                    status="error",
                    message=error_msg,
                    result={
                        "step": "FLW/Sustainability Analyst",
                        "analyst_type": self.analyst_type,
                        "error": str(e)
                    }
                )
            # Add error message to state
            messages = state.get('messages', [])
            messages.append(AIMessage(content=f"\n⚠️ {error_msg}"))
            state['messages'] = messages
            state['flw_data'] = state.get('flw_data', {}) # Ensure key exists even on failure
            # Re-raise the exception to potentially halt the graph or be caught upstream
            raise

    async def run(self, state: ResearchState) -> ResearchState:
        """
        Entry point for the LangGraph node execution.
        Calls the analyze method and returns the updated state.
        """
        try:
            await self.analyze(state)
        except Exception as e:
            # Error is logged and sent via WS in analyze, just ensure state is returned
             logger.error(f"FLWAnalyzer run failed: {e}")
        return state # Always return the state, even if analysis failed