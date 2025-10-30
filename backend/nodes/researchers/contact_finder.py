# backend/nodes/researchers/contact_finder.py
import logging
from typing import Any, Dict

from langchain_core.messages import AIMessage

# Use relative imports assuming standard project structure
from ...classes import ResearchState
from .base import BaseResearcher

logger = logging.getLogger(__name__)

class ContactFinderNode(BaseResearcher):
    """
    (v2) A new researcher node dedicated to finding relevant mid-level contacts 
    at a company, focusing on roles in sustainability, impact, and outreach.
    """
    def __init__(self) -> None:
        super().__init__()
        # Set a specific analyst type for this researcher
        self.analyst_type = "contact_finder"
        logger.info("Contact Finder Node initialized.")

    async def analyze(self, state: ResearchState) -> Dict[str, Any]:
        """
        Analyzes the company's public information to find relevant contacts.
        """
        company = state.get('company', 'Unknown Company')
        industry = state.get('industry', 'Unknown Industry') # Get industry for context
        websocket_manager = state.get('websocket_manager')
        job_id = state.get('job_id')

        # Initial message for logging and state update
        msg = [f"👥 Contact Finder Node searching for contacts at {company}"]
        logger.info(f"Starting contact finding for {company}")

        try:
            # v2: Generate search queries specific to finding people
            queries = await self.generate_queries(
                state,
                f"""
                Generate specific search queries to find a wide breadth of relevant mid-level contacts at "{company}". 
                Focus on roles related to sustainability, corporate social responsibility (CSR), environmental impact, food waste, and community outreach.

                Examples of queries to generate:
                - '"{company}" Head of Sustainability'
                - '"{company}" VP of Impact'
                - '"{company}" corporate giving manager'
                - '"{company}" food waste reduction team'
                - '"{company}" community relations contacts'
                - '"{company}" ESG team'
                - '"{company}" key contacts for {industry} partnerships'
                - '"{company}" executives on LinkedIn'
                """
            )

            # Add generated queries to state messages for transparency
            subqueries_msg = "🔍 Subqueries for contact finding:\n" + "\n".join([f"• {query}" for query in queries])
            messages = state.get('messages', [])
            messages.append(AIMessage(content=subqueries_msg))
            state['messages'] = messages

            # Send WebSocket update: Queries generated
            if websocket_manager and job_id:
                await websocket_manager.send_status_update(
                    job_id=job_id,
                    status="processing",
                    message="Contact finder queries generated",
                    result={
                        "step": "Contact Finder",
                        "analyst_type": self.analyst_type,
                        "queries": queries
                    }
                )

            # Initialize dictionary to store research results
            contact_finder_data = {}

            # Include relevant data from the initial website scrape if available
            if site_scrape := state.get('site_scrape'):
                msg.append(f"\n📊 Including {len(site_scrape)} pages from company website...")
                contact_finder_data.update(site_scrape)
                logger.info(f"Included {len(site_scrape)} site scrape results.")

            # Execute searches for the generated queries
            logger.info(f"Searching documents for {len(queries)} contact queries.")
            documents_found = await self.search_documents(state, queries)

            if documents_found:
                # Add found documents, associating each with its query
                for url, doc in documents_found.items():
                    doc['query'] = doc.get('query', 'Unknown Query')
                    contact_finder_data[url] = doc
                msg.append(f"\n✓ Found {len(documents_found)} documents from web search.")
                logger.info(f"Found {len(documents_found)} documents from web search.")
            else:
                 msg.append("\nℹ️ No additional documents found from web search for contacts.")
                 logger.info("No additional documents found from web search.")

            # Send WebSocket update: Search complete
            if websocket_manager and job_id:
                 await websocket_manager.send_status_update(
                     job_id=job_id,
                     status="processing",
                     message=f"Found {len(documents_found)} documents for contacts",
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
            state['contact_finder_data'] = contact_finder_data
            logger.info(f"Completed contact finding. Total documents collected: {len(contact_finder_data)}")

            return {
                'message': "\n".join(msg),
                'contact_finder_data': contact_finder_data
            }

        except Exception as e:
            error_msg = f"Contact finding failed: {str(e)}"
            logger.error(error_msg, exc_info=True) 

            if websocket_manager and job_id:
                await websocket_manager.send_status_update(
                    job_id=job_id,
                    status="error",
                    message=error_msg,
                    result={
                        "step": "Contact Finder",
                        "analyst_type": self.analyst_type,
                        "error": str(e)
                    }
                )
            
            messages = state.get('messages', [])
            messages.append(AIMessage(content=f"\n⚠️ {error_msg}"))
            state['messages'] = messages
            state['contact_finder_data'] = state.get('contact_finder_data', {}) # Ensure key exists
            raise

    async def run(self, state: ResearchState) -> ResearchState:
        """
        Entry point for the LangGraph node execution.
        Calls the analyze method and returns the updated state.
        """
        try:
            await self.analyze(state)
        except Exception as e:
             logger.error(f"ContactFinderNode run failed: {e}")
             # Ensure key exists even on failure
             if 'contact_finder_data' not in state:
                state['contact_finder_data'] = {}
        return state # Always return the state