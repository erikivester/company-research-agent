# backend/nodes/collector.py
from langchain_core.messages import AIMessage
import asyncio
import logging
from urllib.parse import urlparse # Ensure urlparse is imported

from ..classes import ResearchState
from backend.airtable_uploader import update_airtable_record # synchronous function
from ..utils.status_constants import ResearchStatus


logger = logging.getLogger(__name__)


class Collector:
    """Collects and organizes all research data before curation."""

    # --- MODIFIED HELPER METHOD to use asyncio.to_thread ---
    async def _update_airtable_status(self, state: ResearchState, status_text: str):
        """Helper to call the synchronous update function in a separate thread."""
        record_id = state.get('airtable_record_id')
        if not record_id:
            logger.warning("Airtable status update skipped: No record ID provided.")
            return
        try:
            await asyncio.to_thread(update_airtable_record, record_id, {'Research Status': status_text})
            logger.debug(f"Airtable status update successful for record {record_id}")
        except Exception as e:
            error_message = f"⚠️ Airtable status update failed: {e}"
            logger.error(f"{self.__class__.__name__} failed to update Airtable status for record {record_id}: {e}", exc_info=True)
            state.setdefault('messages', []).append(AIMessage(content=error_message))
    # --- END MODIFIED HELPER METHOD ---

    async def collect(self, state: ResearchState) -> ResearchState:
        """Collect and verify all research data is present."""
        company = state.get('company', 'Unknown Company')
        msg = [f"📦 Collecting research data for {company}:"]
        websocket_manager = state.get('websocket_manager')
        job_id = state.get('job_id')

        if websocket_manager and job_id:
            await websocket_manager.send_status_update(
                job_id=job_id,
                status="processing",
                message=f"Collecting research data for {company}",
                result={"step": "Collecting"}
            )

        # --- v2 MODIFICATION: Updated research_types dictionary ---
        # This now maps to the 5 new/refocused researcher nodes and their state keys
        research_types = {
            'company_brief_data': '🏢 Company Brief',
            'news_signal_data': '📰 News & Signals',
            'flw_data': '🌿 FLW/Sustainability', # This key remains the same
            'contact_finder_data': '👥 Contacts',
            'engagement_finder_data': '🛰️ Engagements'
        }
        # --- END v2 MODIFICATION ---


        # --- LOGIC: Collect all scored documents to infer missing company_url ---
        # --- FIX: We get the company_url from the state *first*. ---
        best_url = state.get('company_url')
        all_scored_docs = []

        # This loop now iterates over the v2 research_types
        for data_field, label in research_types.items():
            data = state.get(data_field, {})
            if data and isinstance(data, dict): # Check if data exists and is a dictionary
                
                # Check for existing data URL and add to all_scored_docs
                for url, doc in data.items():
                    # We look for the raw Tavily search score (which is present in all research documents)
                    score = doc.get('score', 0.0)
                    if url and score > 0.0:
                        all_scored_docs.append({'url': url, 'score': score})
                        
                msg.append(f"• {label}: {len(data)} documents collected")
            else:
                msg.append(f"• {label}: No data found")
                # Ensure the key exists in the state, even if empty, for downstream nodes
                if data_field not in state:
                    state[data_field] = {}

        # --- CRITICAL FIX: ---
        # Only run inference logic if the company URL was not provided in the input state.
        if not best_url and all_scored_docs:
            # --- NEW: Prioritize URLs that are likely homepages ---
            # Shorter URL paths are preferred. '/' is length 1.
            all_scored_docs.sort(key=lambda x: (x['score'], -len(urlparse(x['url']).path)), reverse=True)
            
            # Use the URL of the highest scored and most homepage-like document
            inferred_url = all_scored_docs[0]['url']
            
            # Clean URL to base domain (scheme://netloc)
            if inferred_url and inferred_url.startswith('http'):
                 parsed = urlparse(inferred_url)
                 
                 # Reconstruct URL as just scheme://netloc (homepage)
                 clean_base_url = f"{parsed.scheme}://{parsed.netloc.rstrip('/')}"

                 state['company_url'] = clean_base_url
                 logger.info(f"Inferred company_url set to clean base URL: {clean_base_url} (from top score {all_scored_docs[0]['score']})")
                 msg.append(f"🔗 **Inferred Company URL** set to: {clean_base_url}")
            else:
                 logger.warning(f"Top scored URL '{inferred_url}' was invalid, skipping URL inference.")
        elif best_url:
            logger.info(f"Using provided company_url: {best_url}")
        else:
            logger.warning("No company_url provided and no documents found to infer from.")
        # --- End FIX ---


        # Update state with collection message
        messages = state.get('messages', [])
        messages.append(AIMessage(content="\n".join(msg)))
        state['messages'] = messages

        return state

    async def run(self, state: ResearchState) -> ResearchState:
        if state.get('airtable_record_id'):
            await self._update_airtable_status(state, ResearchStatus.COLLECTING_DATA)
        return await self.collect(state)