# backend/nodes/collector.py
from langchain_core.messages import AIMessage
import asyncio
import logging

from ..classes import ResearchState
from backend.airtable_uploader import update_airtable_record


logger = logging.getLogger(__name__)


class Collector:
    """Collects and organizes all research data before curation."""

    # --- MODIFIED HELPER METHOD to use asyncio.to_thread ---
    async def _update_airtable_status(self, record_id: str, status_text: str):
        """Helper to call the synchronous update function in a separate thread."""
        if not record_id:
            logger.warning("Airtable status update skipped: No record ID provided.")
            return
        try:
            # Use asyncio.to_thread to safely run the synchronous Airtable API call
            await asyncio.to_thread(update_airtable_record, record_id, {'Research Status': status_text})
            logger.debug(f"Airtable status update successful for record {record_id}")
        except Exception as e:
            # Log the error but do not raise, as Airtable update is a secondary task
            logger.error(f"Collector node failed to update Airtable status for record {record_id}: {e}", exc_info=True)
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

        # Check each type of research data, including the new FLW data
        research_types = {
            'financial_data': '💰 Financial',
            'news_data': '📰 News',
            'industry_data': '🏭 Industry',
            'company_data': '🏢 Company',
            'flw_data': '🌿 FLW/Sustainability' # <-- ADDED: Entry for the new data type
        }

        # --- NEW LOGIC: Collect all scored documents to infer missing company_url ---
        best_url = state.get('company_url')
        all_scored_docs = []

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

        if not best_url and all_scored_docs:
            # Sort by score descending to find the single most relevant document
            all_scored_docs.sort(key=lambda x: x['score'], reverse=True)
            
            # Use the URL of the highest scored document
            inferred_url = all_scored_docs[0]['url']
            
            # Simple check for http/https to ensure it's a valid URL format
            if inferred_url and inferred_url.startswith('http'):
                 state['company_url'] = inferred_url
                 logger.info(f"Inferred company_url set to: {inferred_url} (from top score {all_scored_docs[0]['score']})")
                 msg.append(f"🔗 **Inferred Company URL** set to: {inferred_url}")
            else:
                 logger.warning(f"Top scored URL '{inferred_url}' was invalid, skipping URL inference.")
        
        # --- End NEW LOGIC ---


        # Update state with collection message
        messages = state.get('messages', [])
        messages.append(AIMessage(content="\n".join(msg)))
        state['messages'] = messages

        return state

    async def run(self, state: ResearchState) -> ResearchState:
        airtable_record_id = state.get('airtable_record_id')
        if airtable_record_id:
            # This status update is run as a non-blocking background task
            asyncio.create_task(
                self._update_airtable_status(airtable_record_id, "Collecting Data")
            )
        return await self.collect(state)