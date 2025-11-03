# backend/nodes/grounding.py
import logging
import os
import asyncio
from langchain_core.messages import AIMessage
from tavily import AsyncTavilyClient

from ..classes import InputState, ResearchState
from backend.airtable_uploader import update_airtable_record # synchronous function

logger = logging.getLogger(__name__)

class GroundingNode:
    """Gathers initial grounding data about the company."""
    
    def __init__(self) -> None:
        self.tavily_client = AsyncTavilyClient(api_key=os.getenv("TAVILY_API_KEY"))

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
            logger.error(f"{self.__class__.__name__} failed to update Airtable status for record {record_id}: {e}", exc_info=True)
    # --- END MODIFIED HELPER METHOD ---

    async def initial_search(self, state: InputState) -> ResearchState:
        """
        FIXED: This function now modifies the 'state' object in-place
        instead of creating a new 'research_state' dictionary.
        This preserves all incoming fields like job_id, websocket_manager,
        and google_drive_folder_url.
        """
        # Add debug logging at the start to check websocket manager
        if websocket_manager := state.get('websocket_manager'):
            logger.info("Websocket manager found in state")
        else:
            logger.warning("No websocket manager found in state")
        
        company = state.get('company') or 'Unknown Company' # Use 'or' to catch empty strings
        msg = f"🎯 Initiating research for {company}...\n"
        
        if websocket_manager := state.get('websocket_manager'):
            await websocket_manager.safe_send(
                state=state,
                job_id=state.get('job_id'),
                status="processing",
                message=f"🎯 Initiating research for {company}",
                result={"step": "Initializing"}
            )

        site_scrape = {}
        error_str = None # --- FIX: Initialize error_str ---

        # Only attempt extraction if we have a URL
        if url := state.get('company_url'):
            msg += f"\n🌐 Crawling company website: {url}"
            logger.info(f"Starting website analysis for {url}")
            
            # Send initial briefing status
            if websocket_manager := state.get('websocket_manager'):
                await websocket_manager.safe_send(
                    state=state,
                    job_id=state.get('job_id'),
                    status="processing",
                    message="Crawling company website",
                    result={"step": "Initial Site Scrape"}
                )

            try:
                logger.info("Initiating Tavily crawl")
                site_extraction = await self.tavily_client.crawl(
                    url=url, 
                    instructions="Find any pages that will help us understand the company's business, products, services, and any other relevant information.",
                    max_depth=1, 
                    max_breadth=50, 
                    extract_depth="advanced"
                )
                
                site_scrape = {}
                for item in site_extraction.get("results", []):
                    if item.get("raw_content"):
                        page_url = item.get("url", url)
                        site_scrape[page_url] = {
                            'raw_content': item.get('raw_content'),
                            'source': 'company_website'
                        }
                
                if site_scrape:
                    logger.info(f"Successfully crawled {len(site_scrape)} pages from website")
                    msg += f"\n✅ Successfully crawled {len(site_scrape)} pages from website"
                    if websocket_manager := state.get('websocket_manager'):
                        await websocket_manager.safe_send(
                            state=state,
                            job_id=state.get('job_id'),
                            status="processing",
                            message=f"Successfully crawled {len(site_scrape)} pages from website",
                            result={"step": "Initial Site Scrape"}
                        )
                else:
                    logger.warning("No content found in crawl results")
                    msg += "\n⚠️ No content found in website crawl"
                    if websocket_manager := state.get('websocket_manager'):
                        await websocket_manager.safe_send(
                            state=state,
                            job_id=state.get('job_id'),
                            status="processing",
                            message="⚠️ No content found in provided URL",
                            result={"step": "Initial Site Scrape"}
                        )
            except Exception as e:
                error_str = str(e) # --- FIX: Capture error ---
                logger.error(f"Website crawl error: {error_str}", exc_info=True)
                error_msg = f"⚠️ Error crawling website content: {error_str}"
                print(error_msg)
                msg += f"\n{error_msg}"
                if websocket_manager := state.get('websocket_manager'):
                    await websocket_manager.safe_send(
                        state=state,
                        job_id=state.get('job_id'),
                        status="website_error",
                        message=error_msg,
                        result={
                            "step": "Initial Site Scrape", 
                            "error": error_str,
                            "continue_research": True  # Continue with research even if website extraction fails
                        }
                    )
        else:
            msg += "\n⏩ No company URL provided, proceeding directly to research phase"
            if websocket_manager := state.get('websocket_manager'):
                await websocket_manager.safe_send(
                    state=state,
                    job_id=state.get('job_id'),
                    status="processing",
                    message="No company URL provided, proceeding directly to research phase",
                    result={"step": "Initializing"}
                )
        # Add context about what information we have
        context_data = {}
        if hq := state.get('hq_location'):
            msg += f"\n📍 Company HQ: {hq}"
            context_data["hq_location"] = hq
        if industry := state.get('industry'):
            msg += f"\n🏭 Industry: {industry}"
            context_data["industry"] = industry
        
        
        # --- FIX: Modify state in-place instead of replacing it ---
        # Ensure the canonical company value is stored on the state so
        # downstream nodes and LangGraph merge reducers have a concrete
        # non-empty value to work with.
        state['company'] = company

        state['messages'] = [AIMessage(content=msg)]
        state['site_scrape'] = site_scrape
        if error_str:
            state['error'] = error_str

        # Return the MODIFIED state, not a new object
        return state

    async def run(self, state: InputState) -> ResearchState:
        airtable_record_id = state.get('airtable_record_id')
        if airtable_record_id:
            # AWAIT the critical initial status update
            await self._update_airtable_status(airtable_record_id, "In Progress")
        
        # Pass the original state object to be modified
        return await self.initial_search(state)