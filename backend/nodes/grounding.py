# backend/nodes/grounding.py
import logging
import os
import asyncio
from langchain_core.messages import AIMessage
from tavily import AsyncTavilyClient

from ..classes import InputState, ResearchState
from backend.airtable_uploader import update_airtable_record

logger = logging.getLogger(__name__)

class GroundingNode:
    """Gathers initial grounding data about the company."""
    
    def __init__(self) -> None:
        self.tavily_client = AsyncTavilyClient(api_key=os.getenv("TAVILY_API_KEY"))

    # --- NEW HELPER METHOD ---
    async def _update_airtable_status(self, record_id: str, status_text: str):
        """Helper to call the synchronous update function."""
        try:
            update_airtable_record(record_id, {'Research Status': status_text})
            logger.debug(f"Airtable status update successful for record {record_id}")
        except Exception as e:
            logger.error(f"GroundingNode failed to update Airtable status for record {record_id}: {e}", exc_info=True)
    # --- END HELPER METHOD ---

    async def initial_search(self, state: InputState) -> ResearchState:
        # Add debug logging at the start to check websocket manager
        if websocket_manager := state.get('websocket_manager'):
            logger.info("Websocket manager found in state")
        else:
            logger.warning("No websocket manager found in state")
        
        company = state.get('company', 'Unknown Company')
        msg = f"🎯 Initiating research for {company}...\n"
        
        if websocket_manager := state.get('websocket_manager'):
            if job_id := state.get('job_id'):
                await websocket_manager.send_status_update(
                    job_id=job_id,
                    status="processing",
                    message=f"🎯 Initiating research for {company}",
                    result={"step": "Initializing"}
                )

        site_scrape = {}

        # Only attempt extraction if we have a URL
        if url := state.get('company_url'):
            msg += f"\n🌐 Crawling company website: {url}"
            logger.info(f"Starting website analysis for {url}")
            
            # Send initial briefing status
            if websocket_manager := state.get('websocket_manager'):
                if job_id := state.get('job_id'):
                    await websocket_manager.send_status_update(
                        job_id=job_id,
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
                        if job_id := state.get('job_id'):
                            await websocket_manager.send_status_update(
                                job_id=job_id,
                                status="processing",
                                message=f"Successfully crawled {len(site_scrape)} pages from website",
                                result={"step": "Initial Site Scrape"}
                            )
                else:
                    logger.warning("No content found in crawl results")
                    msg += "\n⚠️ No content found in website crawl"
                    if websocket_manager := state.get('websocket_manager'):
                        if job_id := state.get('job_id'):
                            await websocket_manager.send_status_update(
                                job_id=job_id,
                                status="processing",
                                message="⚠️ No content found in provided URL",
                                result={"step": "Initial Site Scrape"}
                            )
            except Exception as e:
                error_str = str(e)
                logger.error(f"Website crawl error: {error_str}", exc_info=True)
                error_msg = f"⚠️ Error crawling website content: {error_str}"
                print(error_msg)
                msg += f"\n{error_msg}"
                if websocket_manager := state.get('websocket_manager'):
                    if job_id := state.get('job_id'):
                        await websocket_manager.send_status_update(
                            job_id=job_id,
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
                if job_id := state.get('job_id'):
                    await websocket_manager.send_status_update(
                        job_id=job_id,
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
        
        # Initialize ResearchState with input information
        research_state = {
            # Copy input fields
            "company": state.get('company'),
            "company_url": state.get('company_url'),
            "hq_location": state.get('hq_location'),
            "industry": state.get('industry'),
            # Initialize research fields
            "messages": [AIMessage(content=msg)],
            "site_scrape": site_scrape,
            # Pass through websocket info
            "websocket_manager": state.get('websocket_manager'),
            "job_id": state.get('job_id'),
            "airtable_record_id": state.get('airtable_record_id') # Ensure ID is passed through
        }

        # If there was an error in the initial crawl, store it in the state
        if "⚠️ Error crawling website content:" in msg:
            research_state["error"] = error_str

        return research_state

    async def run(self, state: InputState) -> ResearchState:
        airtable_record_id = state.get('airtable_record_id')
        if airtable_record_id:
            asyncio.create_task(
                self._update_airtable_status(airtable_record_id, "In Progress")
            )
        return await self.initial_search(state)