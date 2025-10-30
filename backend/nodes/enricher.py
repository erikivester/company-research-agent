# backend/nodes/enricher.py
import asyncio
import os
import logging
from typing import Dict, List, Any

from langchain_core.messages import AIMessage
from tavily import AsyncTavilyClient

from ..classes import ResearchState
from backend.airtable_uploader import update_airtable_record

logger = logging.getLogger(__name__)

class Enricher:
    """Enriches curated documents with raw content."""

    def __init__(self) -> None:
        tavily_key = os.getenv("TAVILY_API_KEY")
        if not tavily_key:
            raise ValueError("TAVILY_API_KEY environment variable is not set")
        self.tavily_client = AsyncTavilyClient(api_key=tavily_key)
        self.batch_size = 20 # Number of URLs to fetch in parallel per batch
        self.semaphore_limit = 10 # Max concurrent requests to Tavily API

    async def fetch_single_content(self, url: str, websocket_manager=None, job_id=None, category=None) -> Dict[str, Any]:
        """Fetch raw content for a single URL using the extract method."""
        try:
            if websocket_manager and job_id:
                await websocket_manager.send_status_update(
                    job_id=job_id, status="extracting",
                    message=f"Extracting content from {url}",
                    result={ "step": "Enriching", "url": url, "category": category }
                )

            # Use Tavily's extract method
            response = await self.tavily_client.extract(url)

            # Parse response
            if response and isinstance(response, dict) and response.get('results'):
                 result_content = response['results'][0].get('raw_content', '')
                 if result_content and result_content.strip():
                     logger.debug(f"Successfully extracted content from {url} (Length: {len(result_content)})")
                     if websocket_manager and job_id:
                         await websocket_manager.send_status_update(
                             job_id=job_id, status="extracted",
                             message=f"Successfully extracted content from {url}",
                             result={ "step": "Enriching", "url": url, "category": category, "success": True }
                         )
                     return {url: result_content} # Return URL mapped to content string
                 else:
                      logger.warning(f"Empty raw_content found in extract results for {url}.")
                      error_msg = "Empty content returned by extract"
                      if websocket_manager and job_id:
                          await websocket_manager.send_status_update(
                              job_id=job_id, status="extraction_error",
                              message=f"Failed to extract content from {url}: {error_msg}",
                              result={"step": "Enriching", "url": url, "category": category, "success": False, "error": error_msg}
                          )
                      # Return None for content, but keep URL as key and include error
                      return {url: None, "error": error_msg}

            else:
                 logger.warning(f"Unexpected response structure or empty results from extract for {url}. Response: {response}")
                 error_msg = "Invalid response from extract API"
                 if websocket_manager and job_id:
                     await websocket_manager.send_status_update(
                         job_id=job_id, status="extraction_error",
                         message=f"Failed to extract content from {url}: {error_msg}",
                         result={"step": "Enriching", "url": url, "category": category, "success": False, "error": error_msg}
                     )
                 return {url: None, "error": error_msg}

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error calling Tavily extract for {url}: {error_msg}", exc_info=True)
            if websocket_manager and job_id:
                await websocket_manager.send_status_update(
                    job_id=job_id, status="extraction_error",
                    message=f"Failed to extract content from {url}: API Error",
                    result={ "step": "Enriching", "url": url, "category": category, "success": False, "error": error_msg }
                )
            return {url: None, "error": error_msg}

    async def fetch_raw_content(self, urls: List[str], websocket_manager=None, job_id=None, category=None) -> Dict[str, Any]:
        """Fetch raw content for multiple URLs in parallel with rate limiting."""
        raw_contents = {}
        if not urls:
             return raw_contents

        total_batches = (len(urls) + self.batch_size - 1) // self.batch_size
        logger.info(f"Fetching content for {len(urls)} URLs in {total_batches} batches (category: {category}).")

        batches = [urls[i:i + self.batch_size] for i in range(0, len(urls), self.batch_size)]
        semaphore = asyncio.Semaphore(self.semaphore_limit)

        async def process_batch(batch_num: int, batch_urls: List[str]) -> Dict[str, Any]:
            async with semaphore:
                logger.debug(f"Processing batch {batch_num + 1}/{total_batches} with {len(batch_urls)} URLs.")
                if websocket_manager and job_id:
                    await websocket_manager.send_status_update(
                        job_id=job_id, status="batch_start",
                        message=f"Processing batch {batch_num + 1}/{total_batches} for {category}",
                        result={ "step": "Enriching", "batch": batch_num + 1, "total_batches": total_batches, "category": category }
                    )

                tasks = [self.fetch_single_content(url, websocket_manager, job_id, category) for url in batch_urls]
                # Gather results, catching exceptions within the batch
                results = await asyncio.gather(*tasks, return_exceptions=True)

                batch_contents = {}
                for result in results:
                     if isinstance(result, Exception):
                          # Log the exception, but don't add it to batch_contents directly
                          # The error should ideally be handled within fetch_single_content
                          logger.error(f"Unhandled exception during batch processing task: {result}")
                     elif isinstance(result, dict):
                          batch_contents.update(result) # Add {url: content} or {url: None, "error": ...}
                     else:
                          logger.warning(f"Unexpected result type in batch processing: {type(result)}")

                logger.debug(f"Batch {batch_num + 1} finished.")
                return batch_contents

        # Gather results from all batches, catching exceptions during batch processing
        all_batch_results = await asyncio.gather(*[
            process_batch(i, batch) for i, batch in enumerate(batches)
        ], return_exceptions=True)

        for batch_result in all_batch_results:
            if isinstance(batch_result, Exception):
                 logger.error(f"Error processing a batch: {batch_result}")
            elif isinstance(batch_result, dict):
                 raw_contents.update(batch_result) # Merge results from the batch
            else:
                 logger.warning(f"Unexpected result type when combining batches: {type(batch_result)}")

        # --- Count Success/Failure ---
        successful_fetches = 0
        failed_fetches = 0
        processed_urls = set()
        for url_key, result_value in raw_contents.items():
            if url_key == "error": continue # Skip the generic error key if present
            processed_urls.add(url_key)
            # Check if the value is a non-empty string for success
            if isinstance(result_value, str) and result_value.strip():
                 successful_fetches += 1
            else:
                 # Treat None, dict with 'error', empty string, or unexpected types as failures
                 failed_fetches += 1
                 if not (result_value is None or (isinstance(result_value, dict) and 'error' in result_value)):
                      logger.debug(f"URL {url_key} treated as failed fetch. Result: {result_value}")


        # URLs that might have failed due to exceptions in gather *before* fetch_single_content handled them
        failures_due_to_exceptions = len(urls) - len(processed_urls)
        total_failed = failed_fetches + failures_due_to_exceptions
        logger.info(f"Finished fetching content for {category}: {successful_fetches} successful, {total_failed} failed out of {len(urls)} URLs.")
        # --- End Count ---

        return raw_contents

    async def enrich_data(self, state: ResearchState) -> ResearchState:
        """(v2) Enrich curated documents with raw content."""
        company = state.get('company', 'Unknown Company')
        airtable_record_id = state.get('airtable_record_id')
        websocket_manager = state.get('websocket_manager')
        job_id = state.get('job_id')

        if airtable_record_id:
            asyncio.create_task(
                self._update_airtable_status(airtable_record_id, "Enriching Content")
            )

        if websocket_manager and job_id:
            await websocket_manager.send_status_update(
                job_id=job_id, status="processing",
                message=f"Starting content enrichment for {company}",
                result={ "step": "Enriching", "substep": "initialization" }
            )

        msg = [f"📚 Enriching curated data for {company}:"]

        # --- v2 MODIFICATION: Updated data_types dictionary ---
        # Maps the v2 researcher node outputs (state keys) to labels
        data_types = {
            'company_brief_data': ('🏢 Company Brief', 'company'),
            'news_signal_data': ('📰 News & Signals', 'news'),
            'flw_data': ('🌿 FLW/Sustainability', 'flw'),
            'contact_finder_data': ('👥 Contacts', 'contact'),
            'engagement_finder_data': ('🛰️ Engagements', 'engagement')
        }
        # --- END v2 MODIFICATION ---
        
        enrichment_tasks = []

        # This loop now iterates over the 5 v2 data_types
        for data_field, (label, category) in data_types.items():
            curated_field = f'curated_{data_field}' # e.g., 'curated_flw_data'
            curated_docs = state.get(curated_field, {})

            if not curated_docs or not isinstance(curated_docs, dict): # Check data exists and is a dict
                msg.append(f"\n• No curated {label} documents to enrich")
                continue

            # Find documents that need raw_content
            docs_needing_content = {
                url: doc for url, doc in curated_docs.items()
                if isinstance(doc, dict) and (not doc.get('raw_content') or not str(doc.get('raw_content')).strip())
            }

            if not docs_needing_content:
                msg.append(f"\n• All {label} documents ({len(curated_docs)}) already have raw content")
                continue

            num_to_enrich = len(docs_needing_content)
            msg.append(f"\n• Enriching {num_to_enrich} / {len(curated_docs)} {label} documents...")

            if websocket_manager and job_id:
                await websocket_manager.send_status_update(
                    job_id=job_id, status="category_start",
                    message=f"Starting enrichment for {label} ({num_to_enrich} docs)",
                    result={ "step": "Enriching", "category": category, "count": num_to_enrich }
                )

            # Add task details for this category
            enrichment_tasks.append({
                'field': curated_field, # e.g., 'curated_flw_data'
                'category': category, # e.g., 'flw'
                'label': label, # e.g., '🌿 FLW/Sustainability'
                'docs_to_enrich': docs_needing_content,
                'all_curated_docs': curated_docs # Pass the full dict for updating
            })

        # Process enrichments in parallel if there are tasks
        if enrichment_tasks:
            async def process_category(task):
                enriched_count = 0; error_count = 0
                urls_to_fetch = list(task['docs_to_enrich'].keys())
                try:
                    # Fetch content only for the docs needing it
                    raw_contents_results = await self.fetch_raw_content(
                        urls_to_fetch, websocket_manager, job_id, task['category']
                    )

                    # Update the main curated_docs dictionary for this category
                    for url in urls_to_fetch:
                        if url in task['all_curated_docs']:
                            fetch_result = raw_contents_results.get(url)

                            # Check if fetch failed (result is None or has "error" key)
                            if fetch_result is None or (isinstance(fetch_result, dict) and 'error' in fetch_result):
                                error_count += 1
                                error_msg = fetch_result.get('error', 'Content fetch failed') if isinstance(fetch_result, dict) else 'Content fetch failed'
                                # Add error info to the specific document in the main dict
                                task['all_curated_docs'][url]['enrichment_error'] = error_msg
                                logger.warning(f"Failed to enrich {url} for {task['category']}: {error_msg}")
                            # Check if fetch succeeded (result is a non-empty string)
                            elif isinstance(fetch_result, str) and fetch_result.strip():
                                task['all_curated_docs'][url]['raw_content'] = fetch_result
                                enriched_count += 1
                            else: # Handle empty string or unexpected type
                                error_count += 1
                                error_msg = "Content missing or empty after fetch"
                                task['all_curated_docs'][url]['enrichment_error'] = error_msg
                                logger.warning(f"Content issue for {url} in {task['category']} post-fetch. Result: {fetch_result}")
                        else:
                             logger.warning(f"URL {url} from fetch task not found in current curated docs for {task['category']}.")

                    # Update the state directly with the modified dictionary for this category
                    state[task['field']] = task['all_curated_docs']

                    logger.info(f"Finished enrichment for {task['label']}: {enriched_count} successful, {error_count} failed out of {len(urls_to_fetch)} attempts.")
                    if websocket_manager and job_id:
                        await websocket_manager.send_status_update(
                            job_id=job_id, status="category_complete",
                            message=f"Completed enrichment for {task['label']} ({enriched_count}/{len(urls_to_fetch)} successful)",
                            result={ "step": "Enriching", "category": task['category'], "enriched": enriched_count, "errors": error_count, "total": len(urls_to_fetch)}
                        )
                    return {'category': task['category'], 'enriched': enriched_count, 'total': len(urls_to_fetch), 'errors': error_count}
                except Exception as e:
                    logger.error(f"Critical error processing enrichment category {task['category']}: {e}", exc_info=True)
                    num_docs = len(urls_to_fetch)
                    return {'category': task['category'], 'enriched': 0, 'total': num_docs, 'errors': num_docs} # Report all as errors

            # Run all category enrichments concurrently
            results = await asyncio.gather(*[process_category(task) for task in enrichment_tasks])

            # Calculate and log totals
            total_enriched = sum(r.get('enriched', 0) for r in results)
            total_attempted = sum(r.get('total', 0) for r in results)
            total_errors = sum(r.get('errors', 0) for r in results)

            status_message = f"Content enrichment complete. Successfully enriched {total_enriched}/{total_attempted} documents"
            if total_errors > 0:
                status_message += f". Failed attempts: {total_errors}."
            logger.info(status_message)

            if websocket_manager and job_id:
                await websocket_manager.send_status_update(
                    job_id=job_id, status="enrichment_complete",
                    message=status_message,
                    result={ "step": "Enriching", "total_enriched": total_enriched, "total_attempted": total_attempted, "total_errors": total_errors }
                )

        # Update final message list in state
        messages = state.get('messages', [])
        messages.append(AIMessage(content="\n".join(msg)))
        state['messages'] = messages
        return state

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
            logger.error(f"{self.__class__.__name__} failed to update Airtable status: {e}", exc_info=True)
    # --- END MODIFIED HELPER METHOD ---

    async def run(self, state: ResearchState) -> ResearchState:
        airtable_record_id = state.get('airtable_record_id') # Get ID early for except block
        try:
            return await self.enrich_data(state)
        except Exception as e:
            error_msg = f"Error in enrichment process: {e}"
            logger.error(error_msg, exc_info=True)
            state.setdefault('messages', []).append(AIMessage(content=f"⚠️ Enrichment node failed: {error_msg}"))
            if airtable_record_id:
                 asyncio.create_task(
                     self._update_airtable_status(airtable_record_id, "Enrichment Failed")
                 )
            
            # --- v2 MODIFICATION: Ensure all new v2 keys exist on failure ---
            v2_curated_keys = [
                'curated_company_brief_data', 'curated_news_signal_data', 'curated_flw_data',
                'curated_contact_finder_data', 'curated_engagement_finder_data'
            ]
            for key in v2_curated_keys:
                state.setdefault(key, {})
            # --- END v2 MODIFICATION ---
            return state