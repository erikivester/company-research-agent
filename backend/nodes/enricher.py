# backend/nodes/enricher.py
import asyncio
import logging
import random
from typing import Any, Dict, List
from urllib.parse import urlparse

from langchain_core.messages import AIMessage

from backend.airtable_uploader import update_airtable_record

from ..classes import ResearchState
from ..config import config
from ..utils.status_constants import ResearchStatus
from ..utils.rate_limiter import tavily_limiter  # NEW: Global rate limiter

logger = logging.getLogger(__name__)


class Enricher:
    """Enriches curated documents with raw content."""

    def __init__(self) -> None:
        # Use config to get the appropriate Tavily client (mock or real)
        self.tavily_client = config.get_tavily_client()

        # --- IMPROVED: Batch processing configuration ---
        self.batch_size = 5  # Process URLs in smaller batches
        self.batch_delay = 0.5  # REDUCED from 3s - minimal delay to prevent rate limiting
        self.semaphore_limit = 5  # Reduced concurrent requests per batch
        # --- END IMPROVED ---

        # --- OPTIMIZED: Reduced timeout budgets for faster failure ---
        self.timeout_budgets = {
            "company": 15,  # Reduced from 20s - fail faster, rely on snippet
            "news": 10,     # Reduced from 15s - news sites are fast or fail
            "contact": 10,  # Reduced from 12s - contact pages are simple
            "flw": 20,      # Reduced from 25s - priority but fail faster
            "engagement": 12,  # Reduced from 15s
        }
        self.default_timeout = 12  # Reduced from 15s - aggressive fail-fast
        self.pdf_timeout = 40  # Higher timeout for priority PDFs (extraction is slower)
        # --- END OPTIMIZED ---

        # --- OPTIMIZED: Reduced retries for faster failure ---
        self.max_retries = 2  # Reduced from 3 - fail faster, rely on snippet fallback
        self.base_retry_delay = 1  # Reduced from 2s - shorter waits between retries
        self.max_retry_delay = 15  # Reduced from 30s - fail fast and move on
        # --- END OPTIMIZED ---

        # --- MEMORY OPTIMIZATION: Content size limit ---
        self.max_content_length = 50000  # 50KB limit per document to prevent memory bloat
        # --- END MEMORY OPTIMIZATION ---

        # --- Define PDF keywords that indicate high-value documents worth extracting ---
        self.PRIORITY_PDF_KEYWORDS = {
            "sustainability",
            "esg",
            "impact",
            "responsibility",
            "environmental",
            "social",
            "governance",
            "annual-report",
            "corporate-responsibility",
            "climate",
            "cdp",
        }
        # --- END PRIORITY PDF KEYWORDS ---

        # --- Define domains that are known to fail extraction ---
        self.BLOCKLIST_DOMAINS = {
            "linkedin.com",
            "www.linkedin.com",
            "indeed.com",
            "www.indeed.com",
            "comparably.com",
            "www.comparably.com",
            "panmore.com",
            "www.panmore.com",
            "zoominfo.com",
            "www.zoominfo.com",
            "facebook.com",
            "www.facebook.com",
            # Additional commonly failing domains from logs
            "stockanalysis.com",
            "rocketreach.co",
            "stockrow.com",
            # --- NEW: Domains with anti-scraping/JS protection (from production logs) ---
            "newsweek.com",
            "www.newsweek.com",
            "supplychaindigital.com",
            "www.supplychaindigital.com",
            "tracxn.com",
            "www.tracxn.com",
            "refrigeratedfrozenfood.com",
            "www.refrigeratedfrozenfood.com",
            "tricitiesbusinessnews.com",
            "www.tricitiesbusinessnews.com",
            # Government sites that block scrapers
            "dol.gov",
            "www.dol.gov",
            # SEC filings (often fail with "Unknown error")
            "sec.gov",
            "www.sec.gov",
            # --- TIMEOUT OPTIMIZATIONS: Slow/problematic sites from Dec 17 logs ---
            # News sites with anti-scraping (from Dec 18 logs)
            "modernretail.co",
            "www.modernretail.co",
            "foodservicedirector.com",
            "www.foodservicedirector.com",
            "njbiz.com",
            "www.njbiz.com",
            "grocerybusiness-digitalmagazine.com",
            "www.grocerybusiness-digitalmagazine.com",
            "designrush.com",
            "news.designrush.com",
            "monitorriau.com",
            "www.monitorriau.com",
            "malaymail.com",
            "www.malaymail.com",
            # Financial/analysis sites that timeout
            "trendspider.com",
            "www.trendspider.com",
            "annualreports.com",
            "www.annualreports.com",
            "towardspackaging.com",
            "www.towardspackaging.com",
            # Forbes articles (consistently return "Unknown error")
            "forbes.com",
            "www.forbes.com",
            "ibisworld.com",           # Slow data aggregator
            "www.ibisworld.com",
            "zippia.com",              # Slow revenue databases
            "www.zippia.com",
            "sfchronicle.com",         # Paywall + slow
            "www.sfchronicle.com",
            "martini.ai",              # Research platform (slow API)
            "www.simplyhired.com",     # Job sites (anti-bot)
            "simplyhired.com",
            "markets.ft.com",          # Financial Times - paywall
            "agalert.com",             # Ag news - often times out
            "www.agalert.com",
            "morningagclips.com",      # Ag news - slow
            "www.morningagclips.com",
            "renewablethermal.org",    # Slow nonprofit sites
            "www.renewablethermal.org",
            "cdic.net",                # Dairy industry sites (slow)
            "www.cdic.net",
            "dairypcc.net",            # Dairy processor council (timeouts)
            "www.dairypcc.net",
            # --- END TIMEOUT OPTIMIZATIONS ---
        }
        # --- END BLOCKLIST ---

    def is_priority_pdf(self, url: str) -> bool:
        """Check if a PDF URL contains keywords indicating high-value content (ESG, impact reports, etc.)"""
        if not url.lower().endswith('.pdf'):
            return False

        url_lower = url.lower()
        return any(keyword in url_lower for keyword in self.PRIORITY_PDF_KEYWORDS)

    async def fetch_single_content(
        self,
        url: str,
        websocket_manager=None,
        job_id=None,
        category=None,
        retries: int = None,
        retry_delay: int = None,
    ) -> Dict[str, Any]:
        """Fetch raw content for a single URL using the extract method with exponential backoff retries."""
        last_error = None

        # Use instance defaults if not provided
        if retries is None:
            retries = self.max_retries
        if retry_delay is None:
            retry_delay = self.base_retry_delay

        # Get timeout for this category (with special handling for priority PDFs)
        is_priority_pdf = self.is_priority_pdf(url)
        if is_priority_pdf:
            timeout = self.pdf_timeout  # Use extended timeout for priority PDFs
            logger.info(f"Using extended timeout ({timeout}s) for priority PDF: {url}")
        else:
            timeout = self.timeout_budgets.get(category, self.default_timeout)

        for attempt in range(retries):
            try:
                if websocket_manager and job_id:
                    status_message = f"Extracting content from {url}"
                    if attempt > 0:
                        status_message += f" (Attempt {attempt + 1}/{retries})"
                    await websocket_manager.send_status_update(
                        job_id=job_id,
                        status="extracting",
                        message=status_message,
                        result={
                            "step": "Enriching",
                            "url": url,
                            "category": category,
                            "attempt": attempt + 1,
                        },
                    )

                # Use Tavily's extract method with explicit timeout
                # NEW: Apply global rate limiting before API call
                await tavily_limiter.acquire()

                # Wrap in asyncio.wait_for to enforce timeout budget
                response = await asyncio.wait_for(
                    self.tavily_client.extract(url), timeout=timeout
                )

                # Parse response
                if response and isinstance(response, dict) and response.get("results"):
                    result_content = response["results"][0].get("raw_content", "")
                    if result_content and result_content.strip():
                        original_length = len(result_content)

                        # Truncate content if it exceeds max length
                        if original_length > self.max_content_length:
                            result_content = result_content[:self.max_content_length]
                            logger.info(
                                f"Truncated content from {url}: {original_length} -> {self.max_content_length} chars"
                            )

                        logger.debug(
                            f"Successfully extracted content from {url} (Length: {len(result_content)}) on attempt {attempt + 1}"
                        )
                        if websocket_manager and job_id:
                            await websocket_manager.send_status_update(
                                job_id=job_id,
                                status="extracted",
                                message=f"Successfully extracted content from {url}",
                                result={
                                    "step": "Enriching",
                                    "url": url,
                                    "category": category,
                                    "success": True,
                                },
                            )
                        return {
                            url: result_content
                        }  # Return URL mapped to content string
                    else:
                        logger.warning(
                            f"Empty raw_content found in extract results for {url} on attempt {attempt + 1}."
                        )
                        last_error = "Empty content returned by extract"
                        # No retry on empty content, as it's a successful but empty response
                        break

                else:
                    logger.warning(
                        f"Unexpected response structure or empty results from extract for {url} on attempt {attempt + 1}. Response: {response}"
                    )
                    last_error = "Invalid response from extract API"
                    # Potentially retryable, continue loop

            except asyncio.TimeoutError:
                last_error = f"Request timed out after {timeout}s"
                logger.warning(
                    f"Timeout on attempt {attempt + 1} for {url} (category: {category}, budget: {timeout}s)"
                )
                if attempt < retries - 1:
                    # Exponential backoff with jitter
                    delay = min(
                        retry_delay * (2**attempt) + random.uniform(0, 1),
                        self.max_retry_delay,
                    )
                    logger.info(f"Retrying {url} in {delay:.2f} seconds...")
                    await asyncio.sleep(delay)
                continue

            except Exception as e:
                last_error = str(e)
                logger.error(
                    f"Error on attempt {attempt + 1} for {url}: {last_error}",
                    exc_info=True,
                )
                if attempt < retries - 1:
                    # Exponential backoff with jitter
                    delay = min(
                        retry_delay * (2**attempt) + random.uniform(0, 1),
                        self.max_retry_delay,
                    )
                    logger.info(f"Retrying {url} in {delay:.2f} seconds...")
                    await asyncio.sleep(delay)
                continue  # Go to next attempt

            # If we got a bad response but didn't raise an exception, wait before retrying
            if attempt < retries - 1:
                # Exponential backoff with jitter for bad responses too
                delay = min(
                    retry_delay * (2**attempt) + random.uniform(0, 1),
                    self.max_retry_delay,
                )
                logger.info(
                    f"Retrying {url} due to bad response in {delay:.2f} seconds..."
                )
                await asyncio.sleep(delay)

        # If all retries fail, send final error status
        error_msg = last_error or "Unknown extraction error"
        logger.error(
            f"Failed to extract content for {url} after {retries} attempts. Last error: {error_msg}"
        )
        if websocket_manager and job_id:
            await websocket_manager.send_status_update(
                job_id=job_id,
                status="extraction_error",
                message=f"Failed to extract content from {url}: {error_msg}",
                result={
                    "step": "Enriching",
                    "url": url,
                    "category": category,
                    "success": False,
                    "error": error_msg,
                },
            )
        return {url: None, "error": error_msg}

    async def fetch_raw_content(
        self, urls: List[str], websocket_manager=None, job_id=None, category=None
    ) -> Dict[str, Any]:
        """Fetch raw content for multiple URLs in batches with rate limiting and delays."""
        if not urls:
            return {}

        total_urls = len(urls)
        logger.info(
            f"Fetching content for {total_urls} URLs (category: {category}) in batches of {self.batch_size} with {self.semaphore_limit} concurrent requests per batch."
        )

        raw_contents = {}

        # Process URLs in batches
        for batch_num, batch_start in enumerate(
            range(0, total_urls, self.batch_size), start=1
        ):
            batch_end = min(batch_start + self.batch_size, total_urls)
            batch_urls = urls[batch_start:batch_end]
            batch_count = len(batch_urls)

            logger.info(
                f"Processing batch {batch_num} ({batch_count} URLs) for category {category}"
            )

            if websocket_manager and job_id:
                await websocket_manager.send_status_update(
                    job_id=job_id,
                    status="batch_processing",
                    message=f"Processing batch {batch_num} ({batch_count} URLs) for {category}",
                    result={
                        "step": "Enriching",
                        "category": category,
                        "batch": batch_num,
                        "batch_size": batch_count,
                    },
                )

            # Create semaphore for this batch
            semaphore = asyncio.Semaphore(self.semaphore_limit)

            async def fetch_with_semaphore(url: str):
                async with semaphore:
                    return await self.fetch_single_content(
                        url, websocket_manager, job_id, category
                    )

            # Process batch concurrently
            tasks = [fetch_with_semaphore(url) for url in batch_urls]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            # Collect results from this batch
            for result in batch_results:
                if isinstance(result, Exception):
                    logger.error(
                        f"Unhandled exception during content fetching: {result}"
                    )
                elif isinstance(result, dict):
                    raw_contents.update(result)
                else:
                    logger.warning(
                        f"Unexpected result type in content fetching: {type(result)}"
                    )

            # Add delay between batches (except after the last batch)
            if batch_end < total_urls:
                logger.info(
                    f"Batch {batch_num} complete. Waiting {self.batch_delay}s before next batch..."
                )
                await asyncio.sleep(self.batch_delay)
            else:
                logger.info(f"Batch {batch_num} complete (final batch).")

        # --- Count Success/Failure ---
        successful_fetches = sum(
            1
            for url, content in raw_contents.items()
            if content and not isinstance(content, dict)
        )
        failed_fetches = total_urls - successful_fetches

        logger.info(
            f"Finished fetching content for {category}: {successful_fetches} successful, {failed_fetches} failed out of {total_urls} URLs."
        )
        # --- End Count ---

        return raw_contents

    async def enrich_data(self, state: ResearchState) -> ResearchState:
        """(v2) Enrich curated documents with raw content."""
        company = state.get("company", "Unknown Company")
        airtable_record_id = state.get("airtable_record_id")
        websocket_manager = state.get("websocket_manager")
        job_id = state.get("job_id")

        if airtable_record_id:
            await self._update_airtable_status(
                airtable_record_id, ResearchStatus.ENRICHING_CONTENT
            )

        if websocket_manager and job_id:
            await websocket_manager.send_status_update(
                job_id=job_id,
                status="processing",
                message=f"Starting content enrichment for {company}",
                result={"step": "Enriching", "substep": "initialization"},
            )

        msg = [f"📚 Enriching curated data for {company}:"]

        # --- v2 MODIFICATION: Updated data_types dictionary ---
        data_types = {
            "company_brief_data": ("🏢 Company Brief", "company"),
            "news_signal_data": ("📰 News & Signals", "news"),
            "flw_data": ("🌿 FLW/Sustainability", "flw"),
            "contact_finder_data": ("👥 Contacts", "contact"),
            "engagement_finder_data": ("🛰️ Engagements", "engagement"),
        }
        # --- END v2 MODIFICATION ---

        enrichment_tasks = []

        # This loop now iterates over the 5 v2 data_types
        for data_field, (label, category) in data_types.items():
            curated_field = f"curated_{data_field}"  # e.g., 'curated_flw_data'
            curated_docs = state.get(curated_field, {})

            if not curated_docs or not isinstance(
                curated_docs, dict
            ):  # Check data exists and is a dict
                msg.append(f"\n• No curated {label} documents to enrich")
                continue

            # --- MODIFIED LOGIC: Pre-filter blocklisted domains ---
            docs_needing_content = {}
            docs_with_content = 0
            docs_blocklisted = 0

            for url, doc in curated_docs.items():
                if not isinstance(doc, dict):
                    continue

                # Check if content already exists
                if doc.get("raw_content") and str(doc.get("raw_content")).strip():
                    docs_with_content += 1
                    continue  # Already enriched

                # Check blocklist and PDF files
                try:
                    domain = urlparse(url).netloc
                    is_pdf = url.lower().endswith('.pdf')
                    is_priority = self.is_priority_pdf(url) if is_pdf else False

                    if domain in self.BLOCKLIST_DOMAINS:
                        # Fallback to snippet and skip API call
                        doc["raw_content"] = doc.get("content", "")
                        doc["enrichment_note"] = "Skipped (blocklist domain)"
                        docs_blocklisted += 1
                    elif is_pdf and not is_priority:
                        # Low-priority PDFs: skip extraction, use snippet
                        doc["raw_content"] = doc.get("content", "")
                        doc["enrichment_note"] = "PDF - using search snippet (not priority)"
                        docs_blocklisted += 1
                        logger.info(f"Skipping low-priority PDF extraction (using snippet): {url}")
                    elif is_pdf and is_priority:
                        # High-priority PDFs (ESG, sustainability reports): attempt extraction
                        docs_needing_content[url] = doc
                        logger.info(f"Attempting extraction for priority PDF: {url}")
                    else:
                        # Add to fetch queue (non-PDF URLs)
                        docs_needing_content[url] = doc
                except Exception as e:
                    logger.warning(f"Error parsing URL {url} for blocklist check: {e}")
                    docs_needing_content[url] = doc  # Add to fetch queue on error
            # --- END MODIFIED LOGIC ---

            if docs_blocklisted > 0:
                msg.append(
                    f"\n• {label}: {docs_blocklisted} docs skipped (blocklist fallback)."
                )
            if docs_with_content > 0:
                msg.append(
                    f"\n• {label}: {docs_with_content} docs already have content."
                )

            if not docs_needing_content:
                if docs_with_content == 0 and docs_blocklisted == 0:
                    # This case handles when there are no docs at all
                    msg.append(f"\n• No {label} documents found to enrich")
                continue

            num_to_enrich = len(docs_needing_content)
            msg.append(
                f"\n• Enriching {num_to_enrich} / {len(curated_docs)} {label} documents..."
            )

            if websocket_manager and job_id:
                await websocket_manager.send_status_update(
                    job_id=job_id,
                    status="category_start",
                    message=f"Starting enrichment for {label} ({num_to_enrich} docs)",
                    result={
                        "step": "Enriching",
                        "category": category,
                        "count": num_to_enrich,
                    },
                )

            # Add task details for this category
            enrichment_tasks.append(
                {
                    "field": curated_field,  # e.g., 'curated_flw_data'
                    "category": category,  # e.g., 'flw'
                    "label": label,  # e.g., '🌿 FLW/Sustainability'
                    "docs_to_enrich": docs_needing_content,
                    "all_curated_docs": curated_docs,  # Pass the full dict for updating
                }
            )

        # Process enrichments in parallel if there are tasks
        if enrichment_tasks:

            async def process_category(task):
                enriched_count = 0
                error_count = 0
                urls_to_fetch = list(task["docs_to_enrich"].keys())
                try:
                    # Fetch content only for the docs needing it
                    raw_contents_results = await self.fetch_raw_content(
                        urls_to_fetch, websocket_manager, job_id, task["category"]
                    )

                    # Update the main curated_docs dictionary for this category
                    for url in urls_to_fetch:
                        if url in task["all_curated_docs"]:
                            fetch_result = raw_contents_results.get(url)

                            # Check if fetch failed (result is None or has "error" key)
                            if fetch_result is None or (
                                isinstance(fetch_result, dict)
                                and "error" in fetch_result
                            ):
                                # --- NEW: Fallback to existing search snippet on extraction failure ---
                                existing_snippet = task["all_curated_docs"][url].get("content", "")
                                if existing_snippet and existing_snippet.strip():
                                    # Use the search snippet as fallback content
                                    task["all_curated_docs"][url]["raw_content"] = existing_snippet
                                    task["all_curated_docs"][url]["enrichment_note"] = "Using search snippet (extraction failed)"
                                    enriched_count += 1  # Count as successful (we have usable content)
                                    logger.info(
                                        f"Extraction failed for {url}, using existing search snippet ({len(existing_snippet)} chars)"
                                    )
                                else:
                                    # No fallback available, record the error
                                    error_count += 1
                                    error_msg = (
                                        fetch_result.get("error", "Content fetch failed")
                                        if isinstance(fetch_result, dict)
                                        else "Content fetch failed"
                                    )
                                    # Add error info to the specific document in the main dict
                                    task["all_curated_docs"][url][
                                        "enrichment_error"
                                    ] = error_msg
                                    logger.warning(
                                        f"Failed to enrich {url} for {task['category']}: {error_msg} (no search snippet available)"
                                    )
                                # --- END NEW FALLBACK LOGIC ---
                            # Check if fetch succeeded (result is a non-empty string)
                            elif isinstance(fetch_result, str) and fetch_result.strip():
                                task["all_curated_docs"][url][
                                    "raw_content"
                                ] = fetch_result
                                enriched_count += 1
                            else:  # Handle empty string or unexpected type
                                # --- NEW: Fallback to search snippet on empty extraction ---
                                existing_snippet = task["all_curated_docs"][url].get("content", "")
                                if existing_snippet and existing_snippet.strip():
                                    # Use the search snippet as fallback content
                                    task["all_curated_docs"][url]["raw_content"] = existing_snippet
                                    task["all_curated_docs"][url]["enrichment_note"] = "Using search snippet (empty extraction)"
                                    enriched_count += 1  # Count as successful
                                    logger.info(
                                        f"Empty extraction for {url}, using existing search snippet ({len(existing_snippet)} chars)"
                                    )
                                else:
                                    # No fallback available
                                    error_count += 1
                                    error_msg = "Content missing or empty after fetch"
                                    task["all_curated_docs"][url][
                                        "enrichment_error"
                                    ] = error_msg
                                    logger.warning(
                                        f"Content issue for {url} in {task['category']} post-fetch. Result: {fetch_result} (no search snippet available)"
                                    )
                                # --- END NEW FALLBACK LOGIC ---
                        else:
                            logger.warning(
                                f"URL {url} from fetch task not found in current curated docs for {task['category']}."
                            )

                    # Update the state directly with the modified dictionary for this category
                    state[task["field"]] = task["all_curated_docs"]

                    logger.info(
                        f"Finished enrichment for {task['label']}: {enriched_count} successful, {error_count} failed out of {len(urls_to_fetch)} attempts."
                    )
                    if websocket_manager and job_id:
                        await websocket_manager.send_status_update(
                            job_id=job_id,
                            status="category_complete",
                            message=f"Completed enrichment for {task['label']} ({enriched_count}/{len(urls_to_fetch)} successful)",
                            result={
                                "step": "Enriching",
                                "category": task["category"],
                                "enriched": enriched_count,
                                "errors": error_count,
                                "total": len(urls_to_fetch),
                            },
                        )
                    return {
                        "category": task["category"],
                        "enriched": enriched_count,
                        "total": len(urls_to_fetch),
                        "errors": error_count,
                    }
                except Exception as e:
                    logger.error(
                        f"Critical error processing enrichment category {task['category']}: {e}",
                        exc_info=True,
                    )
                    num_docs = len(urls_to_fetch)
                    return {
                        "category": task["category"],
                        "enriched": 0,
                        "total": num_docs,
                        "errors": num_docs,
                    }  # Report all as errors

            # Run all category enrichments concurrently
            results = await asyncio.gather(
                *[process_category(task) for task in enrichment_tasks]
            )

            # Calculate and log totals
            total_enriched = sum(r.get("enriched", 0) for r in results)
            total_attempted = sum(r.get("total", 0) for r in results)
            total_errors = sum(r.get("errors", 0) for r in results)

            status_message = f"Content enrichment complete. Successfully enriched {total_enriched}/{total_attempted} documents"
            if total_errors > 0:
                status_message += f". Failed attempts: {total_errors}."
            logger.info(status_message)

            if websocket_manager and job_id:
                await websocket_manager.send_status_update(
                    job_id=job_id,
                    status="enrichment_complete",
                    message=status_message,
                    result={
                        "step": "Enriching",
                        "total_enriched": total_enriched,
                        "total_attempted": total_attempted,
                        "total_errors": total_errors,
                    },
                )

        # Update final message list in state
        messages = state.get("messages", [])
        messages.append(AIMessage(content="\n".join(msg)))
        state["messages"] = messages
        return state

    # --- MODIFIED HELPER METHOD to use asyncio.to_thread ---
    async def _update_airtable_status(self, record_id: str, status_text: str):
        """Helper to call the synchronous update function in a separate thread."""
        if not record_id:
            logger.warning("Airtable status update skipped: No record ID provided.")
            return
        try:
            # Use asyncio.to_thread to safely run the synchronous Airtable API call
            await asyncio.to_thread(
                update_airtable_record, record_id, {"Research Status": status_text}
            )
            logger.debug(f"Airtable status update successful for record {record_id}")
        except Exception as e:
            # Log the error but do not raise, as Airtable update is a secondary task
            logger.error(
                f"{self.__class__.__name__} failed to update Airtable status: {e}",
                exc_info=True,
            )

    # --- END MODIFIED HELPER METHOD ---

    async def run(self, state: ResearchState) -> ResearchState:
        airtable_record_id = state.get(
            "airtable_record_id"
        )  # Get ID early for except block
        try:
            return await self.enrich_data(state)
        except Exception as e:
            error_msg = f"Error in enrichment process: {e}"
            logger.error(error_msg, exc_info=True)
            state.setdefault("messages", []).append(
                AIMessage(content=f"⚠️ Enrichment node failed: {error_msg}")
            )
            if airtable_record_id:
                await self._update_airtable_status(
                    airtable_record_id,
                    ResearchStatus.format_error(
                        ResearchStatus.FAILED_ENRICHMENT, str(e)
                    ),
                )

            # --- v2 MODIFICATION: Ensure all new v2 keys exist on failure ---
            v2_curated_keys = [
                "curated_company_brief_data",
                "curated_news_signal_data",
                "curated_flw_data",
                "curated_contact_finder_data",
                "curated_engagement_finder_data",
            ]
            for key in v2_curated_keys:
                state.setdefault(key, {})
            # --- END v2 MODIFICATION ---
            return state
