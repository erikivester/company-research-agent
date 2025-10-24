# backend/nodes/curator.py
import logging
import asyncio
from typing import Dict
from urllib.parse import urljoin, urlparse

from langchain_core.messages import AIMessage

from ..classes import ResearchState
from ..utils.references import process_references_from_search_results
from backend.airtable_uploader import update_airtable_record

logger = logging.getLogger(__name__)

class Curator:
    def __init__(self) -> None:
        self.relevance_threshold = 0.4
        logger.info(f"Curator initialized with relevance threshold: {self.relevance_threshold}")

    async def evaluate_documents(self, state: ResearchState, docs: list, context: Dict[str, str]) -> list:
        """Evaluate documents based on Tavily's scoring."""
        websocket_manager = state.get('websocket_manager')
        job_id = state.get('job_id')

        if websocket_manager and job_id:
            logger.info(f"Sending initial curation evaluation status update for job {job_id}")
            await websocket_manager.send_status_update(
                job_id=job_id,
                status="processing",
                message="Evaluating documents for curation",
                result={"step": "Curation", "substep": "evaluation"}
            )

        if not docs:
            return []

        logger.info(f"Evaluating {len(docs)} documents")

        evaluated_docs = []
        try:
            for doc in docs:
                try:
                    # Use evaluation score if present (added by Curator previously?), else fallback to raw score
                    if 'evaluation' in doc and 'overall_score' in doc['evaluation']:
                         tavily_score = float(doc['evaluation']['overall_score'])
                    else:
                         tavily_score = float(doc.get('score', 0)) # Fallback to raw score

                    is_company_website = doc.get('source') == 'company_website'

                    # ReFED Preference: Prioritize first-party (company website) sources
                    # Keep if score meets threshold OR if it's from the company website
                    if tavily_score >= self.relevance_threshold or is_company_website:
                        reason = "company website" if is_company_website else f"score {tavily_score:.4f}"
                        logger.info(f"Document kept ({reason}) for '{doc.get('title', 'No title')}' (URL: {doc.get('url', 'Unknown URL')})") # Added URL for context

                        # Ensure 'evaluation' key exists and store the score used for keeping the doc
                        if 'evaluation' not in doc:
                             doc['evaluation'] = {}
                        doc['evaluation']['overall_score'] = tavily_score # Store the score used
                        doc['evaluation']['query'] = doc.get('query', '') # Ensure query is stored

                        evaluated_docs.append(doc) # Add the original doc dict (now potentially with evaluation key)

                        # Send incremental update for kept document via WebSocket
                        if websocket_manager and job_id:
                            await websocket_manager.send_status_update(
                                job_id=job_id,
                                status="document_kept",
                                message=f"Kept document: {doc.get('title', 'No title')}",
                                result={
                                    "step": "Curation",
                                    "doc_type": doc.get('doc_type', 'unknown'), # Pass doc_type added earlier
                                    "title": doc.get('title', 'No title'),
                                    "score": tavily_score,
                                    "url": doc.get('url', 'Unknown URL') # Include URL
                                }
                            )
                    else:
                         logger.debug(f"Document below threshold (Score: {tavily_score:.4f}) for '{doc.get('title', 'No title')}' (URL: {doc.get('url', 'Unknown URL')})") # Use debug level
                except (ValueError, TypeError) as e:
                    logger.warning(f"Error processing score for document '{doc.get('url', 'Unknown URL')}': {e}")
                    continue

        except Exception as e:
            logger.error(f"Error during document evaluation: {e}", exc_info=True)
            return []

        # Sort by the evaluation score we stored
        evaluated_docs.sort(key=lambda x: float(x.get('evaluation', {}).get('overall_score', 0)), reverse=True)
        logger.info(f"Returning {len(evaluated_docs)} evaluated documents")

        return evaluated_docs

    async def curate_data(self, state: ResearchState) -> ResearchState:
        """Curate all collected data based on Tavily scores."""
        company = state.get('company', 'Unknown Company')
        airtable_record_id = state.get('airtable_record_id')
        logger.info(f"Starting curation for company: {company}")

        if airtable_record_id:
            asyncio.create_task(
                self._update_airtable_status(airtable_record_id, "Curating Documents")
            )

        websocket_manager = state.get('websocket_manager')
        job_id = state.get('job_id')

        # Define all data types, including the new FLW category
        data_types = {
            'financial_data': ('💰 Financial', 'financial'),
            'news_data': ('📰 News', 'news'),
            'industry_data': ('🏭 Industry', 'industry'),
            'company_data': ('🏢 Company', 'company'),
            'flw_data': ('🌿 FLW/Sustainability', 'flw') # <-- ADDED FLW entry
        }
        # Initialize doc_counts for all defined types
        doc_counts_init = { info[1]: {"initial": 0, "kept": 0} for _, info in data_types.items() }


        if websocket_manager and job_id:
            logger.info(f"Sending initial curation status update for job {job_id}")
            await websocket_manager.send_status_update(
                job_id=job_id,
                status="processing",
                message=f"Starting document curation for {company}",
                result={
                    "step": "Curation",
                    "doc_counts": doc_counts_init # Send initialized counts
                }
            )

        industry = state.get('industry', 'Unknown')
        context = {
            "company": company,
            "industry": industry,
            "hq_location": state.get('hq_location', 'Unknown')
        }

        msg = [f"🔍 Curating research data for {company}"]
        curation_tasks = []
        # Use a fresh dictionary to track counts accurately during this run
        doc_counts_run = { info[1]: {"initial": 0, "kept": 0} for _, info in data_types.items() }

        for data_field, (emoji, doc_type) in data_types.items():
            data = state.get(data_field, {})
            if not data or not isinstance(data, dict): # Check data exists and is a dict
                logger.info(f"No initial documents found or invalid format for {data_field}")
                state[f'curated_{data_field}'] = {} # Ensure curated key exists
                continue

            # --- URL Normalization and Deduplication ---
            unique_docs = {}
            for url, doc in data.items():
                if not isinstance(doc, dict): # Skip if doc is not a dictionary
                     logger.warning(f"Skipping non-dictionary item under URL '{url}' in {data_field}")
                     continue
                try:
                    parsed = urlparse(url)
                    current_url = url
                    if not parsed.scheme:
                        current_url = 'https://' + url
                        parsed = urlparse(current_url)

                    if not parsed.netloc:
                         logger.warning(f"Skipping invalid URL (no domain): {url} in {data_field}")
                         continue

                    # Normalize URL: remove query, fragment, trailing slash, lower scheme/netloc
                    clean_url = parsed._replace(query='', fragment='',
                                                scheme=parsed.scheme.lower(),
                                                netloc=parsed.netloc.lower()
                                                ).geturl().rstrip('/')

                    if clean_url not in unique_docs:
                        doc['url'] = clean_url # Store cleaned URL in the doc itself
                        doc['doc_type'] = doc_type # Assign the type (financial, news, flw, etc.)
                        unique_docs[clean_url] = doc
                    # Optional: Could add logic here to keep the doc with the higher score if URL collision occurs
                except Exception as parse_exc:
                    logger.warning(f"Error parsing or cleaning URL '{url}' in {data_field}: {parse_exc}")
                    continue
            # --- End URL Normalization ---

            docs = list(unique_docs.values())
            initial_count = len(docs)
            doc_counts_run[doc_type]["initial"] = initial_count # Update count for this run
            if initial_count > 0:
                 logger.info(f"Found {initial_count} unique documents for {data_field} ({doc_type})")
                 curation_tasks.append((data_field, emoji, doc_type, list(unique_docs.keys()), docs))
            else:
                 logger.info(f"No valid, unique documents found for {data_field} ({doc_type}) after cleaning.")
                 state[f'curated_{data_field}'] = {} # Ensure curated key exists

        # --- Process each category ---
        for data_field, emoji, doc_type, urls, docs in curation_tasks:
            msg.append(f"\n{emoji}: Processing {len(docs)} unique {doc_type} documents")

            if websocket_manager and job_id:
                await websocket_manager.send_status_update(
                    job_id=job_id,
                    status="category_start",
                    message=f"Processing {doc_type} documents",
                    result={ "step": "Curation", "doc_type": doc_type, "initial_count": len(docs) }
                )

            # Evaluate documents for relevance
            evaluated_docs = await self.evaluate_documents(state, docs, context)

            if not evaluated_docs:
                msg.append("  ⚠️ No relevant documents kept")
                doc_counts_run[doc_type]["kept"] = 0
                logger.warning(f"No documents kept after evaluation for {doc_type}")
                state[f'curated_{data_field}'] = {} # Ensure curated key exists
                continue

            # --- Map evaluated docs back using URL ---
            evaluated_docs_dict = {doc['url']: doc for doc in evaluated_docs}
            relevant_docs = {url: evaluated_docs_dict[url]
                             for url in urls if url in evaluated_docs_dict}
            # --- END ---

            # Sort by score
            sorted_items = sorted(
                relevant_docs.items(),
                key=lambda item: float(item[1].get('evaluation', {}).get('overall_score', 0)),
                reverse=True
            )

            # Limit to top 30 per category
            if len(sorted_items) > 30:
                logger.info(f"Trimming {doc_type} documents from {len(sorted_items)} to 30.")
                sorted_items = sorted_items[:30]
            relevant_docs = dict(sorted_items)
            kept_count = len(relevant_docs)
            doc_counts_run[doc_type]["kept"] = kept_count # Update kept count for this run


            if relevant_docs:
                msg.append(f"  ✓ Kept {kept_count} relevant documents")
                logger.info(f"Kept {kept_count} documents for {doc_type}")
            else:
                # This case might be redundant due to the 'if not evaluated_docs' check above, but keep for safety
                msg.append("  ⚠️ No documents met relevance threshold after sorting/limiting")
                logger.warning(f"No documents met threshold for {doc_type} after sorting/limiting")

            # Save the curated data to the specific state key (e.g., 'curated_flw_data')
            state[f'curated_{data_field}'] = relevant_docs

        # --- Process References AFTER all categories are curated ---
        try:
            logger.info("Processing references from all curated data...")
            # Ensure process_references passes the state which now includes curated_flw_data
            top_reference_urls, reference_titles, reference_info = process_references_from_search_results(state)
            logger.info(f"Selected top {len(top_reference_urls)} references for the report")
            state['references'] = top_reference_urls
            state['reference_titles'] = reference_titles
            state['reference_info'] = reference_info
        except Exception as ref_exc:
             logger.error(f"Error processing references: {ref_exc}", exc_info=True)
             state['references'] = []
             state['reference_titles'] = {}
             state['reference_info'] = {}
        # --- End Reference Processing ---

        # Update final message list in state
        messages = state.get('messages', [])
        messages.append(AIMessage(content="\n".join(msg)))
        state['messages'] = messages

        # Send final curation stats via WebSocket using the counts from this run
        if websocket_manager and job_id:
             await websocket_manager.send_status_update(
                 job_id=job_id,
                 status="curation_complete",
                 message="Document curation complete",
                 result={
                     "step": "Curation",
                     "doc_counts": doc_counts_run # Send the final counts
                 }
             )
        logger.info(f"Curation complete for {company}. Final counts: {doc_counts_run}")
        return state

    async def _update_airtable_status(self, record_id: str, status_text: str):
        """Helper to call the synchronous update function."""
        try:
            update_airtable_record(record_id, {'Research Status': status_text})
        except Exception as e:
            logger.error(f"Curator node failed to update Airtable status: {e}")

    async def run(self, state: ResearchState) -> ResearchState:
        airtable_record_id = state.get('airtable_record_id') # Get ID early for except block
        try:
            return await self.curate_data(state)
        except Exception as e:
            logger.error(f"Error in Curator run method: {e}", exc_info=True)
            error_msg = f"⚠️ Curator node failed critically: {str(e)}"
            state.setdefault('messages', []).append(AIMessage(content=error_msg))
            if airtable_record_id:
                asyncio.create_task(
                    self._update_airtable_status(airtable_record_id, f"Curation Failed: {str(e)[:50]}")
                )
            # Ensure essential keys exist even on failure
            for data_field in data_types: # Use data_types keys defined in curate_data
                 state.setdefault(f'curated_{data_field}', {})
            state.setdefault('references', [])
            state.setdefault('reference_titles', {})
            state.setdefault('reference_info', {})
            return state