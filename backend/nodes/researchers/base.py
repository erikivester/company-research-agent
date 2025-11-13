import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict
from urllib.parse import urlparse

import markdown
from bs4 import BeautifulSoup

from ...classes import ResearchState
from ...config import config
from ...utils.mock_tavily import MockTavilyClient
from ...utils.references import clean_title

logger = logging.getLogger(__name__)


class BaseResearcher:
    def __init__(self):
        # Get appropriate client from config
        self.tavily_client = config.get_tavily_client()
        self.analyst_type = "base_researcher"  # Default type

    @property
    def analyst_type(self) -> str:
        if not hasattr(self, "_analyst_type"):
            raise ValueError("Analyst type not set by subclass")
        return self._analyst_type

    @analyst_type.setter
    def analyst_type(self, value: str):
        self._analyst_type = value

    async def search_documents(self, state: ResearchState) -> Dict[str, Any]:
        """
        Execute all Tavily searches in parallel for the queries assigned
        to this researcher's analyst_type. Improved to ensure queries always flow correctly.
        """
        websocket_manager = state.get("websocket_manager")
        job_id = state.get("job_id")

        # Defensive: Log all state keys for debugging if queries are missing
        queries = None
        try:
            research_queries = state.get("research_queries")
            logger.info(f"=== BaseResearcher Debug for {self.analyst_type} ===")
            state_keys = list(state.keys())
            logger.info(f"Full state keys: {state_keys}")
            logger.info(f"research_queries present: {research_queries is not None}")
            if research_queries is not None:
                rq_keys = list(research_queries.keys())
                logger.info(f"research_queries keys: {rq_keys}")
                logger.info(f"research_queries content: {research_queries}")

            queries = (
                research_queries.get(self.analyst_type, []) if research_queries else []
            )
            if not queries:
                logger.error(
                    f"No queries found for analyst_type '{self.analyst_type}' in research_queries. Keys: {list(research_queries.keys() if research_queries else [])}"
                )
                raise ValueError(
                    f"No queries found for analyst_type '{self.analyst_type}' in research_queries."
                )

            num_queries = len(queries)
            logger.info(f"Found {num_queries} queries for {self.analyst_type}: {queries}")
            logger.info("=== End BaseResearcher Debug ===")
        except Exception as e:
            logger.error(
                f"Error accessing queries from state for {self.analyst_type}: {e}. State keys: {list(state.keys())}"
            )
            raise

        # Send status update for generated queries
        if not queries:
            logger.warning(f"No queries provided for {self.analyst_type}")
            return {}

        if websocket_manager and job_id:
            await websocket_manager.send_status_update(
                job_id=job_id,
                status="queries_generated",
                message=f"Received {len(queries)} queries for {self.analyst_type}",
                result={
                    "step": "Searching",
                    "analyst": self.analyst_type,
                    "queries": queries,
                    "total_queries": len(queries),
                },
            )

        # First optionally load context documents from Google Drive to reduce API usage
        local_docs: Dict[str, Any] = {}
        use_local_context = state.get(
            "use_local_context", False
        )  # <-- Check Airtable flag
        google_drive_folder_url = state.get("google_drive_folder_url")

        # DEBUG: Log the values with clear markers
        logger.info(f"🔧 CONTEXT CHECK: use_local_context={use_local_context}, google_drive_folder_url={google_drive_folder_url}")

        # NEW: If use_local_context is enabled and GDrive folder URL is provided, download existing research
        if use_local_context and google_drive_folder_url:
            logger.info("🎯 LOCAL CONTEXT MODE ACTIVATED - Will check Google Drive for existing research")
            logger.info(f"📂 Google Drive Folder: {google_drive_folder_url}")
            try:
                from ...utils.gdrive_uploader import download_research_from_gdrive

                logger.info(f"🔍 STARTING Google Drive download for {self.analyst_type}...")
                downloaded_files = await download_research_from_gdrive(
                    google_drive_folder_url
                )
                logger.info(f"📦 Download complete. Retrieved {len(downloaded_files) if downloaded_files else 0} files")

                if downloaded_files:
                    logger.info(f"✅ SUCCESS: Found {len(downloaded_files)} research file(s) in Google Drive")
                    filenames = [f['filename'] for f in downloaded_files]
                    logger.info(f"📋 Files retrieved: {filenames}")

                    # Parse downloaded research files into document format
                    for file_data in downloaded_files:
                        try:
                            research_content = file_data["content"]
                            filename = file_data["filename"]

                            logger.info(f"🔄 Processing file: {filename}")

                            # Extract relevant sections from the research JSON
                            # The research JSON structure typically has sections like:
                            # - briefing_report (main content)
                            # - collected_data (raw research data)
                            # - research_queries (the queries used)

                            # Try to get the most relevant content
                            content_parts = []

                            if "briefing_report" in research_content:
                                content_parts.append(
                                    research_content["briefing_report"]
                                )

                            if "collected_data" in research_content:
                                collected = research_content["collected_data"]
                                # Extract content from collected documents
                                if isinstance(collected, dict):
                                    for url, doc in collected.items():
                                        if isinstance(doc, dict) and "content" in doc:
                                            content_parts.append(
                                                doc["content"][:1000]
                                            )  # Truncate to avoid huge docs

                            # Combine content
                            combined_content = "\n\n".join(content_parts)[
                                :30000
                            ]  # Limit total size

                            if not combined_content:
                                # Fallback: use full JSON as string
                                combined_content = json.dumps(
                                    research_content, indent=2
                                )[:30000]

                            doc_key = f"gdrive://{filename}"
                            local_docs[doc_key] = {
                                "title": filename.replace(".json", "")
                                .replace("_", " ")
                                .title(),
                                "content": combined_content,
                                "query": "google_drive_context",
                                "url": doc_key,
                                "source": "google_drive",
                                "score": 0.98,  # Very high relevance - it's previous research on same company
                                "analyst_type": self.analyst_type,
                                "timestamp": file_data.get(
                                    "created_time", datetime.now().isoformat()
                                ),
                                "content_length": len(combined_content),
                                "domain": "google_drive",
                            }
                            logger.debug(f"✓ Parsed research file: {filename}")

                        except Exception as e:
                            logger.warning(
                                f"⚠️ Failed to parse research file {file_data.get('filename')}: {e}"
                            )
                            continue

                    if local_docs:
                        logger.info(f"✅ SUCCESS: Loaded {len(local_docs)} document(s) from Google Drive research files")
                        logger.info("🚫 SKIPPING TAVILY - Using existing research from Google Drive instead")
                else:
                    logger.warning("📭 NO FILES FOUND in Google Drive folder")
                    logger.info("⏭️ Will proceed with Tavily API search")

            except Exception as e:
                logger.error(
                    f"❌ FAILED to download research from Google Drive: {e}",
                    exc_info=True,
                )
                logger.info("⏭️ Falling back to Tavily API search")
        elif use_local_context and not google_drive_folder_url:
            logger.warning("⚠️ LOCAL CONTEXT MODE enabled but no Google Drive folder URL provided!")
            logger.info("⏭️ Will proceed with Tavily API search")
        else:
            logger.info("📡 NORMAL MODE - Will use Tavily API for research")

        # FALLBACK: Also check local file system if USE_LOCAL_FILES is enabled
        if (use_local_context or config.USE_LOCAL_FILES) and not local_docs:
            try:
                for directory in config.get_local_context_dirs():
                    dir_path = Path(directory)
                    if not dir_path.exists():
                        continue
                    for file_path in dir_path.glob("**/*"):
                        if not file_path.is_file():
                            continue
                        # Only ingest small-ish text/json/markdown/pdf summary placeholders for now
                        if file_path.suffix.lower() in {".txt", ".md", ".json"}:
                            try:
                                raw = file_path.read_text(errors="ignore")[:20000]
                                content = raw
                                if file_path.suffix.lower() == ".md":
                                    try:
                                        html = markdown.markdown(raw)
                                        content = BeautifulSoup(
                                            html, "html.parser"
                                        ).get_text(separator="\n")
                                    except Exception:
                                        pass
                                elif file_path.suffix.lower() == ".json":
                                    try:
                                        data = json.loads(raw)
                                        # Flatten simple JSON structures
                                        if isinstance(data, dict):
                                            content = json.dumps(data, indent=2)[:20000]
                                        elif isinstance(data, list):
                                            content = json.dumps(data[:50], indent=2)[
                                                :20000
                                            ]
                                    except Exception:
                                        pass
                                doc_key = f"local://{file_path.name}"
                                local_docs[doc_key] = {
                                    "title": file_path.stem,
                                    "content": content,
                                    "query": "local_context",
                                    "url": doc_key,
                                    "source": "local_file",
                                    "score": 0.95,  # High relevance so curator keeps it
                                    "analyst_type": self.analyst_type,
                                    "timestamp": datetime.now().isoformat(),
                                    "content_length": len(content),
                                    "domain": "local",
                                }
                            except Exception as e:
                                logger.debug(
                                    f"Failed to read local file {file_path}: {e}"
                                )
                if local_docs:
                    logger.info(
                        f"📂 Loaded {len(local_docs)} local context documents for {self.analyst_type}"
                    )
            except Exception as e:
                logger.warning(f"Failed to load local context directories: {e}")

        # Prepare all search parameters upfront (only if not local-only)
        search_params = {
            "search_depth": "basic",
            "include_raw_content": False,
            "max_results": 3,
        }

        # Customize search parameters based on analyst type
        if self.analyst_type == "news_signal":
            search_params.update(
                {
                    "topic": "news",
                    "search_depth": "advanced",  # More depth for news
                    "max_results": 10,  # More results for news
                }
            )
        elif self.analyst_type == "flw_analyzer":
            search_params.update(
                {
                    "search_depth": "advanced",  # More depth for ESG/sustainability
                    "max_results": 4,  # More results for comprehensive analysis
                }
            )
        elif self.analyst_type == "company_brief":
            search_params.update(
                {
                    "max_results": 4,  # More results for company overview
                    "include_raw_content": True,  # Get full content for better analysis
                }
            )

        if websocket_manager and job_id:
            await websocket_manager.send_status_update(
                job_id=job_id,
                status="search_started",
                message=f"Using Tavily to search for {len(queries)} queries ({self.analyst_type})",
                result={"step": "Searching", "total_queries": len(queries)},
            )

        # Log search parameters for debugging
        logger.info(f"=== Search Parameters for {self.analyst_type} ===")
        logger.info(f"Parameters: {search_params}")
        logger.info(f"Total queries to search: {len(queries)}")
        logger.info("Queries:")
        for i, query in enumerate(queries, 1):
            logger.info(f"{i}. {query}")
        logger.info("=== End Search Parameters ===")

        # Set the analyst type on the mock client
        if isinstance(self.tavily_client, MockTavilyClient):
            self.tavily_client.set_analyst_type(self.analyst_type)

        # If we have local/GDrive documents and use_local_context is enabled, skip Tavily entirely
        if use_local_context and local_docs:
            logger.info(
                f"🚫 Local context mode: returning {len(local_docs)} documents without Tavily calls"
            )
            return local_docs

        # If USE_LOCAL_ONLY env var is set and we have docs, also skip Tavily
        if config.USE_LOCAL_ONLY and local_docs:
            logger.info(
                f"🚫 Local-only mode (env): returning {len(local_docs)} local documents without Tavily calls"
            )
            return local_docs

        # If use_local_context is enabled but no docs found, warn and continue to Tavily
        if use_local_context and not local_docs:
            logger.warning(
                "⚠️ Local context mode enabled but no documents found. Falling back to Tavily API."
            )
            # Continue to Tavily search below

        # Prepare search tasks with error handling
        search_tasks = []
        for query in queries:
            try:
                # Add query-specific parameters
                query_params = search_params.copy()

                # Adjust parameters based on query type
                if any(kw in query.lower() for kw in ["2024", "2025"]):
                    query_params["max_results"] = max(
                        query_params.get("max_results", 3) + 2, 5
                    )

                if "news" in query.lower():
                    query_params["topic"] = "news"

                # Create search task
                task = self.tavily_client.search(query, **query_params)
                search_tasks.append((query, task))
                logger.debug(
                    f"Created search task for query '{query}' with params: {query_params}"
                )
            except Exception as e:
                logger.error(f"Failed to create search task for query '{query}': {e}")
                continue

        # Execute all API calls in parallel
        try:
            # Extract just the tasks for gathering
            tasks = [task for _, task in search_tasks]
            raw_results = await asyncio.gather(*tasks, return_exceptions=True)

            # Process results with their corresponding queries
            results = []
            for i, result in enumerate(raw_results):
                query = search_tasks[i][0]  # Get the query for this result
                if isinstance(result, Exception):
                    logger.error(
                        f"Search failed for query '{query}' in {self.analyst_type}: {result}"
                    )
                    continue

                # Validate result format
                if not isinstance(result, dict):
                    logger.error(
                        f"Invalid result format for query '{query}': {type(result)}"
                    )
                    continue

                if "results" not in result:
                    logger.error(
                        f"Missing 'results' key in response for query '{query}'"
                    )
                    continue

                results.append({"query": query, "data": result})

            if not results:
                logger.error(f"All searches failed for {self.analyst_type}")
                return {}

        except Exception as e:
            logger.error(
                f"Error during parallel search execution for {self.analyst_type}: {e}"
            )
            return {}

        # Process results
        merged_docs = {}
        if not results:
            logger.warning(f"No search results returned for {self.analyst_type}")
            # Still return local docs if available
            return {**local_docs}

        for result_obj in results:
            query = result_obj["query"]
            result_data = result_obj["data"]

            if not isinstance(result_data, dict):
                logger.warning(
                    f"Invalid result data type for query '{query}': {type(result_data)}"
                )
                continue

            for item in result_data.get("results", []):
                if not isinstance(item, dict):
                    logger.warning(
                        f"Invalid result item type for query '{query}': {type(item)}"
                    )
                    continue

            for item in result_data.get("results", []):
                if not item.get("content") or not item.get("url"):
                    continue
                try:
                    # URL validation
                    url = item.get("url", "").strip()
                    try:
                        parsed_url = urlparse(url)
                        if not all(
                            [parsed_url.scheme in ("http", "https"), parsed_url.netloc]
                        ):
                            logger.warning(
                                f"Invalid URL format in search result: {url}"
                            )
                            continue
                    except Exception as e:
                        logger.warning(f"URL parsing failed: {url} - {e}")
                        continue

                    # Title processing
                    title = item.get("title", "").strip()
                    if title:
                        try:
                            title = clean_title(title)
                            if title.lower() == url.lower() or not title.strip():
                                title = ""
                        except Exception as e:
                            logger.warning(f"Title cleaning failed: {e}")
                            title = ""

                    # Content validation
                    content = item.get("content", "").strip()
                    if not content or len(content) < 50:
                        logger.warning(
                            f"Insufficient content length ({len(content) if content else 0}) for URL: {url}"
                        )
                        continue

                    # Score validation
                    try:
                        score = float(item.get("score", 0.0))
                        if score <= 0.2:
                            logger.debug(
                                f"Low relevance score ({score:.2f}) for URL: {url}"
                            )
                            continue
                    except (TypeError, ValueError) as e:
                        logger.warning(
                            f"Invalid score value: {item.get('score')} - {e}"
                        )
                        continue

                    try:
                        # Create document with all fields
                        doc = {
                            "title": title or "Untitled",
                            "content": content,
                            "query": query,
                            "url": url,
                            "source": "web_search",
                            "score": score,
                            "analyst_type": self.analyst_type,
                            "timestamp": datetime.now().isoformat(),
                            "content_length": len(content),
                            "domain": urlparse(url).netloc,
                        }

                        # Add document to results
                        merged_docs[url] = doc
                        logger.debug(
                            f"Added document: {url} (score: {score:.2f}, length: {len(content)})"
                        )
                    except Exception as e:
                        logger.error(f"Failed to create document for {url}: {e}")
                        continue
                except Exception as e:
                    logger.error(f"Error processing search result: {e}")
                    continue

        # Send completion status
        if websocket_manager and job_id:
            await websocket_manager.send_status_update(
                job_id=job_id,
                status="search_complete",
                message=f"Search for {self.analyst_type} completed with {len(merged_docs)} documents",
                result={
                    "step": "Searching",
                    "total_documents": len(merged_docs),
                    "queries_processed": len(queries),
                },
            )

        # Final validation and stats logging
        logger.info(f"=== Search Results Summary for {self.analyst_type} ===")
        logger.info(f"Total documents found: {len(merged_docs)}")
        if merged_docs:
            avg_score = sum(doc["score"] for doc in merged_docs.values()) / len(
                merged_docs
            )
            logger.info(f"Average relevance score: {avg_score:.2f}")
            logger.info("Documents by query:")
            query_counts = {}
            for doc in merged_docs.values():
                query_counts[doc["query"]] = query_counts.get(doc["query"], 0) + 1
            for query, count in query_counts.items():
                logger.info(f"  • {query}: {count} documents")
        logger.info("=== End Search Results Summary ===")

        # Merge local docs (if any) with web results, preferring web when URL collides
        if local_docs:
            merged_docs = {**local_docs, **merged_docs}
        return merged_docs
