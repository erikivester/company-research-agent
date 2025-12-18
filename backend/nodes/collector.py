# backend/nodes/collector.py
import asyncio
import logging
import os
from urllib.parse import urlparse  # Ensure urlparse is imported

from langchain_core.messages import AIMessage
from openai import AsyncOpenAI

from backend.airtable_uploader import update_airtable_record  # synchronous function

from ..classes import ResearchState
from ..utils.status_constants import ResearchStatus

logger = logging.getLogger(__name__)


class Collector:
    """Collects and organizes all research data before curation."""

    def __init__(self):
        """Initialize the Collector with OpenAI client for URL selection."""
        self.openai_key = os.getenv("OPENAI_API_KEY")
        if self.openai_key:
            self.openai_client = AsyncOpenAI(api_key=self.openai_key)
        else:
            self.openai_client = None
            logger.warning("OPENAI_API_KEY not set. URL inference will use fallback logic.")

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
                f"{self.__class__.__name__} failed to update Airtable status for record {record_id}: {e}",
                exc_info=True,
            )

    # --- END MODIFIED HELPER METHOD ---

    async def _select_company_homepage_with_ai(self, company_name: str, url_candidates: list) -> str:
        """
        Use OpenAI to intelligently select the most likely company homepage from candidates.

        Args:
            company_name: Name of the company
            url_candidates: List of dicts with 'url' and 'score' keys

        Returns:
            The selected homepage URL or None if AI selection fails
        """
        if not self.openai_client:
            logger.warning("OpenAI client not available for URL selection")
            return None

        # Format candidates for the prompt (limit to top 10 for token efficiency)
        top_candidates = sorted(url_candidates, key=lambda x: x['score'], reverse=True)[:10]
        candidates_text = "\n".join([
            f"{i+1}. {c['url']} (search relevance: {c['score']:.2f})"
            for i, c in enumerate(top_candidates)
        ])

        prompt = f"""You are analyzing search results to identify the official company homepage.

Company Name: {company_name}

URL Candidates (from web search):
{candidates_text}

TASK: Select the MOST LIKELY official company homepage URL from the list above.

RULES:
- Choose the URL that is most likely the company's own website (not news sites, databases, or aggregators)
- Prefer URLs with the company name in the domain
- Prefer shorter, cleaner URLs (homepages are usually at the root domain)
- EXCLUDE: News sites (forbes.com, bloomberg.com, reuters.com, etc.)
- EXCLUDE: Company databases (crunchbase.com, zippia.com, linkedin.com, etc.)
- EXCLUDE: Financial aggregators (finance.yahoo.com, marketwatch.com, etc.)
- If NO URL looks like an official homepage, respond with "NONE"

OUTPUT FORMAT:
Respond with ONLY the full URL of the best match, or "NONE" if no good match exists.
Example: https://www.example.com
"""

        try:
            logger.info(f"Using AI to select homepage for {company_name} from {len(top_candidates)} candidates")

            response = await self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert at identifying official company websites from search results. Output ONLY the URL or 'NONE'."
                    },
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=100
            )

            selected_url = response.choices[0].message.content.strip()

            if selected_url.upper() == "NONE" or not selected_url.startswith("http"):
                logger.warning(f"AI could not identify a valid homepage URL. Response: {selected_url}")
                return None

            # Validate that the selected URL is actually in our candidates
            if not any(selected_url in c['url'] or c['url'] in selected_url for c in top_candidates):
                logger.warning(f"AI selected URL not in candidates: {selected_url}")
                return None

            logger.info(f"AI selected homepage: {selected_url}")
            return selected_url

        except Exception as e:
            logger.error(f"Error using AI to select homepage: {e}", exc_info=True)
            return None

    async def collect(self, state: ResearchState) -> ResearchState:
        """Collect and verify all research data is present."""
        company = state.get("company", "Unknown Company")
        msg = [f"📦 Collecting research data for {company}:"]
        websocket_manager = state.get("websocket_manager")
        job_id = state.get("job_id")

        if websocket_manager and job_id:
            await websocket_manager.send_status_update(
                job_id=job_id,
                status="processing",
                message=f"Collecting research data for {company}",
                result={"step": "Collecting"},
            )

        # --- v2 MODIFICATION: Updated research_types dictionary ---
        # This now maps to the 5 new/refocused researcher nodes and their state keys
        research_types = {
            "company_brief_data": "🏢 Company Brief",
            "news_signal_data": "📰 News & Signals",
            "flw_data": "🌿 FLW/Sustainability",  # This key remains the same
            "contact_finder_data": "👥 Contacts",
            "engagement_finder_data": "🛰️ Engagements",
        }
        # --- END v2 MODIFICATION ---

        # --- LOGIC: Collect all scored documents to infer missing company_url ---
        # --- FIX: We get the company_url from the state *first*. ---
        best_url = state.get("company_url")

        # DEBUG: Log the initial company_url value to diagnose overwrite issues
        logger.info(f"Collector: Initial company_url from state: '{best_url}' (type: {type(best_url).__name__})")

        # Normalize the URL to ensure it has http/https prefix
        if best_url and isinstance(best_url, str) and best_url.strip():
            best_url = best_url.strip()
            if not best_url.startswith(("http://", "https://")):
                best_url = f"https://{best_url}"
                logger.info(f"Collector: Normalized URL to include https: {best_url}")

        all_scored_docs = []

        # This loop now iterates over the v2 research_types
        for data_field, label in research_types.items():
            data = state.get(data_field, {})
            if data and isinstance(
                data, dict
            ):  # Check if data exists and is a dictionary

                # For URL inference, only use company_brief_data to avoid LinkedIn/contact URLs
                if data_field == "company_brief_data":
                    # Check for existing data URL and add to all_scored_docs
                    for url, doc in data.items():
                        # We look for the raw Tavily search score (which is present in all research documents)
                        score = doc.get("score", 0.0)
                        if url and score > 0.0:
                            all_scored_docs.append({"url": url, "score": score})

                msg.append(f"• {label}: {len(data)} documents collected")
            else:
                msg.append(f"• {label}: No data found")
                # Ensure the key exists in the state, even if empty, for downstream nodes
                if data_field not in state:
                    state[data_field] = {}

        # --- CRITICAL FIX: ---
        # Only run inference logic if NO company URL was provided at all
        # We check:
        # 1. best_url is None, OR
        # 2. best_url is empty string, OR
        # 3. best_url has no content after stripping whitespace
        has_valid_url = (
            best_url
            and isinstance(best_url, str)
            and best_url.strip()
            and best_url.strip() != ""
        )

        if not has_valid_url and all_scored_docs:
            logger.info("Collector: No valid company_url found, will attempt to infer from search results")

            # Try AI-powered URL selection first
            selected_url = await self._select_company_homepage_with_ai(company, all_scored_docs)

            if selected_url:
                # Clean URL to base domain (scheme://netloc)
                parsed = urlparse(selected_url)
                clean_base_url = f"{parsed.scheme}://{parsed.netloc.rstrip('/')}"

                state["company_url"] = clean_base_url
                logger.info(f"AI-selected company_url: {clean_base_url}")
                msg.append(f"🔗 **AI-selected Company URL**: {clean_base_url}")
            else:
                # Fallback: Try to find URL with company name in domain
                logger.info("AI selection failed, using smart fallback heuristic")

                # First, try to find a URL with the company name in the domain
                company_name_clean = company.lower().replace(" ", "").replace("-", "").replace(".", "")
                company_url_found = None

                for doc in all_scored_docs:
                    url_domain = urlparse(doc["url"]).netloc.lower().replace("www.", "")
                    url_clean = url_domain.replace("-", "").replace(".", "")

                    # Check if company name is in domain (e.g., "chickfila" in "chick-fil-a.com")
                    if company_name_clean[:6] in url_clean or url_clean[:6] in company_name_clean:
                        company_url_found = doc["url"]
                        logger.info(f"Fallback: Found URL with company name in domain: {company_url_found}")
                        break

                # If no company name match, use highest score heuristic
                if not company_url_found:
                    all_scored_docs.sort(
                        key=lambda x: (x["score"], -len(urlparse(x["url"]).path)), reverse=True
                    )
                    company_url_found = all_scored_docs[0]["url"]
                    logger.info(f"Fallback: No domain match, using highest score: {company_url_found}")

                if company_url_found and company_url_found.startswith("http"):
                    parsed = urlparse(company_url_found)
                    clean_base_url = f"{parsed.scheme}://{parsed.netloc.rstrip('/')}"

                    state["company_url"] = clean_base_url
                    logger.info(f"Fallback: Inferred company_url: {clean_base_url}")
                    msg.append(f"🔗 **Inferred Company URL** (fallback): {clean_base_url}")
                else:
                    logger.warning(f"Fallback URL '{company_url_found}' was invalid, skipping URL inference.")
        elif has_valid_url:
            # Preserve the user-provided or already-set URL
            logger.info(f"✓ Preserving company_url: {best_url}")
            msg.append(f"🔗 Using provided company URL: {best_url}")
            # Ensure it's saved in state (in case it was only normalized, not yet saved)
            state["company_url"] = best_url
        else:
            logger.warning(
                "No company_url provided and no documents found to infer from."
            )
            msg.append("⚠️ No company URL available")
        # --- End FIX ---

        # Update state with collection message
        messages = state.get("messages", [])
        messages.append(AIMessage(content="\n".join(msg)))
        state["messages"] = messages

        return state

    async def run(self, state: ResearchState) -> ResearchState:
        airtable_record_id = state.get("airtable_record_id")
        if airtable_record_id:
            await self._update_airtable_status(
                airtable_record_id, ResearchStatus.COLLECTING_DATA
            )
        return await self.collect(state)
