# backend/nodes/briefing.py
import asyncio
import logging
import os
from typing import Any, Dict, List, Union

import google.generativeai as genai

# Import the Airtable update function
from backend.airtable_uploader import update_airtable_record  # synchronous function
from backend.utils.utils import company_name

# Assuming ResearchState is in ../classes/state.py relative to this file
from langchain_core.messages import AIMessage

from ..classes import ResearchState
from ..utils.status_constants import ResearchStatus

logger = logging.getLogger(__name__)


class Briefing:
    """(v2) Creates polished briefings for each of the 5 v2 research categories."""

    def __init__(self) -> None:
        self.max_doc_length = 12000  # Increased from 8K for better content preservation
        self.max_total_length = 150000  # Increased from 80K to support more comprehensive briefings

        # Category-specific context budgets (aligned with query counts and priorities)
        self.category_budgets = {
            "company_brief": 35000,    # Core business (4 queries)
            "flw": 50000,              # PRIORITY: ReFED mission (6 queries - most important!)
            "news_signal": 35000,      # Time-sensitive coverage (5 queries)
            "engagement": 35000,       # Partnership opportunities (5 queries)
            "contact": 20000,          # Contact extraction (3 queries - simpler task)
        }

        self.gemini_key = os.getenv("GEMINI_API_KEY")
        if not self.gemini_key:
            raise ValueError("GEMINI_API_KEY environment variable is not set")

        # Configure Gemini
        genai.configure(api_key=self.gemini_key)
        self.gemini_model = genai.GenerativeModel(
            "gemini-2.5-flash-lite",  # COST OPTIMIZATION: Switched from gemini-2.5-flash for 70% cost savings
            generation_config=genai.types.GenerationConfig(
                temperature=0.1, max_output_tokens=8192  # Max for gemini-2.5-flash-lite
            ),
        )
        logger.info("Briefing node initialized with Gemini 2.5 Flash Lite model (cost-optimized, expanded context windows)")

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

    async def generate_category_briefing(
        self,
        docs: Union[Dict[str, Any], List[Dict[str, Any]]],
        category: str,
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Generates a briefing for a specific category using curated documents."""
        company = context.get("company", "Unknown")
        industry = context.get("industry", "Unknown")
        hq_location = context.get("hq_location", "Unknown")
        websocket_manager = context.get("websocket_manager")
        job_id = context.get("job_id")

        # Normalize docs to handle both dict and list inputs
        items = (
            list(docs.items())
            if isinstance(docs, dict)
            else [
                (doc.get("url", f"doc_{i}"), doc)
                for i, doc in enumerate(docs)
                if isinstance(doc, dict)  # Ensure doc is dict
            ]
        )
        num_docs = len(items)
        logger.info(
            f"Generating {category} briefing for {company} using {num_docs} documents"
        )

        # Send category start status
        if websocket_manager and job_id:
            await websocket_manager.send_status_update(
                job_id=job_id,
                status="briefing_start",
                message=f"Generating {category} briefing",
                result={
                    "step": "Briefing",
                    "category": category,  # This will be the v2 category name, e.g., 'contact'
                    "total_docs": num_docs,
                },
            )

        # --- v2: Define prompts for 5 new nodes ---
        prompts = {
            "company_brief": f"""Create a focused Company Brief for {company}, a {industry} company based in {hq_location}.
Your goal is to extract key business and financial facts from the provided documents.

**Instructions:**
1.  **Structure the brief** using the following suggested headers (you may modify this depending on what you find as relevent.). Use bullet points for details under each header.
    *   `### Core Business`: Concisely summarize the company's primary products, services, and mission.
    *   `### Financial Health`: List any revenue figures, funding details, or other financial signals (e.g., "Recent layoffs," "Stock price changes").
2.  **Be factual and specific:** Each bullet point must be a fact derived *only* from the provided documents.
3.  **Include direct quotes:** When relevant, include direct quotes from the source material to support your points.
4.  **Cite your sources:** At the end of each bullet point, include a citation in the format `(Source: [URL])`.
5.  **Omit empty sections:** If you cannot find any relevant information for a header in the documents, leave that section out entirely.
6.  **Fallback:** If no relevant information can be found for *any* section, output only this message: "A detailed company brief could not be generated from the provided documents."
7.  **Be concise:** Do not add any explanations or commentary outside of the structured brief.
""",
            "news_signal": f"""Create a "News & Signals" briefing for {company}.

**Instructions:**
1.  **Format:** Use a simple bulleted list. Do not use headers.
2.  **Content:** Scan the documents for actionable signals from the last 12-18 months that are DIRECTLY ABOUT {company}.
3.  **CRITICAL FILTERING:**
    * ONLY include news items that explicitly mention {company} by name or are clearly about {company}'s own actions, initiatives, or statements.
    * EXCLUDE generic industry news, competitor news, or news about other companies - even if they operate in the same sector as {company}.
    * EXCLUDE news that only mentions {company} in passing or in a list with other companies.
    * If an article discusses {company} AND other companies, only extract the portions specifically about {company}.
4.  **Tagging:** Start each bullet by tagging the signal type:
    * `**FLW/Climate Signal:**` - Food Loss & Waste or climate-related initiatives/achievements by {company}
    * `**Opportunity Signal:**` - New partnerships, programs, or business opportunities for {company}
    * `**Risk Signal:**` - Challenges, controversies, layoffs, or negative developments at {company}
    * `**General News:**` - Other significant company developments (executive changes, expansions, acquisitions)
5.  **Include direct quotes:** When available, include direct quotes from {company} executives, spokespeople, or official statements. Attribute quotes with the speaker's name and title.
6.  **Cite your sources:** At the end of each bullet point, include a citation in the format `(Source: [URL])`.
7.  **Fallback:** If no relevant news or signals about {company} specifically are found, output only this message: "No significant news or signals about {company} were identified in the provided documents."
8.  **Be concise:** Do not add any explanations or commentary outside the bulleted items.
""",
            "flw": f"""As a research analyst for ReFED, a national nonprofit dedicated to ending food loss and waste, create a focused briefing on {company}'s Food Loss & Waste (FLW) and Sustainability efforts. Your analysis should be framed by ReFED's mission to advance data-driven solutions.

**Instructions:**
1.  **Structure:** Use the following suggested headers *only if* you find relevant information for them. Use bullet points for details. Feel free to adapt this section depending on the output/discoveries made.
    *   `### ESG Goals`
    *   `### FLW Initiatives`
    *   `### Food Rescue & Donation`
    *   `### Methane Reduction Efforts`
2.  **Be factual and specific:** Each bullet must be a concise fact derived *only* from the provided documents.
3.  **Include direct quotes:** When relevant, include direct quotes from the source material to support your points.
4.  **Cite your sources:** At the end of each bullet point, include a citation in the format `(Source: [URL])`.
5.  **Omit empty sections:** If you cannot find information for a header, leave that section out.
6.  **Fallback:** If no relevant FLW or sustainability information is found for *any* section, output only this message: "No specific FLW or sustainability initiatives were identified in the provided documents."
7.  **Be concise:** Do not add any explanations or commentary.
""",
            "contact": f"""You are a JSON-only contact extractor. You must output ONLY a valid JSON array and nothing else.

For the provided documents about {company}, extract relevant contacts and output them as JSON.

**CRITICAL VALIDATION REQUIREMENTS:**
1. ONLY include contacts who are CURRENTLY EMPLOYED BY {company} (the target company)
2. EXCLUDE contacts from:
   - Partner organizations, nonprofits, or other companies that {company} works with
   - Consulting firms, agencies, or vendors
   - Previous employers (check dates carefully)
   - Board members unless they are also {company} employees
3. The contact's employer must be explicitly stated as {company} in the source document
4. If a person's affiliation is ambiguous or unclear, DO NOT include them

Output Format:
[
  {{
    "name": "Full Name",
    "title": "Exact Title",
    "company": "{company}",
    "summary": "2-3 sentence summary of role/responsibilities at {company}"
  }}
]

Rules:
1. Return ONLY raw JSON array - no markdown, no backticks, no explanation text
2. Include sustainability/impact/CSR/ESG mid-level managers and directors WHO WORK FOR {company}
3. Skip C-suite unless directly sustainability-related
4. Return empty array [] if no relevant contacts found who actually work for {company}
5. Ensure output is valid JSON with proper escaping
6. ALWAYS include the "company" field set to "{company}" for validation

Critical: Output MUST start with [ and end with ] - absolutely no other text or formatting""",
            "engagement": f"""Create an "Engagements & Affiliations" briefing for {company}.

**Instructions:**
1.  **Structure:** Use the header `### Engagements & Affiliations` followed by a bulleted list.
2.  **Content:** List all signals of external engagement, partnerships, and memberships.
3.  **Format:** Start each bullet with a category tag (e.g., `* **Membership:**`, `* **Event:**`, `* **Partnership:**`).
4.  **Fallback:** If no engagement signals are found, output only this message: "No significant engagements or affiliations were identified in the provided documents."
5.  **Be concise:** Do not add any explanations or commentary.
""",
        }
        # --- END v2 PROMPTS ---

        # Select the appropriate prompt, default to a generic one if category unknown
        prompt_template = prompts.get(
            category,
            f"Create a focused research briefing on {category} for {company} based on the provided documents.",
        )

        # Sort documents by evaluation score (highest first)
        try:
            sorted_items = sorted(
                items,
                key=lambda x: (
                    float(x[1].get("evaluation", {}).get("overall_score", 0))
                    if isinstance(x[1], dict)
                    else 0
                ),
                reverse=True,
            )
        except Exception as sort_exc:
            logger.error(
                f"Error sorting documents for {category}: {sort_exc}. Proceeding with unsorted docs."
            )
            sorted_items = items  # Fallback to unsorted

        # Prepare document text, limiting length
        # Use category-specific budget for better content distribution
        category_budget = self.category_budgets.get(category, self.max_total_length)

        doc_texts = []
        total_length = 0
        separator = "\n" + "-" * 40 + "\n"
        docs_included = 0
        docs_skipped = 0

        for score, doc in sorted_items:
            if not isinstance(doc, dict):
                logger.warning(
                    f"Skipping non-dictionary item during doc text preparation for {category}."
                )
                continue

            title = doc.get("title", "")
            content = doc.get("raw_content") or doc.get("content", "")

            if not isinstance(content, str):
                content = str(content)

            # Convert score to float for comparison
            try:
                score_float = float(score)
            except (ValueError, TypeError):
                score_float = 0.0  # Default to low quality if score invalid
                logger.warning(f"Invalid score type for {doc.get('url', 'unknown')}: {type(score)}, defaulting to 0.0")

            # Smart truncation: preserve more content for high-scoring documents
            if score_float >= 0.8:  # High-quality document
                max_content = self.max_doc_length  # Full 12K chars
            elif score_float >= 0.5:  # Medium-quality
                max_content = int(self.max_doc_length * 0.75)  # 9K chars
            else:  # Lower-quality
                max_content = int(self.max_doc_length * 0.5)  # 6K chars

            if len(content) > max_content:
                content = content[:max_content] + "... [content truncated]"

            doc_url = doc.get("url", "Unknown Source")
            doc_entry = f"Source URL: {doc_url}\nTitle: {title}\n\nContent: {content}"

            entry_len = len(doc_entry) + len(separator)
            if total_length + entry_len < category_budget:
                doc_texts.append(doc_entry)
                total_length += entry_len
                docs_included += 1
            else:
                docs_skipped += 1
                # Continue checking - might find smaller docs that fit
                if docs_skipped > 5:  # Stop after skipping 5 docs
                    break

        logger.info(
            f"{category} briefing: Included {docs_included} docs ({total_length:,} chars of {category_budget:,} budget). Skipped {docs_skipped} docs."
        )

        if not doc_texts:
            logger.warning(
                f"No document content available to generate briefing for {category}."
            )
            if websocket_manager and job_id:
                await websocket_manager.send_status_update(
                    job_id=job_id,
                    status="briefing_complete",
                    message=f"No content for {category} briefing",
                    result={"step": "Briefing", "category": category, "success": False},
                )
        # Removed stray return statement outside of function

        # --- v2: Add appropriate instructions based on category ---
        if category == "contact":
            # For contacts, just provide the documents without markdown polishing instructions
            full_prompt = f"""{prompt_template}

---
Documents for Analysis:
{separator.join(doc_texts)}
---"""
        else:
            # For all other categories, include markdown polishing instructions
            full_prompt = f"""{prompt_template}

---
Documents for Analysis:
{separator.join(doc_texts)}
---

**Polishing Instructions:**
As you write the briefing, ensure clean markdown, remove any redundancies, and write in clear, professional language.
This briefing will be used directly in a report, so do not include any preamble, conversation, or meta-commentary.
Output ONLY the requested markdown content.
"""
        # --- End v2 Instructions ---

        logger.debug(f"Prompt length for {category}: {len(full_prompt)} characters.")

        # Log document URLs for debugging (especially for company_brief failures)
        if category == "company_brief":
            doc_urls = [doc.get("url", "Unknown") for _, doc in sorted_items if isinstance(doc, dict)]
            logger.info(f"company_brief document URLs: {doc_urls}")
            logger.info(f"Full prompt preview (first 500 chars): {full_prompt[:500]}")

        retries = 3
        delay = 5
        for attempt in range(retries):
            try:
                logger.info(
                    f"Sending prompt to Gemini for {category} briefing ({len(doc_texts)} docs)."
                )
                response = await self.gemini_model.generate_content_async(
                    full_prompt, request_options={"timeout": 300}
                )

                # Check if response was blocked BEFORE trying to access parts
                content = ""
                if response:
                    # Check for blocking at the prompt level first
                    if response.prompt_feedback and response.prompt_feedback.block_reason:
                        logger.warning(
                            f"Gemini blocked the {category} briefing prompt. Reason: {response.prompt_feedback.block_reason.name}"
                        )
                        content = ""  # Will be handled below
                    elif response.candidates and len(response.candidates) > 0:
                        # Safe to access parts now
                        if response.parts:
                            content = "".join(
                                part.text for part in response.parts if hasattr(part, "text")
                            ).strip()

                if not content:
                    finish_reason_str = "Unknown"
                    try:
                        # Default to unknown
                        finish_reason_str = "No content and no specific finish reason."

                        # Check for blocking at the prompt level
                        if (
                            response.prompt_feedback
                            and response.prompt_feedback.block_reason
                        ):
                            finish_reason_str = f"Blocked - {response.prompt_feedback.block_reason.name}"

                        # Check finish reason from candidates if available
                        elif response.candidates:
                            candidate = response.candidates[0]
                            finish_reason = candidate.finish_reason
                            finish_reason_str = (
                                finish_reason.name
                                if hasattr(finish_reason, "name")
                                else str(finish_reason)
                            )

                            # Also check for safety ratings if finish reason is SAFETY
                            if hasattr(finish_reason, 'name') and finish_reason.name == "SAFETY":
                                safety_ratings_str = "; ".join(
                                    [
                                        f"{rating.category.name}: {rating.probability.name}"
                                        for rating in candidate.safety_ratings
                                    ]
                                )
                                finish_reason_str += f" ({safety_ratings_str})"

                    except Exception as e:
                        finish_reason_str = f"Could not determine finish reason ({e})"

                    # Special handling for BLOCKED responses - retry with fewer/different documents
                    if "BLOCKED" in finish_reason_str.upper() and attempt < retries - 1:
                        logger.warning(
                            f"Gemini blocked {category} briefing (reason: {finish_reason_str}). Reducing document set and retrying..."
                        )
                        # For blocked content, reduce more aggressively and skip controversial-looking docs
                        # This typically happens with news articles about layoffs, boycotts, CEO changes, etc.
                        reduced_doc_count = max(1, len(doc_texts) // 3)  # More aggressive reduction
                        doc_texts = doc_texts[:reduced_doc_count]
                        logger.info(f"Reduced to {reduced_doc_count} documents to avoid safety filters")

                        # Regenerate prompt with fewer documents
                        if category == "contact":
                            full_prompt = f"""{prompt_template}

---
Documents for Analysis:
{separator.join(doc_texts)}
---"""
                        else:
                            full_prompt = f"""{prompt_template}

---
Documents for Analysis:
{separator.join(doc_texts)}
---

**Polishing Instructions:**
As you write the briefing, ensure clean markdown, remove any redundancies, and write in clear, professional language.
This briefing will be used directly in a report, so do not include any preamble, conversation, or meta-commentary.
Output ONLY the requested markdown content.

**IMPORTANT: Focus on factual, neutral reporting. Avoid sensitive topics like boycotts or controversies.**
"""
                        await asyncio.sleep(delay)
                        delay *= 2
                        continue  # Retry the loop

                    # Special handling for MAX_TOKENS - retry with reduced documents
                    elif "MAX_TOKENS" in finish_reason_str.upper() and attempt < retries - 1:
                        logger.warning(
                            f"Hit MAX_TOKENS for {category} briefing on attempt {attempt + 1}. Reducing document set and retrying..."
                        )
                        # Reduce document set to top 50% and regenerate prompt
                        reduced_doc_count = max(1, len(doc_texts) // 2)
                        doc_texts = doc_texts[:reduced_doc_count]
                        logger.info(f"Reduced to {reduced_doc_count} documents (from {len(doc_texts)*2})")

                        # Regenerate prompt with fewer documents
                        if category == "contact":
                            full_prompt = f"""{prompt_template}

---
Documents for Analysis:
{separator.join(doc_texts)}
---"""
                        else:
                            full_prompt = f"""{prompt_template}

---
Documents for Analysis:
{separator.join(doc_texts)}
---

**Polishing Instructions:**
As you write the briefing, ensure clean markdown, remove any redundancies, and write in clear, professional language.
This briefing will be used directly in a report, so do not include any preamble, conversation, or meta-commentary.
Output ONLY the requested markdown content.

**CRITICAL: This response MUST be concise and fit within output limits. Prioritize the most important information.**
"""
                        await asyncio.sleep(delay)
                        delay *= 2
                        continue  # Retry the loop

                    error_message = f"Briefing generation for '{category}' failed due to an empty response from the AI model (Finish Reason: {finish_reason_str}). This section will be omitted."
                    logger.error(error_message)
                    # Log at ERROR level (not DEBUG) so we can see it in production logs
                    logger.error(
                        f"Full Gemini response for failed {category} briefing: {response}"
                    )

                    if websocket_manager and job_id:
                        await websocket_manager.send_status_update(
                            job_id=job_id,
                            status="briefing_complete",
                            message=f"LLM failed for {category} briefing",
                            result={
                                "step": "Briefing",
                                "category": category,
                                "success": False,
                                "error": f"LLM Error: {finish_reason_str}",
                            },
                        )
                    return {"content": f"_{error_message}_"}

                logger.info(
                    f"Successfully generated {category} briefing (Length: {len(content)} characters)"
                )
                if websocket_manager and job_id:
                    await websocket_manager.send_status_update(
                        job_id=job_id,
                        status="briefing_complete",
                        message=f"Completed {category} briefing",
                        result={
                            "step": "Briefing",
                            "category": category,
                            "success": True,
                        },
                    )

                return {"content": content}
            except Exception as e:
                logger.error(
                    f"Error generating {category} briefing via LLM on attempt {attempt + 1}: {e}",
                    exc_info=True,
                )
                if attempt < retries - 1:
                    await asyncio.sleep(delay)
                    delay *= 2
                else:
                    if websocket_manager and job_id:
                        await websocket_manager.send_status_update(
                            job_id=job_id,
                            status="briefing_complete",
                            message=f"Error generating {category} briefing",
                            result={
                                "step": "Briefing",
                                "category": category,
                                "success": False,
                                "error": str(e),
                            },
                        )
                    return {"content": ""}
    # Removed stray return statement outside of function

    async def create_briefings(self, state: ResearchState) -> ResearchState:
        """(v2) Create briefings for all 5 v2 categories in parallel."""
        company = company_name(state)
        websocket_manager = state.get("websocket_manager")
        job_id = state.get("job_id")

        if websocket_manager and job_id:
            await websocket_manager.send_status_update(
                job_id=job_id,
                status="processing",
                message="Starting research briefings",
                result={"step": "Briefing"},
            )

        context = {
            "company": company,
            "industry": state.get("industry", "Unknown"),
            "hq_location": state.get("hq_location", "Unknown"),
            "websocket_manager": websocket_manager,
            "job_id": job_id,
        }
        logger.info(f"Creating section briefings for {company}")

        # --- v2 MODIFICATION: Updated categories dictionary ---
        # Maps v2 curated data keys -> (v2 prompt category, v2 briefing state key)
        categories = {
            "curated_company_brief_data": ("company_brief", "company_brief_briefing"),
            "curated_news_signal_data": ("news_signal", "news_signal_briefing"),
            "curated_flw_data": ("flw", "flw_sustainability_briefing"),
            "curated_contact_finder_data": ("contact", "contact_briefing"),
            "curated_engagement_finder_data": ("engagement", "engagement_briefing"),
        }
        # --- END v2 MODIFICATION ---

        briefings = {}
        briefing_tasks_details = []

        # Prepare tasks for parallel processing
        for curated_key, (cat, briefing_key) in categories.items():
            curated_data = state.get(curated_key, {})

            if curated_data and isinstance(curated_data, dict):
                logger.info(
                    f"Preparing briefing task for {cat} using {len(curated_data)} documents from {curated_key}"
                )
                briefing_tasks_details.append(
                    {
                        "category": cat,  # e.g., 'contact'
                        "briefing_key": briefing_key,  # e.g., 'contact_briefing'
                        "curated_data": curated_data,
                        "data_field": curated_key,
                    }
                )
            else:
                logger.info(
                    f"No data available or invalid format for {curated_key}, skipping {cat} briefing."
                )
                state[briefing_key] = ""  # Ensure the briefing key exists in the state

        # Process briefings in parallel if tasks were prepared
        if briefing_tasks_details:
            briefing_semaphore = asyncio.Semaphore(
                3
            )  # Limit to 3 concurrent Gemini calls

            async def process_briefing(task_details: Dict[str, Any]) -> Dict[str, Any]:
                """Process a single briefing with rate limiting."""
                async with briefing_semaphore:
                    result = await self.generate_category_briefing(
                        task_details["curated_data"], task_details["category"], context
                    )

                    briefing_content = result.get("content", "")
                    success = bool(briefing_content)

                    state[task_details["briefing_key"]] = briefing_content
                    if success:
                        briefings[task_details["category"]] = briefing_content
                        logger.info(
                            f"Completed {task_details['category']} briefing ({len(briefing_content)} chars)"
                        )
                    else:
                        logger.error(
                            f"Failed to generate briefing for {task_details['category']} using {task_details['data_field']}"
                        )

                    return {
                        "category": task_details["category"],
                        "success": success,
                        "length": len(briefing_content),
                    }

            logger.info(
                f"Starting execution of {len(briefing_tasks_details)} briefing tasks."
            )
            results = await asyncio.gather(
                *[process_briefing(task) for task in briefing_tasks_details]
            )

            successful_briefings = sum(1 for r in results if r.get("success"))
            total_length = sum(r.get("length", 0) for r in results)
            logger.info(
                f"Generated {successful_briefings}/{len(briefing_tasks_details)} briefings successfully. Total characters generated: {total_length}"
            )
        else:
            logger.warning(
                "No briefing tasks were prepared. Skipping parallel processing."
            )

        state["briefings"] = briefings
        logger.info("Finished creating all briefings.")
        return state

    async def run(self, state: ResearchState) -> ResearchState:
        """Executes the briefing generation process."""
        airtable_record_id = state.get("airtable_record_id")
        if airtable_record_id:
            await self._update_airtable_status(
                airtable_record_id, ResearchStatus.GENERATING_BRIEFINGS
            )

        try:
            return await self.create_briefings(state)
        except Exception as e:
            logger.error(
                f"Critical error during briefing node execution: {e}", exc_info=True
            )
            state.setdefault("messages", []).append(
                AIMessage(content=f"⚠️ Briefing node failed: {str(e)}")
            )
            state.setdefault("briefings", {})

            # --- v2 MODIFICATION: Ensure 5 new keys exist on failure ---
            briefing_keys_to_ensure = [
                "company_brief_briefing",
                "news_signal_briefing",
                "flw_sustainability_briefing",
                "contact_briefing",
                "engagement_briefing",
            ]
            # --- END v2 MODIFICATION ---

            for key in briefing_keys_to_ensure:
                state.setdefault(key, "")
            return state
