# backend/nodes/tagger.py
import asyncio
import logging
import os
from typing import Dict, List

from langchain_core.messages import AIMessage
from openai import AsyncOpenAI

# Make sure the uploader function can be imported
from backend.airtable_uploader import update_airtable_record  # synchronous function

# Make sure ResearchState is imported correctly relative to this file's location
from ..classes import ResearchState
from ..utils.status_constants import ResearchStatus
from ..utils.refed_alignment_definitions import (
    get_all_enhanced_prompts,
    get_alignment_category_names,
)

logger = logging.getLogger(__name__)


class Tagger:
    """(v2) Classifies the company based on v2 research briefings using OpenAI."""

    def __init__(self) -> None:
        self.openai_key = os.getenv("OPENAI_API_KEY")
        if not self.openai_key:
            raise ValueError("OPENAI_API_KEY environment variable is not set")

        # Configure OpenAI Client
        self.openai_client = AsyncOpenAI(api_key=self.openai_key)

        # Store the classification rules
        self.classification_rules = self._load_classification_rules()

    def _load_classification_rules(self) -> Dict[str, List[str]]:
        """(v2) Loads the predefined classification options, including ReFED Alignment."""
        # These should exactly match your Airtable options
        return {
            "Country/Region": [
                "North America (US/CA)",
                "Latin America & Caribbean",
                "Europe",
                "Middle East & North Africa (MENA)",
                "Sub-Saharan Africa",
                "South Asia",
                "East Asia",
                "Southeast Asia",
                "Australia & New Zealand",
                "Global",
                "Unknown",
            ],
            "Industries": [
                "Agriculture & Aquaculture",
                "Food & Beverage Manufacturing",
                "Grocery & Food Retail",
                "Restaurants & Foodservice",
                "Hospitality & Lodging",
                "Transportation & Logistics",
                "Packaging & Containers",
                "Waste & Resource Recovery (Recycling/Compost/AD)",
                "E-commerce & Marketplaces",
                "Technology (Hardware & Software)",
                "Data & Analytics Services",
                "Professional Services & Consulting",
                "Marketing, Media & Advertising",
                "Consumer Goods (Non-Food)",
                "Apparel & Outdoor",
                "Beauty & Personal Care",
                "Sports & Recreation",
                "Financial Services & Insurance",
                "Real Estate & Facilities",
                "Energy & Utilities",
                "Chemicals & Materials",
                "Automotive & Mobility",
                "Travel & Tourism",
                "Healthcare & Life Sciences",
                "Education",
                "Government & Public Sector",
                "Nonprofit & Philanthropy",
                "Unknown",
            ],
            # --- MODIFIED: Expanded Revenue Bands ---
            "Revenue Band (est.)": [
                "<$1M",
                "$1M-$10M",
                "$10M-$50M",
                "$50M-$100M",
                "$100M-$500M",
                "$500M-$1B",
                "$1B-$10B",
                "$10B-$50B",
                "$50B+",
                "Unknown",
            ],
            # --- END MODIFICATION ---
            # --- NEW v2 ReFED Alignment Categories (loaded from definitions module) ---
            "ReFED Alignment": get_alignment_category_names(),
            # --- END NEW v2 ---
        }

    async def classify_company(self, state: ResearchState) -> ResearchState:
        """(v2) Classifies the company using OpenAI based on the 5 v2 briefings."""
        company = state.get("company", "Unknown Company")
        logger.info(f"Starting v2 classification for {company}...")

        # --- v2: Gather Content for Classification from 5 new briefings ---
        briefings_content = []
        company_brief_text = ""

        # DEBUG: Track which briefings have content
        briefing_status = {}

        # Inject HQ Location into content for regional classification
        hq_location = state.get("hq_location")
        if hq_location and hq_location.strip() and hq_location.lower() != "unknown":
            briefings_content.append(
                f"## Location Context\n* Headquarters: {hq_location}"
            )
            briefing_status["hq_location"] = True

        # Get Company Brief (for Revenue & Industry)
        if company_briefing := state.get("company_brief_briefing"):
            if isinstance(company_briefing, str) and company_briefing.strip():
                # Skip if this is an error message from failed briefing generation
                # Error messages are formatted as: "_Error message here_"
                if company_briefing.startswith("_") and company_briefing.endswith("_"):
                    logger.warning(
                        f"Skipping company briefing as it appears to be an error message: {company_briefing[:100]}"
                    )
                    briefing_status["company_brief"] = "ERROR"
                else:
                    company_brief_text = company_briefing
                    briefings_content.append(
                        f"## Company Overview & Financial Health\n{company_briefing}"
                    )
                    briefing_status["company_brief"] = f"{len(company_briefing)} chars"
        else:
            briefing_status["company_brief"] = "MISSING"

        # Get FLW Briefing (for Industry & ReFED Alignment)
        if flw_briefing := state.get("flw_sustainability_briefing"):
            if isinstance(flw_briefing, str) and flw_briefing.strip():
                briefings_content.append(
                    f"## FLW & Sustainability Briefing\n{flw_briefing}"
                )
                briefing_status["flw"] = f"{len(flw_briefing)} chars"
        else:
            briefing_status["flw"] = "MISSING"

        # Get News Briefing (for ReFED Alignment)
        if news_briefing := state.get("news_signal_briefing"):
            if isinstance(news_briefing, str) and news_briefing.strip():
                briefings_content.append(f"## News & Signals Briefing\n{news_briefing}")
                briefing_status["news"] = f"{len(news_briefing)} chars"
        else:
            briefing_status["news"] = "MISSING"

        # Get Engagement Briefing (for ReFED Alignment)
        if engagement_briefing := state.get("engagement_briefing"):
            if isinstance(engagement_briefing, str) and engagement_briefing.strip():
                briefings_content.append(
                    f"## Engagements & Affiliations Briefing\n{engagement_briefing}"
                )
                briefing_status["engagement"] = f"{len(engagement_briefing)} chars"
        else:
            briefing_status["engagement"] = "MISSING"

        # Get Contact Briefing (for context)
        if contact_briefing := state.get("contact_briefing"):
            if isinstance(contact_briefing, str) and contact_briefing.strip():
                briefings_content.append(
                    f"## Potential Contacts Briefing\n{contact_briefing}"
                )
                briefing_status["contact"] = f"{len(contact_briefing)} chars"
        else:
            briefing_status["contact"] = "MISSING"

        # --- DEBUG: Log briefing status ---
        logger.info(f"Tagger briefing status for ReFED classification: {briefing_status}")
        logger.info(f"Total combined briefing length: {len(''.join(briefings_content))} characters")
        # --- End v2 Content Gathering ---

        if not briefings_content:
            logger.warning("No valid briefing content available for classification.")
            # Ensure all keys are initialized as empty/unknown before returning
            state.setdefault("airtable_industries", ["Unknown"])
            state.setdefault("airtable_country_region", ["Unknown"])
            state.setdefault("airtable_revenue_band_est", ["Unknown"])
            state.setdefault("airtable_refed_alignment", [])
            return state

        combined_briefings = "\n\n".join(briefings_content)

        # --- v2: Prepare Classification Prompts ---
        prompts = {}
        # Industry Prompt (Uses combined briefings)
        prompts[
            "Industries"
        ] = f"""
Analyze the following company information for "{company}":
--- START COMPANY INFO ---
{combined_briefings}
--- END COMPANY INFO ---
Based *only* on the information provided, select up to 3 relevant industries for this company from the list below. Prioritize specific verticals mentioned. Do not guess. If no industry fits well, output "None".
Available Industries: {', '.join(self.classification_rules['Industries'])}
Output only the selected industry names, separated by commas.
"""
        # Country/Region Prompt (Uses combined briefings)
        prompts[
            "Country/Region"
        ] = f"""
Analyze the following company information for "{company}", paying close attention to locations, addresses, shipping, languages, TLDs, or explicit region mentions:
--- START COMPANY INFO ---
{combined_briefings}
--- END COMPANY INFO ---
Based *only* on the information provided, select all applicable regions of operation. Select "Global" only if explicitly stated. If no region can be determined, output "None".
Available Regions: {', '.join(self.classification_rules['Country/Region'])}
Output only the selected region names, separated by commas.
"""
        # Revenue Band Prompt (Uses company_brief_text if available, fallback to raw data)
        revenue_analysis_text = company_brief_text

        # Fallback: If company_brief_text is empty, try to extract financial info from raw curated data
        if not revenue_analysis_text:
            logger.info(
                "Company briefing is missing, attempting to extract financial info from raw curated data."
            )
            raw_company_data = state.get("curated_company_brief_data", {})
            if raw_company_data and isinstance(raw_company_data, dict):
                # Extract content from raw documents
                raw_financial_snippets = []
                for doc_url, doc_data in list(raw_company_data.items())[:3]:  # Limit to first 3 docs
                    if isinstance(doc_data, dict):
                        content = doc_data.get("raw_content") or doc_data.get("content", "")
                        if content and isinstance(content, str):
                            # Take first 500 chars from each document
                            raw_financial_snippets.append(content[:500])

                if raw_financial_snippets:
                    revenue_analysis_text = "\n\n---\n\n".join(raw_financial_snippets)
                    logger.info(
                        f"Extracted {len(raw_financial_snippets)} document snippets for revenue analysis (total {len(revenue_analysis_text)} chars)"
                    )

        if revenue_analysis_text:
            prompts[
                "Revenue Band (est.)"
            ] = f"""
Analyze the following financial information for "{company}":
--- START FINANCIAL INFO ---
{revenue_analysis_text}
--- END FINANCIAL INFO ---
Based *only* on the financial information provided (like total funding, revenue figures, company size hints), estimate the company's annual revenue band. Choose exactly ONE option from the list below that best fits the evidence. Do not guess or extrapolate heavily. If the information is insufficient to make a reasonable estimate, output "Unknown".

Available Revenue Bands:
{', '.join(self.classification_rules['Revenue Band (est.)'])}

Output only the single selected revenue band name. Example: $10M-$50M
"""
        else:
            logger.info(
                "Skipping Revenue Band estimation as no financial information is available."
            )

        # --- NEW v2 ReFED Alignment Prompt (Enhanced with Rich Context) ---
        enhanced_categories = get_all_enhanced_prompts()
        prompts[
            "ReFED Alignment"
        ] = f"""
You are a ReFED analyst tasked with identifying strategic alignment opportunities. Analyze all the provided briefings for "{company}" to identify all areas where this organization aligns with ReFED's mission to catalyze the food system toward evidence-based action to stop wasting food.

ReFED Context:
• Mission: Catalyze the food system toward evidence-based action to stop wasting food
• Vision: A sustainable, resilient, and inclusive food system that optimizes environmental resources, minimizes climate impacts, and makes the best use of the food we grow
• Core Strategies: Data & Insights, Capital & Innovation, Collaborative Action

--- START COMPANY INFO ---
{combined_briefings}
--- END COMPANY INFO ---

Based *only* on the information provided above, carefully evaluate each alignment category below. Select ALL categories where you find clear, specific evidence of alignment. Look for concrete signals—not vague sustainability mentions. If no specific signals are present for any category, output "None".

=== REFED ALIGNMENT CATEGORIES ===

{enhanced_categories}

=== CLASSIFICATION INSTRUCTIONS ===

1. Review each category's Mission Alignment and Key Signals carefully
2. Look for SPECIFIC evidence in the company briefings that matches the signals
3. Select ALL categories with clear supporting evidence
4. Do NOT select categories based on assumptions or general sustainability mentions
5. Output ONLY the exact category names (comma-separated) or "None"

Output format: Category Name 1, Category Name 2, Category Name 3
Or: None
"""
        # --- END v2 PROMPTS ---

        # --- Call OpenAI API for each classification ---
        tasks = []

        async def get_classification(field_name: str, prompt: str):
            # Inner function to process one classification request
            try:
                logger.info(f"Requesting OpenAI classification for: {field_name}")

                # DEBUG: Log the prompt being sent for ReFED Alignment
                if field_name == "ReFED Alignment":
                    logger.info(f"ReFED Alignment prompt length: {len(prompt)} characters")
                    logger.info(f"ReFED Alignment prompt preview (first 1000 chars): {prompt[:1000]}")

                # Adjust system message for ReFED Alignment to emphasize evidence-based classification
                if field_name == "ReFED Alignment":
                    system_content = "You are a ReFED strategic analyst with deep knowledge of the food waste sector. Carefully evaluate the company information against each alignment category's mission alignment and key signals. Only select categories where you find SPECIFIC, CONCRETE evidence—not general sustainability mentions. Be thorough but precise. Output ONLY exact category names from the provided list, comma-separated. If no clear signals exist, output 'None'."
                else:
                    system_content = "You are an expert analyst classifying companies based on provided text and strict category options. Output ONLY the category name(s) from the provided list, separated by commas if multiple are allowed for the field. If none apply or info is insufficient, output 'None'."

                response = await self.openai_client.chat.completions.create(
                    model="gpt-4o-mini",  # Using o-mini for cost/speed balance
                    messages=[
                        {
                            "role": "system",
                            "content": system_content,
                        },
                        {"role": "user", "content": prompt},
                    ],
                    temperature=0,
                    max_tokens=300,  # Increased max tokens for ReFED Alignment list
                )
                result_text = response.choices[0].message.content.strip()
                logger.info(f"OpenAI response for {field_name}: {result_text}")

                # DEBUG: Additional logging for ReFED Alignment
                if field_name == "ReFED Alignment":
                    logger.info(f"ReFED Alignment raw response: '{result_text}' (type: {type(result_text).__name__}, length: {len(result_text)})")
                    logger.info(f"Available ReFED categories count: {len(self.classification_rules.get(field_name, []))}")

                if not result_text or result_text.lower() == "none":
                    return (
                        field_name,
                        [],
                    )  # Return empty list for "None" or empty response

                selected_tags = [
                    tag.strip() for tag in result_text.split(",") if tag.strip()
                ]

                allowed_options = self.classification_rules.get(field_name, [])
                valid_tags = [tag for tag in selected_tags if tag in allowed_options]

                if not valid_tags:
                    logger.warning(
                        f"OpenAI returned tags for {field_name}, but none matched allowed options: {selected_tags}"
                    )
                    return field_name, []

                if field_name == "Revenue Band (est.)":
                    logger.info(
                        f"Taking first valid tag for single-select field '{field_name}': {valid_tags[0]}"
                    )
                    return field_name, [valid_tags[0]]

                return field_name, valid_tags

            except Exception as e:
                logger.error(
                    f"Error getting OpenAI classification for {field_name}: {e}",
                    exc_info=True,
                )
                return field_name, []

        # Create and run tasks concurrently
        for field, prompt_text in prompts.items():
            tasks.append(get_classification(field, prompt_text))

        results = await asyncio.gather(*tasks)

        # Store results in state using specific keys
        airtable_tags = {}

        # v2: Updated default list
        default_to_unknown_fields = [
            "Country/Region",
            "Revenue Band (est.)",
            "Industries",
        ]

        for field, tags in results:
            # 1. Determine the correct state key
            base_key_name = field.lower().replace("/", "_").replace(" ", "_")
            if base_key_name.endswith("_(est.)"):
                state_key = f"airtable_{base_key_name.replace('_(est.)','_est')}"
            else:
                state_key = (
                    f"airtable_{base_key_name}"  # e.g., "airtable_refed_alignment"
                )

            # 2. Determine the initial value/apply defaulting logic
            if tags:  # Tags were successfully classified and validated
                value_to_save = tags
            elif field in default_to_unknown_fields:
                value_to_save = ["Unknown"]
                logger.info(
                    f"No valid tags found for '{field}'. Defaulting state key '{state_key}' to ['Unknown']."
                )
            else:
                value_to_save = []  # e.g., ReFED Alignment defaults to empty list

            # Cap Country/Region at ['Global'] if more than 2 regions are found
            if field == "Country/Region" and len(value_to_save) > 2:
                logger.info(
                    f"Overriding Country/Region tags (found {len(value_to_save)} regions: {value_to_save}) to ['Global']."
                )
                value_to_save = ["Global"]

            # 3. Save to state
            state[state_key] = value_to_save
            if value_to_save:
                airtable_tags[field] = value_to_save
                logger.info(
                    f"Updating state key '{state_key}' with tags: {value_to_save}"
                )
            else:
                logger.info(
                    f"No valid tags for '{field}'. Setting state key '{state_key}' to empty list."
                )

        logger.info(f"Classification complete for {company}: {airtable_tags}")

        # Add results to messages list for logging/display
        if airtable_tags:
            log_message = f"📊 Classification results for {company}:\n" + "\n".join(
                [
                    f"  • {field}: {', '.join(tags)}"
                    for field, tags in airtable_tags.items()
                ]
            )
            state.setdefault("messages", []).append(AIMessage(content=log_message))
        else:
            logger.info(
                "No classification tags were successfully generated or validated."
            )
            state.setdefault("messages", []).append(
                AIMessage(
                    content=f"📊 No classification tags identified for {company}."
                )
            )

        return state

    async def run(self, state: ResearchState) -> ResearchState:
        """Executes the tagger node."""
        airtable_record_id = state.get("airtable_record_id")
        try:
            # --- Call Airtable Update (Start Status) ---
            if airtable_record_id:
                logger.info(
                    f"Sending 'Classifying' status update to Airtable record: {airtable_record_id}"
                )
                await self._update_airtable_status(
                    airtable_record_id, ResearchStatus.CLASSIFYING
                )

            state = await self.classify_company(state)
            return state

        except Exception as e:
            logger.error(f"Error in Tagger node run method: {e}", exc_info=True)
            error_msg = f"⚠️ Tagger node failed critically: {str(e)}"
            state.setdefault("messages", []).append(AIMessage(content=error_msg))
            if airtable_record_id:
                logger.info(
                    f"Sending 'Tagger Failed' status update to Airtable record: {airtable_record_id}"
                )
                await self._update_airtable_status(
                    airtable_record_id,
                    ResearchStatus.format_error(
                        ResearchStatus.FAILED_CLASSIFICATION, str(e)
                    ),
                )

            # --- v2: Ensure ALL keys exist on failure ---
            state.setdefault("airtable_industries", ["Unknown"])
            state.setdefault("airtable_country_region", ["Unknown"])
            state.setdefault("airtable_revenue_band_est", ["Unknown"])
            state.setdefault("airtable_refed_alignment", [])
            return state

    async def _update_airtable_status(self, record_id: str, status_text: str):
        """Helper to call the synchronous update function in a separate thread."""
        if not record_id:
            logger.warning("Airtable status update skipped: No record ID provided.")
            return
        try:
            await asyncio.to_thread(
                update_airtable_record, record_id, {"Research Status": status_text}
            )
            logger.debug(f"Airtable status update successful for record {record_id}")
        except Exception as e:
            logger.error(
                f"{self.__class__.__name__} failed to update Airtable status for record {record_id}: {e}",
                exc_info=True,
            )
