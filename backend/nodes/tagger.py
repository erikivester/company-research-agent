# backend/nodes/tagger.py
import logging
import os
import re
import asyncio
from typing import Any, Dict, List, Tuple

from openai import AsyncOpenAI
from langchain_core.messages import AIMessage

# Make sure ResearchState is imported correctly relative to this file's location
from ..classes import ResearchState
# Make sure the uploader function can be imported
from backend.airtable_uploader import update_airtable_record

logger = logging.getLogger(__name__)

class Tagger:
    """Classifies the company based on research briefings using OpenAI."""

    def __init__(self) -> None:
        self.openai_key = os.getenv("OPENAI_API_KEY")
        if not self.openai_key:
            raise ValueError("OPENAI_API_KEY environment variable is not set")

        # Configure OpenAI Client
        self.openai_client = AsyncOpenAI(api_key=self.openai_key)

        # Store the classification rules
        self.classification_rules = self._load_classification_rules()

    def _load_classification_rules(self) -> Dict[str, List[str]]:
        """Loads the predefined classification options."""
        # These should exactly match your Airtable options
        return {
            "Country/Region": [
                "North America (US/CA)", "Latin America & Caribbean", "Europe",
                "Middle East & North Africa (MENA)", "Sub-Saharan Africa", "South Asia",
                "East Asia", "Southeast Asia", "Australia & New Zealand", "Global", "Unknown"
            ],
            "Industries": [
                "Agriculture & Aquaculture", "Food & Beverage Manufacturing", "Grocery & Food Retail",
                "Restaurants & Foodservice", "Hospitality & Lodging", "Transportation & Logistics",
                "Packaging & Containers", "Waste & Resource Recovery (Recycling/Compost/AD)",
                "E-commerce & Marketplaces", "Technology (Hardware & Software)",
                "Data & Analytics Services", "Professional Services & Consulting",
                "Marketing, Media & Advertising", "Consumer Goods (Non-Food)",
                "Apparel & Outdoor", "Beauty & Personal Care", "Sports & Recreation",
                "Financial Services & Insurance", "Real Estate & Facilities",
                "Energy & Utilities", "Chemicals & Materials", "Automotive & Mobility",
                "Travel & Tourism", "Healthcare & Life Sciences", "Education",
                "Government & Public Sector", "Nonprofit & Philanthropy", "Unknown"
            ],
            "Revenue Band (est.)": [
                "<$1M", "$1M-$10M", "$10M-$50M", "$50M-$100M",
                "$100M-$500M", "$500M-$1B", "$1B+", "Unknown"
            ]
        }


    async def classify_company(self, state: ResearchState) -> ResearchState:
        """Classifies the company using OpenAI based on briefings."""
        company = state.get('company', 'Unknown Company')
        logger.info(f"Starting classification for {company}...")

        # --- Gather Content for Classification ---
        briefings_content = []
        financial_briefing_text = ""
        
        # Inject HQ Location into content for regional classification
        hq_location = state.get('hq_location')
        if hq_location and hq_location.strip() and hq_location.lower() != 'unknown':
            briefings_content.append(f"## Location Context\n* Headquarters: {hq_location}")
            
        if financial_briefing := state.get("financial_briefing"):
            # Ensure briefing is a non-empty string before using
            if isinstance(financial_briefing, str) and financial_briefing.strip():
                financial_briefing_text = financial_briefing
                briefings_content.append(f"## Financial Overview\n{financial_briefing}")
        if company_briefing := state.get("company_briefing"):
            if isinstance(company_briefing, str) and company_briefing.strip():
                briefings_content.append(f"## Company Overview\n{company_briefing}")
        if industry_briefing := state.get("industry_briefing"):
            if isinstance(industry_briefing, str) and industry_briefing.strip():
                briefings_content.append(f"## Industry Overview\n{industry_briefing}")

        site_scrape = state.get("site_scrape", {})
        if site_scrape:
            # Safely get raw_content, defaulting to empty string
            site_context = "\n\n".join(
                f"URL: {url}\nContent Snippet:\n{data.get('raw_content', '')[:1000]}..."
                for url, data in list(site_scrape.items())[:3] # Limit context size
            )
            if site_context.strip(): # Check if context was actually generated
                briefings_content.append(f"## Website Content Snippets\n{site_context}")

        if not briefings_content:
            logger.warning("No valid briefing or site scrape content available for classification.")
            return state # Return early if no content

        combined_briefings = "\n\n".join(briefings_content)

        # --- Prepare Classification Prompts ---
        prompts = {}
        # Industry Prompt
        prompts["Industries"] = f"""
Analyze the following company information for "{company}":
--- START COMPANY INFO ---
{combined_briefings}
--- END COMPANY INFO ---
Based *only* on the information provided, select up to 3 relevant industries for this company from the list below. Prioritize specific verticals mentioned. Do not guess. If no industry fits well, output "None".
Available Industries: {', '.join(self.classification_rules['Industries'])}
Output only the selected industry names, separated by commas.
"""
        # Country/Region Prompt
        prompts["Country/Region"] = f"""
Analyze the following company information for "{company}", paying close attention to locations, addresses, shipping, languages, TLDs, or explicit region mentions:
--- START COMPANY INFO ---
{combined_briefings}
--- END COMPANY INFO ---
Based *only* on the information provided, select all applicable regions of operation. Select "Global" only if explicitly stated. If no region can be determined, output "None".
Available Regions: {', '.join(self.classification_rules['Country/Region'])}
Output only the selected region names, separated by commas.
"""
        # Revenue Band Prompt (Uses only financial briefing if available and valid)
        if financial_briefing_text:
             prompts["Revenue Band (est.)"] = f"""
Analyze the following financial information for "{company}":
--- START FINANCIAL INFO ---
{financial_briefing_text}
--- END FINANCIAL INFO ---
Based *only* on the financial information provided (like total funding, revenue figures, company size hints), estimate the company's annual revenue band. Choose exactly ONE option from the list below that best fits the evidence. Do not guess or extrapolate heavily. If the information is insufficient to make a reasonable estimate, output "None".

Available Revenue Bands:
{', '.join(self.classification_rules['Revenue Band (est.)'])}

Output only the single selected revenue band name. Example: $10M-$50M
"""
        else:
             logger.info("Skipping Revenue Band estimation as Financial Briefing is missing or empty.")


        # --- Call OpenAI API for each classification ---
        classification_results = {}
        tasks = []

        async def get_classification(field_name: str, prompt: str):
            # Inner function to process one classification request
            try:
                logger.info(f"Requesting OpenAI classification for: {field_name}")
                response = await self.openai_client.chat.completions.create(
                    model="gpt-4o-mini", # Using o-mini for cost/speed balance
                    messages=[
                        {"role": "system", "content": "You are an expert analyst classifying companies based on provided text and strict category options. Output ONLY the category name(s) from the provided list, separated by commas if multiple are allowed for the field. If none apply or info is insufficient, output 'None'."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0,
                    max_tokens=150
                )
                result_text = response.choices[0].message.content.strip()
                logger.info(f"OpenAI response for {field_name}: {result_text}")

                if not result_text or result_text.lower() == "none":
                    return field_name, [] # Return empty list for "None" or empty response

                # Split tags, strip whitespace
                selected_tags = [tag.strip() for tag in result_text.split(',') if tag.strip()] # Filter empty strings

                # Validate against allowed options for the specific field
                allowed_options = self.classification_rules.get(field_name, [])
                valid_tags = [tag for tag in selected_tags if tag in allowed_options]

                if not valid_tags:
                     logger.warning(f"OpenAI returned tags for {field_name}, but none matched allowed options: {selected_tags}")
                     return field_name, [] # Return empty list if no valid tags found

                # For single-select fields like Revenue, ensure only one tag is returned
                if field_name == "Revenue Band (est.)":
                    logger.info(f"Taking first valid tag for single-select field '{field_name}': {valid_tags[0]}")
                    return field_name, [valid_tags[0]] # Return only the first valid tag in a list

                return field_name, valid_tags # Return list of valid tags

            except Exception as e:
                logger.error(f"Error getting OpenAI classification for {field_name}: {e}", exc_info=True) # Add exc_info
                return field_name, [] # Return empty list on error

        # Create and run tasks concurrently
        for field, prompt_text in prompts.items():
             tasks.append(get_classification(field, prompt_text))

        results = await asyncio.gather(*tasks)

# Store results in state using specific keys
        airtable_tags = {} # For logging purposes
        # Fields that should default to ['Unknown'] if LLM returns None or invalid tags
        default_to_unknown_fields = ["Country/Region", "Revenue Band (est.)", "Industries"] 
        
        for field, tags in results:
            # 1. Determine the correct state key
            base_key_name = field.lower().replace('/','_').replace(' ','_')
            if base_key_name.endswith('_(est.)'):
                state_key = f"airtable_{base_key_name.replace('_(est.)','_est')}"
            else:
                state_key = f"airtable_{base_key_name}"

            # 2. Determine the initial value/apply defaulting logic
            if tags: # Tags were successfully classified and validated
                value_to_save = tags
            elif field in default_to_unknown_fields:
                value_to_save = ['Unknown']
                logger.info(f"No valid tags found for '{field}'. Defaulting state key '{state_key}' to ['Unknown'].")
            else:
                value_to_save = []

            # --- NEW FEATURE: Cap Country/Region at ['Global'] if more than 2 regions are found ---
            if field == "Country/Region" and len(value_to_save) > 2:
                # Log the change
                logger.info(f"Overriding Country/Region tags (found {len(value_to_save)} regions: {value_to_save}) to ['Global'].")
                value_to_save = ['Global']
            # --- END NEW FEATURE ---
            
            if value_to_save:
                # Use value_to_save for logging, not the original 'tags'
                airtable_tags[field] = value_to_save 
                logger.info(f"Updating state key '{state_key}' with tags: {value_to_save}")
                state[state_key] = value_to_save # Assign the list of tags to the state
            else:
                # Ensure key is initialized if it wasn't a defaulted field and had empty tags
                if state_key not in state:
                     logger.info(f"No valid tags for '{field}'. Setting state key '{state_key}' to empty list (no default).")
                     state[state_key] = []
                
        logger.info(f"Classification complete for {company}: {airtable_tags}")

        # Add results to messages list for logging/display
        if airtable_tags: # Only add message if tags were found
            log_message = f"📊 Classification results for {company}:\n" + "\n".join([f"  • {field}: {', '.join(tags)}" for field, tags in airtable_tags.items()])
            state.setdefault('messages', []).append(AIMessage(content=log_message))
        else:
            logger.info("No classification tags were successfully generated or validated.")
            state.setdefault('messages', []).append(AIMessage(content=f"📊 No classification tags identified for {company}."))


        # --- MORE DETAILED DEBUG LOGGING BEFORE RETURN ---
        logger.info("-" * 20)
        logger.info("DEBUG: Final state inspection BEFORE returning from classify_company:")
        current_keys = list(state.keys())
        logger.info(f"All Keys: {current_keys}")

        revenue_key_string = 'airtable_revenue_band_est' # Define the exact key string we expect

        # Check if the exact key string exists
        if revenue_key_string in state:
            logger.info(f"DEBUG: Key '{revenue_key_string}' FOUND in state.")
            value = state[revenue_key_string] # Access directly, not via .get()
            logger.info(f"DEBUG: Value via direct access state['{revenue_key_string}']: {value}")
            logger.info(f"DEBUG: Type via direct access: {type(value)}")
        else:
            logger.warning(f"DEBUG: Key '{revenue_key_string}' NOT FOUND in state using 'in'.")
            # Check if a visually similar key exists
            similar_keys = [k for k in current_keys if 'revenue' in k]
            if similar_keys:
                logger.warning(f"DEBUG: Found similar keys: {similar_keys}")
            else:
                logger.warning("DEBUG: No keys containing 'revenue' found.")

        # Check other keys for comparison
        logger.info(f"DEBUG: Value of airtable_industries BEFORE return: {state.get('airtable_industries')}")
        logger.info(f"DEBUG: Value of airtable_country_region BEFORE return: {state.get('airtable_country_region')}")
        logger.info("-" * 20)
        # --- END DEBUG LOGGING ---

        return state

    async def run(self, state: ResearchState) -> ResearchState:
        """Executes the tagger node."""
        airtable_record_id = state.get('airtable_record_id') # Get ID early for except block
        try:
            # --- Call Airtable Update ---
            if airtable_record_id:
                logger.info(f"Sending 'Classifying' status update to Airtable record: {airtable_record_id}")
                asyncio.create_task(
                    self._update_airtable_status(airtable_record_id, "Classifying")
                )
            # --- End Airtable Update Call ---

            # Run the main classification logic
            state = await self.classify_company(state)
            return state

        except Exception as e:
            logger.error(f"Error in Tagger node run method: {e}", exc_info=True)
            error_msg = f"⚠️ Tagger node failed critically: {str(e)}"
            state.setdefault('messages', []).append(AIMessage(content=error_msg))
            # --- ADD AIRTABLE FAILURE UPDATE ---
            if airtable_record_id:
                logger.info(f"Sending 'Tagger Failed' status update to Airtable record: {airtable_record_id}")
                asyncio.create_task(
                    self._update_airtable_status(airtable_record_id, f"Tagger Failed: {str(e)[:50]}") # Update status on error
                )
            # --- END ---
            # Ensure keys exist even on failure, set to empty lists
            state.setdefault('airtable_industries', [])
            state.setdefault('airtable_country_region', [])
            state.setdefault('airtable_revenue_band_est', [])
            return state

    async def _update_airtable_status(self, record_id: str, status_text: str):
        """Helper to call the synchronous update function."""
        try:
            # Import locally to potentially avoid startup circular dependencies
            # from backend.airtable_uploader import update_airtable_record
            logger.debug(f"Attempting to update Airtable record {record_id} status to '{status_text}'")
            # Direct call
            update_airtable_record(record_id, {'Research Status': status_text})
            logger.debug(f"Airtable status update successful for record {record_id}")
        except Exception as e:
            logger.error(f"Tagger node failed to update Airtable status for record {record_id}: {e}", exc_info=True) # Add exc_info