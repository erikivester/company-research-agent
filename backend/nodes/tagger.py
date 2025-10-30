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
from backend.airtable_uploader import update_airtable_record # synchronous function

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
            ],
            # --- NEW v2 ReFED Alignment Categories ---
            "ReFED Alignment": [
                "Insights Engine Engagement", "Data Contributor / Partner", "Business Services Opportunity",
                "U.S. Food Waste Pact Prospect/Member", "FWFC: Capital-Seeking", "FWFC: Capital Provider",
                "Catalytic Grant Fund Fit", "Events & Sponsorship (Summit/FWAN)", "Policy & Public Affairs Alignment",
                "Measurement & Disclosure", "Solution Adopter (Corporate)", "Solution Provider (Vendor/Innovator)",
                "Communications & Thought Leadership"
            ]
            # --- END NEW v2 ---
        }


    async def classify_company(self, state: ResearchState) -> ResearchState:
        """(v2) Classifies the company using OpenAI based on the 5 v2 briefings."""
        company = state.get('company', 'Unknown Company')
        logger.info(f"Starting v2 classification for {company}...")

        # --- v2: Gather Content for Classification from 5 new briefings ---
        briefings_content = []
        company_brief_text = ""
        
        # Inject HQ Location into content for regional classification
        hq_location = state.get('hq_location')
        if hq_location and hq_location.strip() and hq_location.lower() != 'unknown':
            briefings_content.append(f"## Location Context\n* Headquarters: {hq_location}")
            
        # Get Company Brief (for Revenue & Industry)
        if company_briefing := state.get("company_brief_briefing"):
            if isinstance(company_briefing, str) and company_briefing.strip():
                company_brief_text = company_briefing
                briefings_content.append(f"## Company Overview & Financial Health\n{company_briefing}")
        
        # Get FLW Briefing (for Industry & ReFED Alignment)
        if flw_briefing := state.get("flw_sustainability_briefing"):
            if isinstance(flw_briefing, str) and flw_briefing.strip():
                briefings_content.append(f"## FLW & Sustainability Briefing\n{flw_briefing}")
        
        # Get News Briefing (for ReFED Alignment)
        if news_briefing := state.get("news_signal_briefing"):
            if isinstance(news_briefing, str) and news_briefing.strip():
                briefings_content.append(f"## News & Signals Briefing\n{news_briefing}")

        # Get Engagement Briefing (for ReFED Alignment)
        if engagement_briefing := state.get("engagement_briefing"):
            if isinstance(engagement_briefing, str) and engagement_briefing.strip():
                briefings_content.append(f"## Engagements & Affiliations Briefing\n{engagement_briefing}")

        # Get Contact Briefing (for context)
        if contact_briefing := state.get("contact_briefing"):
            if isinstance(contact_briefing, str) and contact_briefing.strip():
                briefings_content.append(f"## Potential Contacts Briefing\n{contact_briefing}")
        # --- End v2 Content Gathering ---

        if not briefings_content:
            logger.warning("No valid briefing content available for classification.")
            # Ensure all keys are initialized as empty/unknown before returning
            state.setdefault('airtable_industries', ['Unknown'])
            state.setdefault('airtable_country_region', ['Unknown'])
            state.setdefault('airtable_revenue_band_est', ['Unknown'])
            state.setdefault('airtable_refed_alignment', [])
            return state

        combined_briefings = "\n\n".join(briefings_content)

        # --- v2: Prepare Classification Prompts ---
        prompts = {}
        # Industry Prompt (Uses combined briefings)
        prompts["Industries"] = f"""
Analyze the following company information for "{company}":
--- START COMPANY INFO ---
{combined_briefings}
--- END COMPANY INFO ---
Based *only* on the information provided, select up to 3 relevant industries for this company from the list below. Prioritize specific verticals mentioned. Do not guess. If no industry fits well, output "None".
Available Industries: {', '.join(self.classification_rules['Industries'])}
Output only the selected industry names, separated by commas.
"""
        # Country/Region Prompt (Uses combined briefings)
        prompts["Country/Region"] = f"""
Analyze the following company information for "{company}", paying close attention to locations, addresses, shipping, languages, TLDs, or explicit region mentions:
--- START COMPANY INFO ---
{combined_briefings}
--- END COMPANY INFO ---
Based *only* on the information provided, select all applicable regions of operation. Select "Global" only if explicitly stated. If no region can be determined, output "None".
Available Regions: {', '.join(self.classification_rules['Country/Region'])}
Output only the selected region names, separated by commas.
"""
        # Revenue Band Prompt (Uses only company_brief_text if available)
        if company_brief_text:
             prompts["Revenue Band (est.)"] = f"""
Analyze the following financial information for "{company}":
--- START FINANCIAL INFO ---
{company_brief_text}
--- END FINANCIAL INFO ---
Based *only* on the financial information provided (like total funding, revenue figures, company size hints), estimate the company's annual revenue band. Choose exactly ONE option from the list below that best fits the evidence. Do not guess or extrapolate heavily. If the information is insufficient to make a reasonable estimate, output "None".

Available Revenue Bands:
{', '.join(self.classification_rules['Revenue Band (est.)'])}

Output only the single selected revenue band name. Example: $10M-$50M
"""
        else:
             logger.info("Skipping Revenue Band estimation as Company Briefing is missing or empty.")

        # --- NEW v2 ReFED Alignment Prompt ---
        prompts["ReFED Alignment"] = f"""
You are a ReFED analyst. Analyze all the provided briefings for "{company}" to identify all areas of alignment with ReFED's work.
--- START COMPANY INFO ---
{combined_briefings}
--- END COMPANY INFO ---
Based *only* on the text, select ALL relevant alignment categories from the list below. Do not guess. If no specific signals are present, output "None".

Available Alignment Categories:
- **Insights Engine Engagement:** (Signals: cites Insights Engine, Food Waste Monitor, Impact Calculator, Solutions Database.)
- **Data Contributor / Partner:** (Signals: open to data partnerships, APIs, dashboards, ESG data exchanges.)
- **Business Services Opportunity:** (Signals: public waste goals but no clear roadmap; pilot interest; RFPs.)
- **U.S. Food Waste Pact Prospect/Member:** (Signals: cross-value-chain commitments; supplier programs; scope 3 focus; signatory.)
- **FWFC: Capital-Seeking:** (Signals: company/nonprofit raising a round; pilot scale-up; impact financing needs.)
- **FWFC: Capital Provider:** (Signals: investor, lender, corporate VC, foundation with climate/food/ag focus.)
- **Catalytic Grant Fund Fit:** (Signals: nonprofit/initiative with prevention/rescue/recycling projects; measurable impact; funding gap.)
- **Events & Sponsorship (Summit/FWAN):** (Signals: conference speaking/sponsorship history; FWAN participation.)
- **Policy & Public Affairs Alignment:** (Signals: policy statements; government affairs team; coalitions on food waste.)
- **Measurement & Disclosure:** (Signals: public food loss/waste goals; CDP/ESG reports; WRAP/FLW Protocol usage.)
- **Solution Adopter (Corporate):** (Signals: actively piloting/rolling out solutions like inventory AI, dynamic pricing, donation programs, byproduct valorization.)
- **Solution Provider (Vendor/Innovator):** (Signals: B2B solution with retail/CPG/foodservice customers; case studies.)
- **Communications & Thought Leadership:** (Signals: sustainability campaigns, media reach, executive platforms.)

Output only the selected category names, separated by commas.
"""
        # --- END v2 PROMPTS ---

        # --- Call OpenAI API for each classification ---
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
                    max_tokens=250 # Increased max tokens for ReFED Alignment list
                )
                result_text = response.choices[0].message.content.strip()
                logger.info(f"OpenAI response for {field_name}: {result_text}")

                if not result_text or result_text.lower() == "none":
                    return field_name, [] # Return empty list for "None" or empty response

                selected_tags = [tag.strip() for tag in result_text.split(',') if tag.strip()]

                allowed_options = self.classification_rules.get(field_name, [])
                valid_tags = [tag for tag in selected_tags if tag in allowed_options]

                if not valid_tags:
                     logger.warning(f"OpenAI returned tags for {field_name}, but none matched allowed options: {selected_tags}")
                     return field_name, [] 

                if field_name == "Revenue Band (est.)":
                    logger.info(f"Taking first valid tag for single-select field '{field_name}': {valid_tags[0]}")
                    return field_name, [valid_tags[0]] 

                return field_name, valid_tags 

            except Exception as e:
                logger.error(f"Error getting OpenAI classification for {field_name}: {e}", exc_info=True) 
                return field_name, [] 

        # Create and run tasks concurrently
        for field, prompt_text in prompts.items():
             tasks.append(get_classification(field, prompt_text))

        results = await asyncio.gather(*tasks)

        # Store results in state using specific keys
        airtable_tags = {}
        
        # v2: Updated default list
        default_to_unknown_fields = ["Country/Region", "Revenue Band (est.)", "Industries"] 
        
        for field, tags in results:
            # 1. Determine the correct state key
            base_key_name = field.lower().replace('/','_').replace(' ','_')
            if base_key_name.endswith('_(est.)'):
                state_key = f"airtable_{base_key_name.replace('_(est.)','_est')}"
            else:
                state_key = f"airtable_{base_key_name}" # e.g., "airtable_refed_alignment"

            # 2. Determine the initial value/apply defaulting logic
            if tags: # Tags were successfully classified and validated
                value_to_save = tags
            elif field in default_to_unknown_fields:
                value_to_save = ['Unknown']
                logger.info(f"No valid tags found for '{field}'. Defaulting state key '{state_key}' to ['Unknown'].")
            else:
                value_to_save = [] # e.g., ReFED Alignment defaults to empty list

            # Cap Country/Region at ['Global'] if more than 2 regions are found
            if field == "Country/Region" and len(value_to_save) > 2:
                logger.info(f"Overriding Country/Region tags (found {len(value_to_save)} regions: {value_to_save}) to ['Global'].")
                value_to_save = ['Global']
            
            # 3. Save to state
            state[state_key] = value_to_save 
            if value_to_save:
                airtable_tags[field] = value_to_save
                logger.info(f"Updating state key '{state_key}' with tags: {value_to_save}")
            else:
                logger.info(f"No valid tags for '{field}'. Setting state key '{state_key}' to empty list.")

                
        logger.info(f"Classification complete for {company}: {airtable_tags}")

        # Add results to messages list for logging/display
        if airtable_tags: 
            log_message = f"📊 Classification results for {company}:\n" + "\n".join([f"  • {field}: {', '.join(tags)}" for field, tags in airtable_tags.items()])
            state.setdefault('messages', []).append(AIMessage(content=log_message))
        else:
            logger.info("No classification tags were successfully generated or validated.")
            state.setdefault('messages', []).append(AIMessage(content=f"📊 No classification tags identified for {company}."))

        return state

    async def run(self, state: ResearchState) -> ResearchState:
        """Executes the tagger node."""
        airtable_record_id = state.get('airtable_record_id')
        try:
            # --- Call Airtable Update (Start Status) ---
            if airtable_record_id:
                logger.info(f"Sending 'Classifying' status update to Airtable record: {airtable_record_id}")
                asyncio.create_task(
                    self._update_airtable_status(airtable_record_id, "Classifying")
                )

            state = await self.classify_company(state)
            return state

        except Exception as e:
            logger.error(f"Error in Tagger node run method: {e}", exc_info=True)
            error_msg = f"⚠️ Tagger node failed critically: {str(e)}"
            state.setdefault('messages', []).append(AIMessage(content=error_msg))
            if airtable_record_id:
                logger.info(f"Sending 'Tagger Failed' status update to Airtable record: {airtable_record_id}")
                asyncio.create_task(
                    self._update_airtable_status(airtable_record_id, f"Tagger Failed: {str(e)[:50]}")
                )
            
            # --- v2: Ensure ALL keys exist on failure ---
            state.setdefault('airtable_industries', ['Unknown'])
            state.setdefault('airtable_country_region', ['Unknown'])
            state.setdefault('airtable_revenue_band_est', ['Unknown'])
            state.setdefault('airtable_refed_alignment', [])
            return state

    async def _update_airtable_status(self, record_id: str, status_text: str):
        """Helper to call the synchronous update function in a separate thread."""
        if not record_id:
            logger.warning("Airtable status update skipped: No record ID provided.")
            return
        try:
            await asyncio.to_thread(update_airtable_record, record_id, {'Research Status': status_text})
            logger.debug(f"Airtable status update successful for record {record_id}")
        except Exception as e:
            logger.error(f"{self.__class__.__name__} failed to update Airtable status for record {record_id}: {e}", exc_info=True)