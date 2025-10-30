# backend/nodes/briefing.py
import asyncio
import logging
import os
from typing import Any, Dict, List, Union

import google.generativeai as genai

# Assuming ResearchState is in ../classes/state.py relative to this file
from ..classes import ResearchState
# Import the Airtable update function
from backend.airtable_uploader import update_airtable_record # synchronous function

logger = logging.getLogger(__name__)

class Briefing:
    """(v2) Creates polished briefings for each of the 5 v2 research categories."""

    def __init__(self) -> None:
        self.max_doc_length = 8000  # Maximum document content length per doc
        self.max_total_length = 120000 # Max total characters to send to Gemini
        self.gemini_key = os.getenv("GEMINI_API_KEY")
        if not self.gemini_key:
            raise ValueError("GEMINI_API_KEY environment variable is not set")

        # Configure Gemini
        genai.configure(api_key=self.gemini_key)
        self.gemini_model = genai.GenerativeModel('gemini-1.5-flash') # Use Flash for speed/context
        logger.info("Briefing node initialized with Gemini model.")
    
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

    async def generate_category_briefing(
        self, docs: Union[Dict[str, Any], List[Dict[str, Any]]],
        category: str, context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generates a briefing for a specific category using curated documents."""
        company = context.get('company', 'Unknown')
        industry = context.get('industry', 'Unknown')
        hq_location = context.get('hq_location', 'Unknown')
        websocket_manager = context.get('websocket_manager')
        job_id = context.get('job_id')

        # Normalize docs to handle both dict and list inputs
        items = list(docs.items()) if isinstance(docs, dict) else [
            (doc.get('url', f'doc_{i}'), doc) for i, doc in enumerate(docs) if isinstance(doc, dict) # Ensure doc is dict
        ]
        num_docs = len(items)
        logger.info(f"Generating {category} briefing for {company} using {num_docs} documents")


        # Send category start status
        if websocket_manager and job_id:
            await websocket_manager.send_status_update(
                job_id=job_id,
                status="briefing_start",
                message=f"Generating {category} briefing",
                result={
                    "step": "Briefing",
                    "category": category, # This will be the v2 category name, e.g., 'contact'
                    "total_docs": num_docs
                }
            )

        # --- v2: Define prompts for 5 new nodes ---
        prompts = {
            'company_brief': f"""Create a focused Company Brief for {company}, a {industry} company based in {hq_location}.
Key requirements:
1. Structure using these exact headers. Use only bullet points under headers.
### Core Business
* Conc cisely summarize {company}'s primary products, services, and mission.
### Financial Health
* List any "ballpark" revenue figures (e.g., "$100M-$500M", "Est. $1B+").
* List any "financial health signals" found (e.g., "Recent layoffs reported", "Stock price drop", "Secured new funding").
2. Each bullet must be a single, concise, complete fact derived *only* from the documents.
3. If information for a header is not found in the documents, OMIT that header entirely.
4. NEVER state "no information found" or "data not available".
5. Provide only the briefing content in markdown format. No explanations or commentary.
""",
            'news_signal': f"""Create a "News & Signals" briefing for {company}.
Key requirements:
1. Structure using ONLY bullet points (*). DO NOT use ### headers.
2. Scan documents for specific, actionable signals from the last 12-18 months.
3. Format bullets to tag the signal type:
   * **FLW/Climate Signal:** [Detail of ESG report, methane goal, food waste initiative, etc.]
   * **Opportunity Signal:** [Detail of new VP of Impact, new relevant initiative, etc.]
   * **Risk Signal:** [Detail of layoff, boycott, stock issue, etc.]
   * **General News:** [Detail of product launch, partnership, etc.]
4. Sort items newest to oldest *if dates are available*, otherwise list as found.
5. If no information is found for a category bullet, OMIT that bullet entirely.
6. Provide only the briefing content as a single bulleted list. No explanation or commentary.
""",
            'flw': f"""Create a focused briefing on {company}'s Food Loss & Waste (FLW) and Sustainability efforts.
Key Requirements:
1. Structure using these exact headers ONLY IF relevant information is found. Use only bullet points under headers.
### ESG & Methane Goals
* Stated climate goals mentioned (especially methane reduction, SBTi).
* Mention of sustainability or ESG reports (e.g., '2024 ESG Report').
### FLW Initiatives
* Specific actions mentioned for preventing food waste (e.g., forecasting, shelf-life extension).
* Information on food waste recycling (e.g., composting, anaerobic digestion).
### Food Rescue & Donation
* Details on food rescue or donation programs mentioned (e.g., partners, volumes).
### Sustainable Packaging
* Details on packaging materials (e.g., recycled content, compostable).
* Mention of packaging optimization or reduction initiatives.
2. Each bullet must be a single, concise, verifiable fact derived *only* from the provided documents. Include dates if mentioned.
3. If information for a specific header is not found, OMIT that header entirely.
4. NEVER state "no information found".
5. Provide only the briefing content in markdown format. No explanations or commentary.
""",
            'contact': f"""Create a "Potential Contacts" briefing for {company}.
Key Requirements:
1. Structure using the exact header: ### Key Contacts
2. List relevant mid-level contacts (e.g., in Sustainability, Impact, CSR, Community Relations) found in the documents.
3. Format as: `* **[Name]:** [Title] - [Brief 1-2 sentence summary of their role or relevance from the text].`
4. Do NOT include C-suite (CEO, COO, CFO) unless their role is *directly* tied to sustainability or impact.
5. If no relevant contacts are found, OMIT the header and output nothing.
6. Provide only the briefing content in markdown format. No explanations or commentary.
""",
            'engagement': f"""Create an "Engagements & Affiliations" briefing for {company}.
Key Requirements:
1. Structure using the exact header: ### Engagements & Affiliations
2. List all signals of external engagement, partnerships, and memberships.
3. Format as: `* **[Category]:** [Specific detail found in text]`
   * Examples:
     * **Membership:** 1% for the Planet
     * **Event:** Spoke at ReFED Food Waste Solutions Summit 2024
     * **Award:** Named one of Fast Company's Most Innovative 2025
     * **Partnership:** Partnered with World Wildlife Fund on regenerative agriculture
     * **Coalition:** Signatory of the US Food Waste Pact
4. If no signals are found, OMIT the header and output nothing.
5. Provide only the briefing content in markdown format. No explanations or commentary.
"""
        }
        # --- END v2 PROMPTS ---

        # Select the appropriate prompt, default to a generic one if category unknown
        prompt_template = prompts.get(category, f'Create a focused research briefing on {category} for {company} based on the provided documents.')

        # Sort documents by evaluation score (highest first)
        try:
             sorted_items = sorted(
                 items,
                 key=lambda x: float(x[1].get('evaluation', {}).get('overall_score', 0)) if isinstance(x[1], dict) else 0,
                 reverse=True
             )
        except Exception as sort_exc:
             logger.error(f"Error sorting documents for {category}: {sort_exc}. Proceeding with unsorted docs.")
             sorted_items = items # Fallback to unsorted

        # Prepare document text, limiting length
        doc_texts = []
        total_length = 0
        separator = "\n" + "-" * 40 + "\n"
        for _, doc in sorted_items:
             if not isinstance(doc, dict):
                  logger.warning(f"Skipping non-dictionary item during doc text preparation for {category}.")
                  continue

             title = doc.get('title', '')
             content = doc.get('raw_content') or doc.get('content', '')

             if not isinstance(content, str):
                  content = str(content) 
             if len(content) > self.max_doc_length:
                  content = content[:self.max_doc_length] + "... [content truncated]"

             doc_url = doc.get('url', 'Unknown Source')
             doc_entry = f"Source URL: {doc_url}\nTitle: {title}\n\nContent: {content}"

             entry_len = len(doc_entry) + len(separator)
             if total_length + entry_len < self.max_total_length:
                 doc_texts.append(doc_entry)
                 total_length += entry_len
             else:
                 logger.warning(f"Reached max total length ({self.max_total_length} chars). Truncating documents for {category} briefing.")
                 break 

        if not doc_texts:
             logger.warning(f"No document content available to generate briefing for {category}.")
             if websocket_manager and job_id:
                  await websocket_manager.send_status_update(
                      job_id=job_id, status="briefing_complete",
                      message=f"No content for {category} briefing",
                      result={ "step": "Briefing", "category": category, "success": False }
                  )
             return {'content': ''}

        # --- v2: Add Polishing Instructions to main prompt ---
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
        # --- End v2 Polishing ---
        
        logger.debug(f"Prompt length for {category}: {len(full_prompt)} characters.")

        try:
            logger.info(f"Sending prompt to Gemini for {category} briefing ({len(doc_texts)} docs).")
            generation_config = genai.types.GenerationConfig(
                 temperature=0.1, 
                 max_output_tokens=8192
            )
            safety_settings = { 
                 'HARM_CATEGORY_HATE_SPEECH': 'BLOCK_NONE',
                 'HARM_CATEGORY_HARASSMENT': 'BLOCK_NONE',
                 'HARM_CATEGORY_SEXUALLY_EXPLICIT': 'BLOCK_NONE',
                 'HARM_CATEGORY_DANGEROUS_CONTENT': 'BLOCK_NONE',
            }
            response = await self.gemini_model.generate_content_async( 
                 full_prompt,
                 generation_config=generation_config,
                 safety_settings=safety_settings,
                 request_options={'timeout': 300} 
            )

            content = ""
            if response and response.parts:
                 content = "".join(part.text for part in response.parts if hasattr(part, 'text')).strip()
            
            if not content:
                 finish_reason = response.prompt_feedback.block_reason if response.prompt_feedback else "Unknown"
                 logger.error(f"Empty response from LLM for {category} briefing. Finish Reason: {finish_reason}")
                 if websocket_manager and job_id:
                      await websocket_manager.send_status_update(
                          job_id=job_id, status="briefing_complete",
                          message=f"LLM failed for {category} briefing",
                          result={ "step": "Briefing", "category": category, "success": False, "error": f"LLM Error: {finish_reason}" }
                      )
                 return {'content': ''}

            logger.info(f"Successfully generated {category} briefing (Length: {len(content)} characters)")
            if websocket_manager and job_id:
                await websocket_manager.send_status_update(
                    job_id=job_id,
                    status="briefing_complete",
                    message=f"Completed {category} briefing",
                    result={
                        "step": "Briefing",
                        "category": category,
                        "success": True 
                    }
                )

            return {'content': content}
        except Exception as e:
            logger.error(f"Error generating {category} briefing via LLM: {e}", exc_info=True)
            if websocket_manager and job_id:
                 await websocket_manager.send_status_update(
                     job_id=job_id, status="briefing_complete",
                     message=f"Error generating {category} briefing",
                     result={ "step": "Briefing", "category": category, "success": False, "error": str(e) }
                 )
            return {'content': ''}

    async def create_briefings(self, state: ResearchState) -> ResearchState:
        """(v2) Create briefings for all 5 v2 categories in parallel."""
        company = state.get('company', 'Unknown Company')
        websocket_manager = state.get('websocket_manager')
        job_id = state.get('job_id')

        if websocket_manager and job_id:
            await websocket_manager.send_status_update(
                job_id=job_id,
                status="processing",
                message="Starting research briefings",
                result={"step": "Briefing"}
            )

        context = {
            "company": company,
            "industry": state.get('industry', 'Unknown'),
            "hq_location": state.get('hq_location', 'Unknown'),
            "websocket_manager": websocket_manager,
            "job_id": job_id
        }
        logger.info(f"Creating section briefings for {company}")

        # --- v2 MODIFICATION: Updated categories dictionary ---
        # Maps v2 curated data keys -> (v2 prompt category, v2 briefing state key)
        categories = {
            'curated_company_brief_data': ("company_brief", "company_brief_briefing"),
            'curated_news_signal_data': ("news_signal", "news_signal_briefing"),
            'curated_flw_data': ("flw", "flw_sustainability_briefing"),
            'curated_contact_finder_data': ("contact", "contact_briefing"),
            'curated_engagement_finder_data': ("engagement", "engagement_briefing")
        }
        # --- END v2 MODIFICATION ---

        briefings = {} 
        briefing_tasks_details = [] 

        # Prepare tasks for parallel processing
        for curated_key, (cat, briefing_key) in categories.items():
            curated_data = state.get(curated_key, {})

            if curated_data and isinstance(curated_data, dict): 
                logger.info(f"Preparing briefing task for {cat} using {len(curated_data)} documents from {curated_key}")
                briefing_tasks_details.append({
                    'category': cat, # e.g., 'contact'
                    'briefing_key': briefing_key, # e.g., 'contact_briefing'
                    'curated_data': curated_data,
                    'data_field': curated_key 
                })
            else:
                logger.info(f"No data available or invalid format for {curated_key}, skipping {cat} briefing.")
                state[briefing_key] = "" # Ensure the briefing key exists in the state

        # Process briefings in parallel if tasks were prepared
        if briefing_tasks_details:
            briefing_semaphore = asyncio.Semaphore(3) # Limit to 3 concurrent Gemini calls

            async def process_briefing(task_details: Dict[str, Any]) -> Dict[str, Any]:
                """Process a single briefing with rate limiting."""
                async with briefing_semaphore:
                    result = await self.generate_category_briefing(
                        task_details['curated_data'],
                        task_details['category'],
                        context
                    )

                    briefing_content = result.get('content', '')
                    success = bool(briefing_content) 

                    state[task_details['briefing_key']] = briefing_content
                    if success:
                         briefings[task_details['category']] = briefing_content
                         logger.info(f"Completed {task_details['category']} briefing ({len(briefing_content)} chars)")
                    else:
                         logger.error(f"Failed to generate briefing for {task_details['category']} using {task_details['data_field']}")

                    return {
                        'category': task_details['category'],
                        'success': success,
                        'length': len(briefing_content)
                    }

            logger.info(f"Starting execution of {len(briefing_tasks_details)} briefing tasks.")
            results = await asyncio.gather(*[
                process_briefing(task)
                for task in briefing_tasks_details
            ])

            successful_briefings = sum(1 for r in results if r.get('success'))
            total_length = sum(r.get('length', 0) for r in results)
            logger.info(f"Generated {successful_briefings}/{len(briefing_tasks_details)} briefings successfully. Total characters generated: {total_length}")
        else:
             logger.warning("No briefing tasks were prepared. Skipping parallel processing.")


        state['briefings'] = briefings
        logger.info("Finished creating all briefings.")
        return state

    async def run(self, state: ResearchState) -> ResearchState:
        """Executes the briefing generation process."""
        airtable_record_id = state.get('airtable_record_id')
        if airtable_record_id:
            asyncio.create_task(
                self._update_airtable_status(airtable_record_id, "Generating Briefings")
            )
            
        try:
             return await self.create_briefings(state)
        except Exception as e:
             logger.error(f"Critical error during briefing node execution: {e}", exc_info=True)
             state.setdefault('messages', []).append(AIMessage(content=f"⚠️ Briefing node failed: {str(e)}"))
             state.setdefault('briefings', {})
             
             # --- v2 MODIFICATION: Ensure 5 new keys exist on failure ---
             briefing_keys_to_ensure = [
                  "company_brief_briefing", "news_signal_briefing", 
                  "flw_sustainability_briefing", "contact_briefing", "engagement_briefing"
             ]
             # --- END v2 MODIFICATION ---
             
             for key in briefing_keys_to_ensure:
                  state.setdefault(key, "")
             return state