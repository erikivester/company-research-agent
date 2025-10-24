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
    """Creates briefings for each research category and updates the ResearchState."""

    def __init__(self) -> None:
        self.max_doc_length = 8000  # Maximum document content length per doc
        self.max_total_length = 120000 # Max total characters to send to Gemini
        self.gemini_key = os.getenv("GEMINI_API_KEY")
        if not self.gemini_key:
            raise ValueError("GEMINI_API_KEY environment variable is not set")

        # Configure Gemini
        genai.configure(api_key=self.gemini_key)
        # Consider using a model with a larger context window if needed, like gemini-1.5-flash
        self.gemini_model = genai.GenerativeModel('gemini-2.5-flash') # Updated model
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
                    "category": category,
                    "total_docs": num_docs
                }
            )

        # Define prompts for each category
        prompts = {
            'company': f"""Create a focused company briefing for {company}, a {industry} company based in {hq_location}.
Key requirements:
1. Start with: "{company} is a [what] that [does what] for [whom]" (Fill in the brackets based *only* on provided documents).
2. Structure using these exact headers and bullet points. Use only bullet points under headers.
### Core Product/Service
* List distinct products/features mentioned.
* Include only verified capabilities from the documents.
### Leadership Team
* List key leadership team members mentioned.
* Include their roles if specified.
### Target Market
* List specific target audiences or customer segments mentioned.
* List verified use cases mentioned.
* List confirmed customers/partners mentioned.
### Key Differentiators
* List unique features or advantages mentioned.
### Business Model
* Discuss product/service pricing if mentioned.
* List distribution channels if mentioned.
3. Each bullet must be a single, concise, complete fact derived *only* from the documents.
4. If information for a header/bullet is not found in the documents, OMIT that header/bullet entirely.
5. NEVER state "no information found" or "data not available".
6. Provide only the briefing content in markdown format. No explanations or commentary.
""",
            'industry': f"""Create a focused industry briefing for {company}, a {industry} company.
Key requirements:
1. Structure using these exact headers and bullet points. Use only bullet points under headers.
### Market Overview
* State {company}'s market segment if mentioned.
* List market size with year if provided.
* List growth rate with year range if provided.
### Direct Competition
* List named direct competitors mentioned.
* List specific competing products mentioned.
### Competitive Advantages
* List unique technical features or advantages mentioned for {company} relative to the industry.
### Market Challenges
* List specific verified challenges mentioned for the industry.
2. Each bullet must be a single, concise fact derived *only* from the documents.
3. If information for a header/bullet is not found in the documents, OMIT that header/bullet entirely.
4. NEVER state "no information found" or "data not available".
5. Provide only the briefing content in markdown format. No explanation or commentary.
""",
            'financial': f"""Create a focused financial briefing for {company}.
Key requirements:
1. Structure using these headers and bullet points. Use only bullet points under headers.
### Funding & Investment
* Total funding amount with date if mentioned.
* List each funding round with amount and date if mentioned.
* List named investors if mentioned.
### Revenue Model
* Discuss product/service pricing if applicable and mentioned.
2. Include specific numbers *only* if found in the documents.
3. Each bullet must be a single, concise fact derived *only* from the documents.
4. If information for a header/bullet is not found in the documents, OMIT that header/bullet entirely.
5. NEVER state "no information found" or "data not available".
6. Do not repeat the same funding round. Assume rounds in the same month are the same unless explicitly different.
7. Do not include ranges of funding amounts; use specific figures found or omit.
8. Provide only the briefing content in markdown format. No explanation or commentary.
""",
            'news': f"""Create a focused news briefing for {company}.
Key requirements:
1. Structure using these categories as bullet points. DO NOT use ### headers.
* **Major Announcements**: Product/service launches, new initiatives mentioned.
* **Partnerships**: Integrations, collaborations mentioned.
* **Recognition**: Awards, significant press coverage mentioned.
2. Sort items newest to oldest *if dates are available*, otherwise list as found.
3. Each bullet point should represent one distinct event or piece of information.
4. Derive information *only* from the provided documents.
5. If no information is found for a category bullet, OMIT that bullet entirely.
6. NEVER state "no information found" or "data not available".
7. Provide only the briefing content as a single bulleted list. No explanation or commentary.
""",
            # --- ADDED: FLW/Sustainability Prompt ---
'flw': f"""Create a focused briefing on {company}'s Food Loss & Waste (FLW) and Sustainability efforts. 
Structure the data to highlight actionable, verifiable metrics relevant to ReFED's Solutions Framework.

Key Requirements:
1. Structure using these exact headers. Use only concise bullet points under headers.
### Prevention Initiatives (e.g., Forecasting, Shelf-life Extension)
* Specific programs, tools (like AI/Sensors), or techniques mentioned.
* **Quantify Impact:** Include measurable figures like '% reduction goal', 'X tons diverted', or 'Y dollars invested' if explicitly stated.
### Food Rescue & Donation Programs
* Key partners (e.g., Feeding America, food banks) and program scope.
* **Quantify Impact:** State achieved metrics like 'X pounds/tons donated' or 'Y meals recovered' if available.
### Recycling & Resource Recovery (e.g., Composting, Anaerobic Digestion)
* Mention of off-site partners or on-site processing methods (AD/Composting).
* **Quantify Capacity:** Include volume handled (tons/year) or diversion rate (%) if available.
### Corporate Commitments & Targets
* Stated climate goals (especially methane reduction targets or SBTi).
* Official disclosure reports cited (e.g., year and type of ESG/Sustainability report, 10-K).

2. Each bullet must be a single, concise, verifiable fact derived *only* from the documents.
3. If information for a specific header is not found in the documents, OMIT that header entirely.
4. Output only the briefing content in markdown format. No explanation or commentary.
"""
            # --- END ADDED PROMPT ---
        }

        # Select the appropriate prompt, default to a generic one if category unknown
        prompt_template = prompts.get(category, f'Create a focused research briefing on {category} for {company} based on the provided documents.')

        # Sort documents by evaluation score (highest first)
        try:
             # Safer sorting with explicit float conversion and default
             sorted_items = sorted(
                 items,
                 # Ensure evaluation and overall_score exist, default score to 0
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
             # Ensure doc is a dictionary before proceeding
             if not isinstance(doc, dict):
                  logger.warning(f"Skipping non-dictionary item during doc text preparation for {category}.")
                  continue

             title = doc.get('title', '')
             # Use enriched raw_content if available, otherwise fallback to potentially shorter 'content'
             content = doc.get('raw_content') or doc.get('content', '')

             # Ensure content is string and truncate if necessary
             if not isinstance(content, str):
                  content = str(content) # Attempt conversion
             if len(content) > self.max_doc_length:
                  content = content[:self.max_doc_length] + "... [content truncated]"

             # Add source URL to context for the LLM
             doc_url = doc.get('url', 'Unknown Source')
             doc_entry = f"Source URL: {doc_url}\nTitle: {title}\n\nContent: {content}"

             entry_len = len(doc_entry) + len(separator)
             if total_length + entry_len < self.max_total_length:
                 doc_texts.append(doc_entry)
                 total_length += entry_len
             else:
                 logger.warning(f"Reached max total length ({self.max_total_length} chars). Truncating documents for {category} briefing.")
                 break # Stop adding docs if limit reached

        if not doc_texts:
             logger.warning(f"No document content available to generate briefing for {category}.")
             # Send completion status (empty)
             if websocket_manager and job_id:
                  await websocket_manager.send_status_update(
                      job_id=job_id, status="briefing_complete",
                      message=f"No content for {category} briefing",
                      result={ "step": "Briefing", "category": category, "success": False }
                  )
             return {'content': ''} # Return empty if no docs processed

        # Construct the final prompt
        full_prompt = f"""{prompt_template}

Analyze the following documents and extract key information according to the requirements. Provide only the briefing, no explanations or commentary:

{separator.join(doc_texts)}
"""
        logger.debug(f"Prompt length for {category}: {len(full_prompt)} characters.")

        try:
            logger.info(f"Sending prompt to Gemini for {category} briefing ({len(doc_texts)} docs).")
            # Increased timeout and added safety settings
            generation_config = genai.types.GenerationConfig(
                 temperature=0.1, # Low temp for factual summary
                 max_output_tokens=8192 # Max output for Flash 1.5
            )
            safety_settings = { # Adjust as needed, be less restrictive for factual extraction
                 'HARM_CATEGORY_HATE_SPEECH': 'BLOCK_NONE',
                 'HARM_CATEGORY_HARASSMENT': 'BLOCK_NONE',
                 'HARM_CATEGORY_SEXUALLY_EXPLICIT': 'BLOCK_NONE',
                 'HARM_CATEGORY_DANGEROUS_CONTENT': 'BLOCK_NONE',
            }
            response = await self.gemini_model.generate_content_async( # Use async version
                 full_prompt,
                 generation_config=generation_config,
                 safety_settings=safety_settings,
                 request_options={'timeout': 300} # 5 min timeout
            )

            # Safer access to response text
            content = ""
            if response and response.parts:
                 content = "".join(part.text for part in response.parts if hasattr(part, 'text')).strip()
            # Log potential finish reasons if content is empty
            if not content:
                 finish_reason = response.prompt_feedback.block_reason if response.prompt_feedback else "Unknown"
                 logger.error(f"Empty response from LLM for {category} briefing. Finish Reason: {finish_reason}")
                 # Send completion status (failed)
                 if websocket_manager and job_id:
                      await websocket_manager.send_status_update(
                          job_id=job_id, status="briefing_complete",
                          message=f"LLM failed for {category} briefing",
                          result={ "step": "Briefing", "category": category, "success": False, "error": f"LLM Error: {finish_reason}" }
                      )
                 return {'content': ''} # Return empty on failure


            logger.info(f"Successfully generated {category} briefing (Length: {len(content)} characters)")
            # Send completion status (success)
            if websocket_manager and job_id:
                await websocket_manager.send_status_update(
                    job_id=job_id,
                    status="briefing_complete",
                    message=f"Completed {category} briefing",
                    result={
                        "step": "Briefing",
                        "category": category,
                        "success": True # Indicate success
                    }
                )

            return {'content': content}
        except Exception as e:
            logger.error(f"Error generating {category} briefing via LLM: {e}", exc_info=True)
            # Send completion status (failed)
            if websocket_manager and job_id:
                 await websocket_manager.send_status_update(
                     job_id=job_id, status="briefing_complete",
                     message=f"Error generating {category} briefing",
                     result={ "step": "Briefing", "category": category, "success": False, "error": str(e) }
                 )
            return {'content': ''} # Return empty on exception

    async def create_briefings(self, state: ResearchState) -> ResearchState:
        """Create briefings for all categories in parallel."""
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

        # Mapping of curated data fields to briefing categories and state keys
        # --- UPDATED: Added FLW mapping ---
        categories = {
            'curated_financial_data': ("financial", "financial_briefing"),
            'curated_news_data': ("news", "news_briefing"),
            'curated_industry_data': ("industry", "industry_briefing"),
            'curated_company_data': ("company", "company_briefing"),
            'curated_flw_data': ("flw", "flw_sustainability_briefing") # <-- ADDED FLW
        }
        # --- END UPDATE ---

        briefings = {} # Dictionary to store generated briefing content
        briefing_tasks_details = [] # List to hold details for task creation

        # Prepare tasks for parallel processing
        for curated_key, (cat, briefing_key) in categories.items():
            curated_data = state.get(curated_key, {})

            if curated_data and isinstance(curated_data, dict): # Check data exists and is dict
                logger.info(f"Preparing briefing task for {cat} using {len(curated_data)} documents from {curated_key}")
                # Add details needed for the task
                briefing_tasks_details.append({
                    'category': cat, # e.g., 'flw'
                    'briefing_key': briefing_key, # e.g., 'flw_sustainability_briefing'
                    'curated_data': curated_data,
                    'data_field': curated_key # Log which data field was used
                })
            else:
                logger.info(f"No data available or invalid format for {curated_key}, skipping {cat} briefing.")
                # Ensure the briefing key exists in the state, even if empty
                state[briefing_key] = ""

        # Process briefings in parallel if tasks were prepared
        if briefing_tasks_details:
            # Rate limiting semaphore for LLM API (limit concurrent calls)
            briefing_semaphore = asyncio.Semaphore(3) # Limit to 3 concurrent Gemini calls

            async def process_briefing(task_details: Dict[str, Any]) -> Dict[str, Any]:
                """Process a single briefing with rate limiting."""
                async with briefing_semaphore:
                    # Call the async generate function
                    result = await self.generate_category_briefing(
                        task_details['curated_data'],
                        task_details['category'],
                        context
                    )

                    briefing_content = result.get('content', '')
                    success = bool(briefing_content) # Success if content is not empty

                    # Update state and internal briefings dictionary
                    state[task_details['briefing_key']] = briefing_content
                    if success:
                         briefings[task_details['category']] = briefing_content # Store successful content
                         logger.info(f"Completed {task_details['category']} briefing ({len(briefing_content)} chars)")
                    else:
                         logger.error(f"Failed to generate briefing for {task_details['category']} using {task_details['data_field']}")

                    # Return stats for logging
                    return {
                        'category': task_details['category'],
                        'success': success,
                        'length': len(briefing_content)
                    }

            # Create and run tasks concurrently using asyncio.gather
            logger.info(f"Starting execution of {len(briefing_tasks_details)} briefing tasks.")
            results = await asyncio.gather(*[
                process_briefing(task)
                for task in briefing_tasks_details
            ])

            # Log completion statistics
            successful_briefings = sum(1 for r in results if r.get('success'))
            total_length = sum(r.get('length', 0) for r in results)
            logger.info(f"Generated {successful_briefings}/{len(briefing_tasks_details)} briefings successfully. Total characters generated: {total_length}")
        else:
             logger.warning("No briefing tasks were prepared. Skipping parallel processing.")


        # Store the dictionary of generated briefings in the state
        state['briefings'] = briefings
        logger.info("Finished creating all briefings.")
        return state

    async def run(self, state: ResearchState) -> ResearchState:
        """Executes the briefing generation process."""
        airtable_record_id = state.get('airtable_record_id')
        if airtable_record_id:
            asyncio.create_task(
                self._update_airtable_status(airtable_record_id, "Generating Briefings") # <--- ADDED CALL
            )
            
        try:
             return await self.create_briefings(state)
        except Exception as e:
             logger.error(f"Critical error during briefing node execution: {e}", exc_info=True)
             state.setdefault('messages', []).append(AIMessage(content=f"⚠️ Briefing node failed: {str(e)}"))
             # Ensure essential keys exist even on failure
             state.setdefault('briefings', {})
             # Ensure individual briefing keys exist
             briefing_keys_to_ensure = [
                  "financial_briefing", "news_briefing", "industry_briefing",
                  "company_briefing", "flw_sustainability_briefing"
             ]
             for key in briefing_keys_to_ensure:
                  state.setdefault(key, "")
             return state