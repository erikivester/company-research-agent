# backend/nodes/editor.py
import logging
import os
import asyncio
from typing import Any, Dict

from langchain_core.messages import AIMessage
from openai import AsyncOpenAI

from ..classes import ResearchState
from ..utils.references import format_references_section
from backend.airtable_uploader import update_airtable_record

logger = logging.getLogger(__name__)


class Editor:
    """Compiles individual section briefings into a cohesive final report."""

    def __init__(self) -> None:
        self.openai_key = os.getenv("OPENAI_API_KEY")
        if not self.openai_key:
            raise ValueError("OPENAI_API_KEY environment variable is not set")

        self.openai_client = AsyncOpenAI(api_key=self.openai_key)
        self.context = {
            "company": "Unknown Company",
            "industry": "Unknown",
            "hq_location": "Unknown"
        }

    async def compile_briefings(self, state: ResearchState) -> ResearchState:
        """Compile individual briefing categories from state into a final report."""
        company = state.get('company', 'Unknown Company')
        airtable_record_id = state.get('airtable_record_id')

        if airtable_record_id:
            asyncio.create_task(
                self._update_airtable_status(airtable_record_id, "Compiling Report")
            )

        self.context = {
            "company": company,
            "industry": state.get('industry', 'Unknown'),
            "hq_location": state.get('hq_location', 'Unknown')
        }
        websocket_manager = state.get('websocket_manager')
        job_id = state.get('job_id')

        if websocket_manager and job_id:
            await websocket_manager.send_status_update(
                job_id=job_id,
                status="processing",
                message=f"Starting report compilation for {company}",
                result={
                    "step": "Editor",
                    "substep": "initialization"
                }
            )

        msg = [f"📑 Compiling final report for {company}..."]

        # --- UPDATED: Include FLW briefing key ---
        briefing_keys = {
            'company': 'company_briefing',
            'industry': 'industry_briefing',
            'financial': 'financial_briefing',
            'news': 'news_briefing',
            'flw': 'flw_sustainability_briefing' # <-- ADDED FLW key
        }
        # --- END UPDATE ---

        if websocket_manager and job_id:
            await websocket_manager.send_status_update(
                job_id=job_id,
                status="processing",
                message="Collecting section briefings",
                result={
                    "step": "Editor",
                    "substep": "collecting_briefings"
                }
            )

        individual_briefings = {}
        for category, key in briefing_keys.items():
            content = state.get(key)
            if isinstance(content, str) and content.strip():
                # --- MODIFIED: Use category name from briefing_keys for logging ---
                log_category_name = category.replace('flw', 'FLW/Sustainability').capitalize() # Make it user-friendly
                individual_briefings[category] = content
                msg.append(f"✓ Found {log_category_name} briefing ({len(content)} chars)")
                # --- END MODIFICATION ---
            else:
                 log_category_name = category.replace('flw', 'FLW/Sustainability').capitalize()
                 if content == "":
                      msg.append(f"○ Empty {log_category_name} briefing found")
                      logger.warning(f"Briefing content for {key} is empty.")
                 else:
                      msg.append(f"✗ No {log_category_name} briefing available")
                      logger.warning(f"Missing or invalid state key: {key}")


        compiled_report = ""
        if not individual_briefings:
            msg.append("\n⚠️ No briefing sections available to compile")
            logger.error("No briefings found in state to compile.")
            if airtable_record_id:
                 asyncio.create_task(self._update_airtable_status(airtable_record_id, "Compilation Failed - No Briefings"))
            # Ensure report key exists even if empty
            state['report'] = ""
        else:
            try:
                compiled_report = await self.edit_report(state, individual_briefings, self.context)
                if not compiled_report or not compiled_report.strip():
                    logger.error("Compiled report is empty after editing!")
                    if airtable_record_id:
                         asyncio.create_task(self._update_airtable_status(airtable_record_id, "Compilation Failed - Empty Report"))
                    # Keep existing report content if edit failed, or set empty
                    compiled_report = state.get('report', "")
                else:
                    logger.info(f"Successfully compiled and edited report (Length: {len(compiled_report)})")
                    state['report'] = compiled_report # Update state with the final edited report

            except Exception as e:
                logger.error(f"Error during report compilation/editing: {e}", exc_info=True)
                msg.append(f"\n⚠️ Error during report compilation/editing: {e}")
                if airtable_record_id:
                     asyncio.create_task(self._update_airtable_status(airtable_record_id, f"Compilation Failed: {str(e)[:50]}"))
                # Keep existing report content if edit failed, or set empty
                compiled_report = state.get('report', "")


        # Append messages and ensure report key exists
        state.setdefault('messages', []).append(AIMessage(content="\n".join(msg)))
        if 'report' not in state:
             state['report'] = compiled_report if compiled_report else ""

        return state

    async def edit_report(self, state: ResearchState, briefings: Dict[str, str], context: Dict[str, Any]) -> str:
        """Compile section briefings into a final report using LLMs and update the state."""
        websocket_manager = state.get('websocket_manager')
        job_id = state.get('job_id')
        initial_report = ""
        final_report = ""
        company = self.context["company"] # Use context set in compile_briefings

        try:
            # Step 1: Initial Compilation
            if websocket_manager and job_id:
                await websocket_manager.send_status_update(
                    job_id=job_id, status="processing", message="Compiling initial research report",
                    result={"step": "Editor", "substep": "compilation"}
                )

            initial_report = await self.compile_content(state, briefings, company)
            if not initial_report or not initial_report.strip():
                logger.error("Initial compilation failed or returned empty. Attempting fallback concatenation.")
                # Fallback: Simple concatenation if LLM fails
                # --- UPDATED FALLBACK: Include FLW ---
                report_order = ['company', 'industry', 'financial', 'flw', 'news'] # Define order
                fallback_parts = [ f"## {cat.replace('flw', 'FLW and Sustainability').capitalize()} Overview\n{briefings[cat]}"
                                   for cat in report_order if cat in briefings and briefings[cat].strip()]
                initial_report = f"# {company} Research Report\n\n" + "\n\n".join(fallback_parts)
                # --- END UPDATE ---
                # Append references manually in fallback
                references = state.get('references', [])
                if references:
                     ref_info = state.get('reference_info', {})
                     ref_titles = state.get('reference_titles', {})
                     try:
                          ref_text = format_references_section(references, ref_info, ref_titles)
                          initial_report += f"\n\n{ref_text}" # Append directly
                     except Exception as ref_fmt_exc:
                          logger.error(f"Error formatting references during fallback: {ref_fmt_exc}")
                          initial_report += "\n\n## References\n[Error formatting references]"

                if not initial_report.strip():
                     raise ValueError("Fallback concatenation also resulted in empty report.")
                logger.info("Using fallback concatenation for initial report.")


            # Step 2 & 3: Cleanup and Formatting (Content Sweep)
            if websocket_manager and job_id:
                await websocket_manager.send_status_update(
                    job_id=job_id, status="processing", message="Cleaning up and formatting report",
                    result={"step": "Editor", "substep": "formatting"}
                )

            final_report = await self.content_sweep(state, initial_report, company)

            if not final_report or not final_report.strip():
                logger.warning("Content sweep returned empty string. Using initial compiled report.")
                final_report = initial_report # Use output from step 1 if sweep fails
            else:
                 logger.info(f"Content sweep formatting successful (Length: {len(final_report)})")

            logger.info(f"Final report generated (Length: {len(final_report)})")
            if not final_report.strip():
                logger.error("Final report is unexpectedly empty!")
                return ""

            # Update state with the final report (this is the definitive update)
            state['report'] = final_report
            # state['status'] = "editor_complete" # status key seems unused elsewhere, stick to state['report']

            if websocket_manager and job_id:
                await websocket_manager.send_status_update(
                    job_id=job_id, status="editor_complete", message="Research report completed",
                    result={
                        "step": "Editor", "report_length": len(final_report),
                        "company": company, "status": "completed"
                    }
                )

            return final_report
        except Exception as e:
            logger.error(f"Error in edit_report: {e}", exc_info=True)
            # Use whichever report stage has content as fallback, prefer final if available
            state['report'] = final_report if final_report else initial_report if initial_report else ""
            # state['status'] = "editor_failed" # Status key seems unused
            # Optionally send WS error status here too
            if websocket_manager and job_id:
                await websocket_manager.send_status_update(
                    job_id=job_id, status="editor_failed", message=f"Report editing failed: {e}",
                    result={"step": "Editor", "error": str(e)}
                )
            return state['report'] # Return whatever report content exists

    async def compile_content(self, state: ResearchState, briefings: Dict[str, str], company: str) -> str:
        """Initial compilation of research sections using an LLM."""

        # --- UPDATED: Include FLW briefing ---
        # Define order for combining briefings
        report_order = ['company', 'industry', 'financial', 'flw', 'news']
        combined_content_parts = []
        for cat in report_order:
             if cat in briefings and isinstance(briefings[cat], str) and briefings[cat].strip():
                  # Create a user-friendly header for the combined content
                  header = cat.replace('flw', 'FLW and Sustainability').capitalize()
                  combined_content_parts.append(f"## {header} Briefing\n{briefings[cat]}")

        combined_content = "\n\n---\n\n".join(combined_content_parts)
        # --- END UPDATE ---

        if not combined_content:
             logger.error("No valid briefings content to provide to LLM for initial compilation.")
             return ""

        # --- Reference Formatting (remains the same) ---
        references = state.get('references', [])
        reference_text_to_append = ""
        if references:
            logger.info(f"Found {len(references)} references to add during compilation")
            reference_info = state.get('reference_info', {})
            reference_titles = state.get('reference_titles', {})
            try:
                reference_text_to_append = format_references_section(references, reference_info, reference_titles)
                logger.info("References section formatted successfully for appending.")
            except Exception as ref_fmt_exc:
                 logger.error(f"Error formatting references section: {ref_fmt_exc}", exc_info=True)
                 reference_text_to_append = "\n\n## References\n[Error formatting references]"
        # --- End Reference Formatting ---

        # Use context set earlier
        industry = self.context["industry"]
        hq_location = self.context["hq_location"]

        # --- UPDATED PROMPT: Added FLW Section ---
        prompt = f"""You are compiling a comprehensive research report about "{company}".
Company Context: Industry='{industry}', HQ='{hq_location}'

Provided Briefings:
{combined_content}

Task: Create a cohesive research report about "{company}" based *only* on the provided briefings.

Formatting Rules:
1. Start the report *exactly* with: # {company} Research Report
2. Use the following EXACT ## headers in this EXACT order:
   - ## Company Overview
   - ## Industry Overview
   - ## Financial Overview
   - ## FLW and Sustainability  # <-- ADDED FLW Header
   - ## News
3. Integrate information from the corresponding briefings under the correct ## header.
4. Use ### headers for logical subsections within Company, Industry, Financial, and FLW overviews *if appropriate based on briefing content*. DO NOT invent subsections.
5. The News section should ONLY contain bullet points (*), NEVER use ### headers within News.
6. Ensure the narrative flows logically and avoids repetition.
7. Remove introductory/transitional phrases from the original briefings (e.g., "This briefing covers...", "Key findings include...").
8. Do NOT add a "References" section; it will be appended later.
9. Output only the clean markdown report content. No explanations, commentary, apologies, or preamble.
"""
        # --- END PROMPT UPDATE ---

        try:
            logger.info("Sending initial compilation request to OpenAI (gpt-4o-mini)")
            response = await self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are an expert report editor. Follow formatting rules precisely."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=4000,
                stream=False # Keep stream False for initial compile
            )
            initial_report = response.choices[0].message.content.strip() if response.choices else ""
            if not initial_report:
                 logger.error("OpenAI initial compilation returned empty content.")
                 return "" # Return empty if LLM response is empty

            logger.info(f"OpenAI initial compilation successful (Length: {len(initial_report)})")

            # Append the references section AFTER LLM processing
            if reference_text_to_append:
                # Ensure two newlines before references if report isn't empty
                initial_report = f"{initial_report}\n\n{reference_text_to_append}"
                logger.info("Appended formatted references section.")

            return initial_report
        except Exception as e:
            logger.error(f"Error in initial compilation LLM call: {e}", exc_info=True)
            return "" # Return empty on LLM error

    async def content_sweep(self, state: ResearchState, content: str, company: str) -> str:
        """Sweep the content for formatting, redundancy, and adherence to rules using an LLM."""
        if not content or not content.strip():
             logger.warning("Content sweep called with empty input content.")
             return ""

        # Use context set earlier
        industry = self.context["industry"]
        hq_location = self.context["hq_location"]
        websocket_manager = state.get('websocket_manager')
        job_id = state.get('job_id')

        # --- UPDATED PROMPT: Added FLW Section to rules ---
        prompt = f"""You are an expert markdown formatter specializing in research reports. You are given a draft report about "{company}" ({industry}, HQ: {hq_location}).

Draft Report:
--- START REPORT ---
{content}
--- END REPORT ---

Task: Polish the draft report into a final, clean markdown document adhering strictly to the following rules:

Critical Formatting Rules:
1. The document MUST start *exactly* with: # {company} Research Report
2. The document MUST ONLY use these EXACT ## headers in this EXACT order:
   - ## Company Overview
   - ## Industry Overview
   - ## Financial Overview
   - ## FLW and Sustainability # <-- ADDED FLW Header
   - ## News
   - ## References
3. NO OTHER ## HEADERS ARE ALLOWED. Ensure all content falls under one of the allowed ## headers. If draft content appears misplaced, move it under the most appropriate allowed header.
4. Use ### headers for subsections ONLY within "Company Overview", "Industry Overview", "Financial Overview", and "FLW and Sustainability". # <-- ADDED FLW to ### rule
5. Do NOT use ### headers in "News" or "References".
6. The "News" section MUST use ONLY bullet points (*). Remove any ### headers found within the News section content.
7. The "References" section MUST be preserved *exactly* as provided in the draft, including the "* Website. \"Title.\" URL" format. DO NOT CHANGE THE REFERENCE FORMATTING OR CONTENT. Ensure it starts immediately after the `## References` header.
8. Remove redundant or repetitive information *within* each section. DO NOT remove entire sections, even if content seems sparse, unless the section header itself is invalid (not one of the 6 allowed).
9. Remove any meta-commentary, conversational text, or explanations (e.g., "Here is the news...", "This section covers...", "Based on the data...").
10. Ensure exactly ONE blank line before each `##` header (except the first `#` title).
11. Ensure exactly ONE blank line after each `##` header and before its content (or bullets).
12. Ensure exactly ONE blank line between the end of one section's content and the start of the next `##` header.
13. Format ALL bullet points consistently using an asterisk followed by a space (* ). Ensure one blank line before the start and after the end of bulleted lists, unless the list is the only content under a header.
14. Remove any markdown code blocks (```).
15. Fix minor grammatical errors or awkward phrasing for clarity, while preserving the original meaning and data.

Output ONLY the final, polished markdown report. No explanations, commentary, or preamble.
"""
        # --- END PROMPT UPDATE ---

        try:
            logger.info("Sending content sweep request to OpenAI (gpt-4o-mini)")
            response_stream = await self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are an expert markdown formatter. Follow all rules precisely."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.0,
                max_tokens=4000, # Increased slightly, adjust if needed
                stream=True # Stream the final formatting
            )

            accumulated_text = ""
            buffer = ""
            async for chunk in response_stream:
                finish_reason = chunk.choices[0].finish_reason
                chunk_text = chunk.choices[0].delta.content

                if chunk_text:
                    accumulated_text += chunk_text
                    buffer += chunk_text
                    # Send chunks via WebSocket
                    if len(buffer) > 30 or '\n' in buffer: # Send frequent small updates
                        if websocket_manager and job_id:
                             await websocket_manager.send_status_update(
                                 job_id=job_id, status="report_chunk",
                                 message="Formatting final report content",
                                 result={"chunk": buffer, "step": "Editor", "substep": "streaming_format"}
                             )
                        buffer = ""

                if finish_reason: # Handle stop or other reasons
                     # Send any remaining buffer content
                     if websocket_manager and job_id and buffer:
                          await websocket_manager.send_status_update(
                              job_id=job_id, status="report_chunk", message="Final report content chunk",
                              result={"chunk": buffer, "step": "Editor", "substep": "streaming_format_final"}
                          )
                     if finish_reason == "stop":
                         logger.info("OpenAI content sweep stream finished normally.")
                     else:
                         logger.warning(f"OpenAI content sweep stream finished unexpectedly. Reason: {finish_reason}")
                         if websocket_manager and job_id:
                               await websocket_manager.send_status_update(
                                   job_id=job_id, status="editor_warning",
                                   message=f"Report formatting may be incomplete. Reason: {finish_reason}",
                                   result={"step": "Editor", "finish_reason": finish_reason}
                               )
                     break # Exit loop

            final_swept_content = accumulated_text.strip()
            if not final_swept_content:
                 logger.warning("Content sweep resulted in an empty string. Returning original compiled content.")
                 return content.strip() # Fallback to pre-sweep content

            logger.info(f"Content sweep formatting complete (Final length: {len(final_swept_content)})")
            return final_swept_content
        except Exception as e:
            logger.error(f"Error in content sweep LLM call: {e}", exc_info=True)
            return content.strip() # Fallback to pre-sweep content on error

    async def _update_airtable_status(self, record_id: str, status_text: str):
        """Helper to call the synchronous update function."""
        try:
            update_airtable_record(record_id, {'Research Status': status_text})
        except Exception as e:
            logger.error(f"Editor node failed to update Airtable status: {e}")

    async def run(self, state: ResearchState) -> ResearchState:
        airtable_record_id = state.get('airtable_record_id')
        try:
            # compile_briefings updates the state with the final report
            state = await self.compile_briefings(state)
            return state
        except Exception as e:
            logger.error(f"Error in Editor run method: {e}", exc_info=True)
            error_msg = f"⚠️ Editor node failed critically: {str(e)}"
            state.setdefault('messages', []).append(AIMessage(content=error_msg))
            if airtable_record_id:
                asyncio.create_task(
                    self._update_airtable_status(airtable_record_id, f"Editor Failed: {str(e)[:50]}")
                )
            # Ensure report key exists even on failure
            state.setdefault('report', "[Report generation failed in Editor node]")
            return state