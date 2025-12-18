import logging
# backend/graph.py
from langchain_core.messages import AIMessage  # Used in simple_report_compiler_node
from langchain_core.messages import SystemMessage
from langgraph.graph import StateGraph

from .classes.state import InputState, ResearchState
from .nodes import GroundingNode
from .nodes.briefing import Briefing
from .nodes.collector import Collector
from .nodes.curator import Curator
from .nodes.enricher import Enricher
from .nodes.executive_summary import (
    ExecutiveSummaryNode,
)  # <-- NEW: Import executive summary
from .nodes.query_generator import QueryGeneratorNode  # <-- NEW: Import query generator

# --- v2 Node Imports ---
# Import the 5 new/refocused researcher nodes
from .nodes.researchers.company import (
    CompanyBriefNode,
)  # MODIFIED: Renamed from CompanyAnalyzer
from .nodes.researchers.contact_finder import ContactFinderNode  # NEW: Added node
from .nodes.researchers.engagement_finder import EngagementFinderNode  # NEW: Added node
from .nodes.researchers.flw import FLWAnalyzer  # KEPT: This is our 5th node
from .nodes.researchers.news import NewsSignalNode  # MODIFIED: Renamed from NewsScanner
from .nodes.tagger import Tagger

# --- End v2 Node Imports ---


logger = logging.getLogger(__name__)


# --- UPDATED HELPER FUNCTION TO BYPASS EDITOR ---
async def simple_report_compiler_node(state: ResearchState) -> ResearchState:
    """
    Compiles individual briefings into a raw, unedited markdown report.
    This function now carefully preserves the entire state, ensuring no data is lost.
    """
    logger.info(
        f"DEBUG: PDF path in state at start of compiler: {state.get('executive_summary_pdf_file')}"
    )

    # --- v2: Updated to use the 5 new briefing keys ---
    briefing_keys_map = {
        "company_brief_briefing": "Company Overview & Financial Health",
        "news_signal_briefing": "News & Signals",
        "flw_sustainability_briefing": "FLW & Sustainability",
        "contact_briefing": "Potential Contacts",
        "engagement_briefing": "Engagement & Affiliations",
    }
    report_order = [
        "company_brief_briefing",
        "flw_sustainability_briefing",
        "news_signal_briefing",
        "engagement_briefing",
        "contact_briefing",
    ]
    # --- End v2 Update ---

    report_parts = []
    company = state.get("company", "Research Report")
    report_parts.append(f"# {company} Research Report (Raw)\n")

    for key in report_order:
        content = state.get(key)
        if isinstance(content, str) and content.strip():
            header = briefing_keys_map.get(key, key.replace("_", " ").title())
            report_parts.append(f"## {header}\n{content}\n")

    # Append references section
    references_formatted = state.get("references_formatted", "")
    if references_formatted:
        report_parts.append(references_formatted)

    # --- FIX: Update the state object directly ---
    state["report"] = "\n".join(report_parts)

    # Ensure executive_summary_pdf_file is preserved
    pdf_file_path = state.get("executive_summary_pdf_file")
    if pdf_file_path:
        state["executive_summary_pdf_file"] = pdf_file_path
        logger.info(f"DEBUG: Preserving PDF path in compiler: {pdf_file_path}")
    else:
        logger.warning(
            "simple_report_compiler_node: executive_summary_pdf_file was not found in state."
        )

    # Add a status message for tracking
    messages = state.get("messages", [])
    messages.append(
        AIMessage(
            content=f"🚧 Editor Bypassed. Generated raw report from 5 briefings (Length: {len(state['report'])} chars)."
        )
    )
    state["messages"] = messages

    logger.info(
        f"DEBUG: PDF path in state at end of compiler: {state.get('executive_summary_pdf_file')}"
    )

    return state


# --- END UPDATED HELPER FUNCTION ---


class Graph:
    async def run(self, **kwargs):
        """
        Run the workflow graph, yielding full state snapshots at each node.
        Uses astream(mode="values") to get complete state instead of deltas.
        Accepts keyword arguments to pass as the initial state or config.
        """
        # If thread is passed (as in application.py), use it as the initial state/config
        # Otherwise, use self.input_state as the default
        initial_state = kwargs.get("thread", self.input_state)
        # Use mode="values" to yield full state snapshots instead of deltas/updates
        # This ensures all keys from previous nodes are preserved
        async for s in self.app.astream(initial_state, mode="values"):
            yield s
    def _build_workflow(self):
        """Configure the state graph workflow (v2)"""
        self.workflow = StateGraph(ResearchState)

        # Add nodes
        self.workflow.add_node("grounding", self.ground.run)
        self.workflow.add_node(
            "query_generator", self.query_generator.run
        )  # <-- NEW: Add generator node
        # --- v2: Add 5 new/refocused nodes ---
        self.workflow.add_node("company_brief_node", self.company_brief_node.run)
        self.workflow.add_node("news_signal_node", self.news_signal_node.run)
        self.workflow.add_node("flw_analyzer", self.flw_analyzer.run)
        self.workflow.add_node("contact_finder", self.contact_finder.run)
        self.workflow.add_node("engagement_finder", self.engagement_finder.run)
        # --- End v2 Nodes ---
        self.workflow.add_node("collector", self.collector.run)
        self.workflow.add_node("curator", self.curator.run)
        self.workflow.add_node("enricher", self.enricher.run)
        self.workflow.add_node("briefing", self.briefing.run)
        self.workflow.add_node(
            "executive_summary", self.executive_summary.run
        )  # <-- NEW: Add executive summary node
        self.workflow.add_node(
            "raw_compiler", simple_report_compiler_node
        )  # Keep raw compiler
        self.workflow.add_node("tagger", self.tagger.run)
        self.workflow.add_node("airtable_uploader", self.airtable_upload_node)

        # Configure workflow edges
        self.workflow.set_entry_point("grounding")
        self.workflow.set_finish_point("airtable_uploader")

        self.workflow.add_edge(
            "grounding", "query_generator"
        )  # <-- NEW: Link grounding to generator

        # --- v2: Define 5 parallel research nodes ---
        research_nodes = [
            "company_brief_node",
            "news_signal_node",
            "flw_analyzer",
            "contact_finder",
            "engagement_finder",
        ]
        # --- End v2 ---

        # --- NEW: Link generator to parallel researchers ---
        for node in research_nodes:
            self.workflow.add_edge("query_generator", node)
            self.workflow.add_edge(node, "collector")
        # --- END NEW LINKS ---

        self.workflow.add_edge("collector", "curator")
        self.workflow.add_edge("curator", "enricher")
        self.workflow.add_edge("enricher", "briefing")

        # --- MODIFIED EDGES TO BYPASS EDITOR ---
        self.workflow.add_edge(
            "briefing", "executive_summary"
        )  # Generate executive summary after briefings
        self.workflow.add_edge(
            "executive_summary", "raw_compiler"
        )  # Compiler still creates markdown report
        self.workflow.add_edge(
            "raw_compiler", "tagger"
        )  # Compiler output (with state['report']) goes to tagger
        self.workflow.add_edge(
            "tagger", "airtable_uploader"
        )  # Tagger completes, then upload to Airtable
        # --- END MODIFIED EDGES ---

        self.app = self.workflow.compile()

    def __init__(
        self,
        company=None,
        url=None,
        hq_location=None,
        industry=None,
        websocket_manager=None,
        job_id=None,
        google_drive_folder_url=None,
        use_local_context=False,
    ):  # <-- NEW: flag from Airtable
        self.websocket_manager = websocket_manager
        self.job_id = job_id

        self.input_state = InputState(
            company=company,
            company_url=url,
            hq_location=hq_location,
            industry=industry,
            websocket_manager=websocket_manager,
            job_id=job_id,
            airtable_record_id=None,
            google_drive_folder_url=google_drive_folder_url,  # Pass GDrive URL
            use_local_context=use_local_context,  # <-- NEW: pass to initial state
            messages=[
                SystemMessage(content="Expert researcher starting investigation")
            ],
        )

        self._init_nodes()
        self._build_workflow()

    def _init_nodes(self):
        """Initialize all workflow nodes (v2)"""
        self.ground = GroundingNode()
        self.query_generator = QueryGeneratorNode()  # <-- NEW: Initialize generator

        # --- v2: Initialize 5 new/refocused researcher nodes ---
        self.company_brief_node = CompanyBriefNode()
        self.news_signal_node = NewsSignalNode()
        self.flw_analyzer = FLWAnalyzer()
        self.contact_finder = ContactFinderNode()
        self.engagement_finder = EngagementFinderNode()
        # --- End v2 Init ---

        self.collector = Collector()
        self.curator = Curator()
        self.enricher = Enricher()
        self.briefing = Briefing()
        self.executive_summary = (
            ExecutiveSummaryNode()
        )  # <-- NEW: Initialize executive summary
        self.tagger = Tagger()
        # NOTE: self.editor is correctly removed

    async def airtable_upload_node(self, state: ResearchState) -> ResearchState:
        """(v2) Uploads final report to Airtable AND raw context to Google Drive."""
        import asyncio
        import os
        from datetime import datetime
        from langchain_core.messages import AIMessage
        from backend.airtable_uploader import upload_to_airtable
        from backend.utils.references import format_references_section

        logger.info("Starting final upload node (Airtable + Google Drive)...")
        try:
            job_id = state.get("job_id")
            record_id = state.get("airtable_record_id")
            company_name = state.get("company", "Unknown_Company")

            # Update status to "Compiling Report"
            if record_id:
                from backend.utils.status_constants import ResearchStatus
                from backend.airtable_uploader import update_airtable_record
                await asyncio.to_thread(update_airtable_record, record_id, {'Research Status': ResearchStatus.COMPILING_REPORT})
                logger.info(f"Updated Airtable status to 'Compiling Report' for record {record_id}")

            # --- 1. Google Drive Uploads (JSON + PDF) ---
            google_drive_folder_url = state.get("google_drive_folder_url")
            if google_drive_folder_url:
                logger.info(f"Google Drive URL found. Preparing files for upload...")
                from backend.utils.gdrive_uploader import upload_context_to_gdrive

                gdrive_upload_results = {}

                # 1a. Upload JSON research file
                try:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    json_filename = f"{company_name.lower().replace(' ', '_')}_research_{timestamp}.json"

                    # Prepare research data dictionary
                    research_json = {
                        "company": company_name,
                        "website": state.get("website", ""),
                        "hq_location": state.get("hq_location", ""),
                        "job_id": job_id,
                        "timestamp": timestamp,
                        "industries": state.get("airtable_industries", []),
                        "country_region": state.get("airtable_country_region", []),
                        "revenue_band": state.get("airtable_revenue_band_est", []),
                        "refed_alignment": state.get("airtable_refed_alignment", []),
                        "briefings": {
                            "company_brief": state.get("company_brief_briefing", ""),
                            "flw_sustainability": state.get("flw_sustainability_briefing", ""),
                            "news_signals": state.get("news_signal_briefing", ""),
                            "engagement": state.get("engagement_briefing", ""),
                            "contacts": state.get("contact_briefing", ""),
                        },
                        "executive_summary": state.get("executive_summary", ""),
                        "references": state.get("references", []),
                        "reference_info": state.get("reference_info", {}),
                        "reference_titles": state.get("reference_titles", {}),
                    }

                    await upload_context_to_gdrive(
                        research_json,
                        google_drive_folder_url,
                        json_filename,
                        'application/json'
                    )
                    logger.info(f"✅ Successfully uploaded JSON research file to Google Drive: {json_filename}")
                    gdrive_upload_results['json_file'] = json_filename
                except Exception as json_exc:
                    logger.error(f"Failed to upload JSON research file to Google Drive: {json_exc}", exc_info=True)
                    state.setdefault("messages", []).append(
                        AIMessage(content=f"⚠️ Failed to upload JSON to Google Drive: {json_exc}")
                    )

                # 1b. Upload executive summary PDF
                pdf_path = state.get("executive_summary_pdf_file")
                if pdf_path and os.path.exists(pdf_path):
                    try:
                        # Use the filename from the path
                        pdf_filename = os.path.basename(pdf_path)
                        # Open the PDF file and upload
                        with open(pdf_path, "rb") as pdf_file:
                            await upload_context_to_gdrive(
                                pdf_file,
                                google_drive_folder_url,
                                pdf_filename,
                                'application/pdf'
                            )
                        logger.info(f"✅ Successfully uploaded executive summary PDF to Google Drive: {pdf_filename}")
                        gdrive_upload_results['pdf_file'] = pdf_filename
                        # Optionally, delete the temp file after upload
                        try:
                            os.remove(pdf_path)
                            logger.info(f"Deleted temporary PDF file: {pdf_path}")
                        except Exception as del_exc:
                            logger.warning(f"Could not delete temp PDF file: {pdf_path} ({del_exc})")
                    except Exception as gdrive_exc:
                        logger.error(f"Failed to upload executive summary PDF to Google Drive: {gdrive_exc}", exc_info=True)
                        state.setdefault("messages", []).append(
                            AIMessage(content=f"⚠️ Failed to upload PDF to Google Drive: {gdrive_exc}")
                        )
                else:
                    logger.warning("No executive summary PDF file found in state, skipping PDF upload.")

                # Store upload results
                if gdrive_upload_results:
                    state['gdrive_uploads'] = gdrive_upload_results
            else:
                logger.info("No Google Drive URL provided in state, skipping file uploads.")

            # --- 2. Airtable Upload Preparation ---
            # Build Process Notes
            process_notes = []
            queries_found = False
            for message in state.get("messages", []):
                content = getattr(message, 'content', '')
                if isinstance(content, str):
                    if content.startswith("🔍 Subqueries") or content.startswith("📊 Successfully generated all research queries"):
                        if not queries_found:
                            process_notes.append("--- Queries Generated ---")
                            queries_found = True
                        queries = content.split('\n', 1)[-1] if '\n' in content else content
                        process_notes.append(queries)
                    elif any(keyword in content.lower() for keyword in [
                        "curating", "document kept", "no relevant documents",
                        "enriching", "extracting content", "enrichment complete",
                        "briefing for", "briefing start", "briefing complete",
                        "compiling", "classification", "classifying",
                        "editor bypassed"
                    ]):
                         process_notes.append(content)
            if not process_notes:
                 process_notes.append(f"Final Report Uploaded on {datetime.now().isoformat()} (Job ID: {job_id})")
            process_notes_str = "\n".join(process_notes)

            # Build References
            references_str = ""
            references_list = state.get("references", [])
            reference_info = state.get("reference_info", {})
            reference_titles = state.get("reference_titles", {})
            if references_list:
                try:
                    references_str = format_references_section(references_list, reference_info, reference_titles)
                    references_str = references_str.replace("## References\n", "").strip()
                except Exception as ref_fmt_exc:
                     logger.error(f"Error formatting references in upload node: {ref_fmt_exc}")
                     references_str = "[Error formatting references]"

            # Map v2 data for Airtable
            revenue_tag_list = state.get("airtable_revenue_band_est", [])
            revenue_tag = revenue_tag_list[0] if isinstance(revenue_tag_list, list) and revenue_tag_list else None

            report_data = {
                 # --- BASIC INFO (Match Airtable column names) ---
                 "Organization": state.get("company"),
                 "Website": state.get("company_url"),

                 # --- v2 TAG MAPPINGS (Match Airtable column names) ---
                 "Industries": state.get("airtable_industries", []),
                 "Country/Region": state.get("airtable_country_region", []),
                 "Revenue Band (est.)": revenue_tag,
                 "ReFED Alignment": state.get("airtable_refed_alignment", []),

                 # --- v2 REPORT/BRIEFING MAPPINGS (Match Airtable column names) ---
                 "Markdown Report": state.get("report", ""),
                 "Company Briefing": state.get("company_brief_briefing", ""),
                 "News & Signals Briefing": state.get("news_signal_briefing", ""),
                 "FLW and Sustainability Briefing": state.get("flw_sustainability_briefing", ""),
                 "Engagements Briefing": state.get("engagement_briefing", ""),

                 # --- NOTES/REFERENCES MAPPINGS (Match Airtable column names) ---
                 "Process Notes": process_notes_str,
                 "References": references_str,
            }

            # Log data being sent (excluding large fields)
            loggable_report_data = {k: v for k, v in report_data.items() if k not in [
                "Markdown Report", "Process Notes", "References",
                "Company Briefing", "News & Signals Briefing", "FLW and Sustainability Briefing",
                "Engagements Briefing"
            ]}
            logger.info(f"DEBUG: Data prepared for Airtable: {loggable_report_data}")

            # Step 1: Upload main company record
            upload_result = await asyncio.to_thread(
                upload_to_airtable,
                report_data,
                job_id,
                record_id
            )
            logger.info(f"Airtable upload result: {upload_result}")

            # Step 2: If company upload successful, process contacts
            if upload_result.get("status") == "Success" and upload_result.get("airtable_record_id"):
                state["airtable_record_id"] = upload_result.get("airtable_record_id")

                # Get the contact briefing JSON string
                contact_briefing = state.get("contact_briefing")
                if contact_briefing:
                    try:
                        # Process contacts in their own table
                        from backend.airtable_uploader import create_and_link_contacts
                        contact_result = await asyncio.to_thread(
                            create_and_link_contacts,
                            contact_briefing,
                            state["airtable_record_id"]
                        )
                        logger.info(f"Contact processing result: {contact_result}")

                        # Store contact processing results in state
                        state["contact_processing_results"] = contact_result

                        if contact_result.get("status") != "Success":
                            logger.error(f"Failed to process contacts: {contact_result.get('error')}")
                            state.setdefault("messages", []).append(
                                AIMessage(content=f"⚠️ Contact processing failed: {contact_result.get('error')}")
                            )
                    except Exception as contact_exc:
                        logger.error(f"Error during contact processing: {contact_exc}")
                        state.setdefault("messages", []).append(
                            AIMessage(content=f"⚠️ Contact processing error: {str(contact_exc)}")
                        )
                else:
                    logger.info("No contact briefing found in state, skipping contact processing")

        except Exception as e:
            logger.error(f"Error during Airtable upload node: {e}", exc_info=True)
            state.setdefault("messages", []).append(
                AIMessage(content=f"⚠️ Airtable upload node failed: {str(e)}")
            )
            # CRITICAL: Return state even on error to prevent graph termination
            return state

        return state

    def _sync_generate_pdf(self, markdown_content: str, output_path: str):
        """Synchronously generates a PDF from markdown content using WeasyPrint."""
        from weasyprint import CSS, HTML

        # --- NEW: Improved CSS for better formatting ---
        css_style = """
        @page {
            size: A4;
            margin: 1in;
        }
        h1 {
            font-size: 24pt;
            color: #333;
        }
        h2 {
            font-size: 20pt;
            color: #444;
        }
        h3 {
            font-size: 18pt;
            color: #555;
        }
        p {
            font-size: 12pt;
            line-height: 1.5;
            color: #666;
        }
        ul, ol {
            margin-left: 20px;
            margin-bottom: 10px;
        }
        li {
            font-size: 12pt;
            color: #666;
        }
        """
        # --- END NEW ---

        # Convert markdown to HTML
        html_content = f"<html><body>{markdown_content}</body></html>"

        # Generate PDF
        HTML(string=html_content).write_pdf(
            output_path, stylesheets=[CSS(string=css_style)]
        )
