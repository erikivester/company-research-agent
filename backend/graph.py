# backend/graph.py
import logging
from typing import Any, AsyncIterator, Dict
from datetime import datetime

from langchain_core.messages import SystemMessage
from langgraph.graph import StateGraph

from .classes.state import InputState, ResearchState
from .nodes import GroundingNode
from .nodes.briefing import Briefing
from .nodes.collector import Collector
from .nodes.curator import Curator
from .nodes.editor import Editor
from .nodes.enricher import Enricher
from .nodes.tagger import Tagger
from .nodes.researchers import (
    CompanyAnalyzer,
    FinancialAnalyst,
    IndustryAnalyzer,
    NewsScanner,
    FLWAnalyzer
)
from backend.airtable_uploader import upload_to_airtable
from backend.utils.references import format_references_section


logger = logging.getLogger(__name__)

class Graph:
    def __init__(self, company=None, url=None, hq_location=None, industry=None,
                 websocket_manager=None, job_id=None):
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
            messages=[
                SystemMessage(content="Expert researcher starting investigation")
            ]
        )

        self._init_nodes()
        self._build_workflow()

    def _init_nodes(self):
        """Initialize all workflow nodes"""
        self.ground = GroundingNode()
        self.financial_analyst = FinancialAnalyst()
        self.news_scanner = NewsScanner()
        self.industry_analyst = IndustryAnalyzer()
        self.company_analyst = CompanyAnalyzer()
        self.flw_analyzer = FLWAnalyzer()
        self.collector = Collector()
        self.curator = Curator()
        self.enricher = Enricher()
        self.briefing = Briefing()
        self.editor = Editor()
        self.tagger = Tagger()

    async def airtable_upload_node(self, state: ResearchState) -> ResearchState:
        """Calls the Airtable uploader function with the final report data."""
        logger.info("Uploading final report to Airtable...")
        try:
            # Basic state logging
            logger.info(f"DEBUG: State keys received by airtable_upload_node: {list(state.keys())}")

            job_id = state.get("job_id")
            record_id = state.get("airtable_record_id")

            # --- Build Process Notes ---
            process_notes = []
            queries_found = False
            for message in state.get("messages", []):
                content = getattr(message, 'content', '')
                if isinstance(content, str):
                    if content.startswith("🔍 Subqueries"):
                        if not queries_found:
                            process_notes.append("--- Queries Generated ---")
                            queries_found = True
                        queries = content.split('\n', 1)[-1] if '\n' in content else content
                        process_notes.append(queries)
                    # Add other relevant messages based on keywords
                    elif any(keyword in content.lower() for keyword in [
                        "curating", "document kept", "no relevant documents",
                        "enriching", "extracting content", "enrichment complete",
                        "briefing for", "briefing start", "briefing complete",
                        "compiling", "report compilation", "classification", "classifying"
                    ]):
                         process_notes.append(content)

            if not process_notes:
                 process_notes.append(f"Final Report Uploaded on {datetime.now().isoformat()} (Job ID: {job_id})")
            process_notes_str = "\n".join(process_notes)
            # --- End Process Notes ---

            # --- Build References ---
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
            # --- End References ---

            # --- Map data for Airtable (CRITICAL FIX) ---
            revenue_tag_list = state.get("airtable_revenue_band_est", [])
            revenue_tag = revenue_tag_list[0] if isinstance(revenue_tag_list, list) and revenue_tag_list else None

            # Keys must use the internal names expected by upload_to_airtable in airtable_uploader.py
            report_data = {
                 # Basic fields
                 "company_name": state.get("company"),
                 "company_url": state.get("company_url"),
                 
                 # --- TAG MAPPINGS (Using internal keys expected by airtable_uploader.py) ---
                 "industries_tags": state.get("airtable_industries", []),
                 "region_tags": state.get("airtable_country_region", []),
                 "revenue_tags": revenue_tag, # Value is already extracted to be a string or None
                 
                 # --- REPORT/BRIEFING MAPPINGS (Using internal keys expected by airtable_uploader.py) ---
                 "report_markdown": state.get("report", ""),
                 "financial_briefing": state.get("financial_briefing", ""), 
                 "industry_briefing": state.get("industry_briefing", ""),   
                 "company_briefing": state.get("company_briefing", ""),     
                 "news_briefing": state.get("news_briefing", ""),         
                 "flw_sustainability_briefing": state.get("flw_sustainability_briefing", ""), 
                 
                 # --- NOTES/REFERENCES MAPPINGS (Using internal keys expected by airtable_uploader.py) ---
                 "process_notes": process_notes_str, 
                 "references_formatted": references_str, 
                 # Note: Research Status is implicitly set to 'Completed' inside upload_to_airtable
            }
            # --- END report_data UPDATE ---

            # Log data being sent (excluding potentially large fields)
            loggable_report_data = {k: v for k, v in report_data.items() if k not in [
                "report_markdown", "process_notes", "references_formatted",
                "financial_briefing", "industry_briefing", "company_briefing",
                "news_briefing", "flw_sustainability_briefing"
            ]}
            logger.info(f"DEBUG: Data prepared for Airtable: {loggable_report_data}")

            # Call the uploader function (defined in backend/airtable_uploader.py)
            upload_result = upload_to_airtable(report_data, job_id, record_id)
            logger.info(f"Airtable upload result: {upload_result}")

            # Store the final Airtable record ID back into the state if successful
            if upload_result.get("status") == "Success" and upload_result.get("airtable_record_id"):
                 state["airtable_record_id"] = upload_result.get("airtable_record_id")

        except Exception as e:
            logger.error(f"Error during Airtable upload node: {e}", exc_info=True)

        return state # Always return the state

    def _build_workflow(self):
        """Configure the state graph workflow"""
        self.workflow = StateGraph(ResearchState)

        # Add nodes
        self.workflow.add_node("grounding", self.ground.run)
        self.workflow.add_node("financial_analyst", self.financial_analyst.run)
        self.workflow.add_node("news_scanner", self.news_scanner.run)
        self.workflow.add_node("industry_analyst", self.industry_analyst.run)
        self.workflow.add_node("company_analyst", self.company_analyst.run)
        self.workflow.add_node("flw_analyzer", self.flw_analyzer.run)
        self.workflow.add_node("collector", self.collector.run)
        self.workflow.add_node("curator", self.curator.run)
        self.workflow.add_node("enricher", self.enricher.run)
        self.workflow.add_node("briefing", self.briefing.run)
        self.workflow.add_node("editor", self.editor.run)
        self.workflow.add_node("tagger", self.tagger.run)
        self.workflow.add_node("airtable_uploader", self.airtable_upload_node)

        # Configure workflow edges
        self.workflow.set_entry_point("grounding")
        self.workflow.set_finish_point("airtable_uploader")

        research_nodes = [
            "financial_analyst", "news_scanner", "industry_analyst",
            "company_analyst", "flw_analyzer"
        ]

        for node in research_nodes:
            self.workflow.add_edge("grounding", node)
            self.workflow.add_edge(node, "collector")

        self.workflow.add_edge("collector", "curator")
        self.workflow.add_edge("curator", "enricher")
        self.workflow.add_edge("enricher", "briefing")
        self.workflow.add_edge("briefing", "editor")
        self.workflow.add_edge("editor", "tagger")
        self.workflow.add_edge("tagger", "airtable_uploader")

    async def run(self, thread: Dict[str, Any]) -> AsyncIterator[Dict[str, Any]]:
        """Execute the research workflow"""
        initial_state_data = self.input_state.copy()
        if 'airtable_record_id' in thread:
             initial_state_data['airtable_record_id'] = thread['airtable_record_id']

        compiled_graph = self.workflow.compile()

        async for state_update in compiled_graph.astream(
            initial_state_data, config=thread
        ):
             current_state = list(state_update.values())[0] if state_update else {}
             if self.websocket_manager and self.job_id:
                  current_node = list(state_update.keys())[0] if state_update else "unknown"
                  current_state['current_node'] = str(current_node)
                  await self._handle_ws_update(current_state)
             yield current_state

    async def _handle_ws_update(self, state: Dict[str, Any]):
        """Handle WebSocket updates based on state changes"""
        current_node_name = state.get("current_node", "unknown")
        update = {
            "type": "state_update",
            "data": {
                "current_node": current_node_name,
                "progress": self._calculate_progress(current_node_name),
                "keys": list(state.keys())
            }
        }
        job_id_to_use = state.get('job_id', self.job_id)
        if job_id_to_use:
             await self.websocket_manager.broadcast_to_job(job_id_to_use, update)
        else:
             logger.warning("Could not send WebSocket update: job_id missing in state.")

    def _calculate_progress(self, current_node_name: str) -> int:
        """Estimates progress based on the current node."""
        node_order = [
            "grounding", "financial_analyst", # Treat parallel block start as one step
            "collector", "curator", "enricher", "briefing",
            "editor", "tagger", "airtable_uploader", "__end__"
        ]
        try:
             base_index = -1
             if current_node_name in ["financial_analyst", "news_scanner", "industry_analyst", "company_analyst", "flw_analyzer"]:
                 base_index = node_order.index("financial_analyst")
             elif current_node_name in node_order:
                  base_index = node_order.index(current_node_name)

             if base_index != -1:
                  # Adjusted denominator to exclude __end__
                  progress = int(((base_index + 1) / (len(node_order) - 1)) * 100)
                  return min(progress, 100)
             else:
                  logger.warning(f"Node '{current_node_name}' not found for progress calculation.")
                  return 0
        except ValueError:
             logger.warning(f"Error finding node '{current_node_name}' for progress calculation.")
             return 0

    def compile(self):
        """Compiles the graph."""
        graph = self.workflow.compile()
        return graph