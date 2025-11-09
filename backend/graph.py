# backend/graph.py
import asyncio
import logging
from typing import Any, AsyncIterator, Dict
from datetime import datetime

from backend.utils.context_polisher import ContextPolisher

from langchain_core.messages import AIMessage # Used in simple_report_compiler_node
from langchain_core.messages import SystemMessage
from langgraph.graph import StateGraph

from .classes.state import InputState, ResearchState
from .nodes import GroundingNode
from .nodes.query_generator import QueryGeneratorNode # <-- NEW: Import query generator
from .nodes.briefing import Briefing
from .nodes.collector import Collector
from .nodes.curator import Curator
from .nodes.enricher import Enricher
from .nodes.tagger import Tagger

# --- v2 Node Imports ---
# Import the 5 new/refocused researcher nodes
from .nodes.researchers.company import CompanyBriefNode     # MODIFIED: Renamed from CompanyAnalyzer
from .nodes.researchers.news import NewsSignalNode          # MODIFIED: Renamed from NewsScanner
from .nodes.researchers.flw import FLWAnalyzer              # KEPT: This is our 5th node
from .nodes.researchers.contact_finder import ContactFinderNode     # NEW: Added node
from .nodes.researchers.engagement_finder import EngagementFinderNode # NEW: Added node
# --- End v2 Node Imports ---

from backend.airtable_uploader import upload_to_airtable
from backend.utils.references import format_references_section
# --- Import for Google Drive Utility ---
from backend.utils.gdrive_uploader import (
    upload_context_to_gdrive, 
    upload_research_with_pdf
)

logger = logging.getLogger(__name__)

# --- UPDATED HELPER FUNCTION TO BYPASS EDITOR ---
async def simple_report_compiler_node(state: ResearchState) -> ResearchState:
    """
    Compiles individual briefings into a raw, unedited markdown report (state['report'])
    as the editor node is now bypassed.
    """
    # --- v2: Updated to use the 5 new briefing keys ---
    briefing_keys_map = {
        'company_brief_briefing': 'Company Overview & Financial Health',
        'news_signal_briefing': 'News & Signals',
        'flw_sustainability_briefing': 'FLW & Sustainability',
        'contact_briefing': 'Potential Contacts',
        'engagement_briefing': 'Engagement & Affiliations'
    }
    # Define preferred order
    report_order = [
        'company_brief_briefing', 
        'flw_sustainability_briefing',
        'news_signal_briefing', 
        'engagement_briefing',
        'contact_briefing'
    ]
    # --- End v2 Update ---

    report_parts = []
    
    company = state.get('company', 'Research Report')
    report_parts.append(f"# {company} Research Report (Raw)\n")

    for key in report_order:
        content = state.get(key)
        if isinstance(content, str) and content.strip():
            header = briefing_keys_map.get(key, key.replace('_', ' ').title())
            report_parts.append(f"## {header}\n{content}\n")
    
    # Append references section (logic remains the same)
    references_list = state.get("references", [])
    if references_list:
        ref_info = state.get("reference_info", {})
        ref_titles = state.get("reference_titles", {})
        try:
            ref_text = format_references_section(references_list, ref_info, ref_titles)
            report_parts.append(ref_text)
        except Exception as ref_fmt_exc:
            logger.error(f"Error formatting references during raw compilation: {ref_fmt_exc}")
            report_parts.append("\n## References\n[Error formatting references]")
            
    final_report = "\n".join(report_parts)
    state['report'] = final_report
    
    # Add status message to the stream for tracking
    messages = state.get('messages', [])
    messages.append(AIMessage(content=f"🚧 Editor Bypassed. Generated raw report from 5 briefings (Length: {len(final_report)} chars)."))
    state['messages'] = messages
    
    return state
# --- END UPDATED HELPER FUNCTION ---


class Graph:
    def __init__(self, company=None, url=None, hq_location=None, industry=None,
                 websocket_manager=None, job_id=None, google_drive_folder_url=None): # Added GDrive URL
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
            google_drive_folder_url=google_drive_folder_url, # Pass GDrive URL
            messages=[
                SystemMessage(content="Expert researcher starting investigation")
            ]
        )

        self._init_nodes()
        self._build_workflow()

    def _init_nodes(self):
        """Initialize all workflow nodes (v2)"""
        self.ground = GroundingNode()
        self.query_generator = QueryGeneratorNode() # <-- NEW: Initialize generator
        
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
        self.tagger = Tagger()
        # NOTE: self.editor is correctly removed

    async def airtable_upload_node(self, state: ResearchState) -> ResearchState:
        """(v2) Uploads final report to Airtable AND raw context to Google Drive."""
        logger.info("Starting final upload node (Airtable + Google Drive)...")
        try:
            job_id = state.get("job_id")
            record_id = state.get("airtable_record_id")
            company_name = state.get("company", "Unknown_Company")

            # --- 1. Google Drive Context Upload ---
            google_drive_folder_url = state.get("google_drive_folder_url")
            if google_drive_folder_url:
                logger.info(f"Google Drive URL found. Compiling comprehensive research context for upload...")
                
                # Create comprehensive context with all available content
                full_context = {
                    "company_identity": {
                        "name": state.get("company"),
                        "headquarters": state.get("hq_location"),
                        "website": state.get("company_url"),
                        "industry": state.get("classified_industry"),
                        "region": state.get("classified_region"),
                        "scale": state.get("classified_revenue"),
                        "sustainability_role": state.get("classified_flw_role"),
                        "partnership_fit": state.get("classified_refed_alignment")
                    },
                    "official_content": {
                        "company_information": {
                            source_url: {
                                "title": data.get("title", ""),
                                "raw_content": data.get("raw_content", ""),
                                "content": data.get("content", ""),
                                "source_type": data.get("doc_type", ""),
                                "relevance": data.get("score", 0),
                                "timestamp": data.get("timestamp", ""),
                                "query_context": data.get("query", "")
                            }
                            for source_url, data in state.get("company_brief_data", {}).items()
                            if data.get("raw_content") or data.get("content")
                        },
                        "sustainability_reporting": {
                            source_url: {
                                "title": data.get("title", ""),
                                "raw_content": data.get("raw_content", ""),
                                "content": data.get("content", ""),
                                "source_type": data.get("doc_type", ""),
                                "relevance": data.get("score", 0),
                                "timestamp": data.get("timestamp", ""),
                                "query_context": data.get("query", "")
                            }
                            for source_url, data in state.get("flw_data", {}).items()
                            if data.get("raw_content") or data.get("content")
                        }
                    },
                    "recent_developments": {
                        "news_coverage": {
                            source_url: {
                                "title": data.get("title", ""),
                                "raw_content": data.get("raw_content", ""),
                                "content": data.get("content", ""),
                                "source_type": data.get("doc_type", ""),
                                "relevance": data.get("score", 0),
                                "timestamp": data.get("timestamp", ""),
                                "query_context": data.get("query", "")
                            }
                            for source_url, data in state.get("news_signal_data", {}).items()
                            if data.get("raw_content") or data.get("content")
                        }
                    },
                    "key_personnel": {
                        "identified_contacts": {
                            source_url: {
                                "title": data.get("title", ""),
                                "raw_content": data.get("raw_content", ""),
                                "content": data.get("content", ""),
                                "source_type": data.get("doc_type", ""),
                                "relevance": data.get("score", 0),
                                "timestamp": data.get("timestamp", ""),
                                "extracted_contacts": data.get("extracted_contacts", []),
                                "query_context": data.get("query", "")
                            }
                            for source_url, data in state.get("contact_finder_data", {}).items()
                            if data.get("raw_content") or data.get("content")
                        }
                    },
                    "engagement_signals": {
                        "partnerships_and_initiatives": {
                            source_url: {
                                "title": data.get("title", ""),
                                "raw_content": data.get("raw_content", ""),
                                "content": data.get("content", ""),
                                "source_type": data.get("doc_type", ""),
                                "relevance": data.get("score", 0),
                                "timestamp": data.get("timestamp", ""),
                                "query_context": data.get("query", "")
                            }
                            for source_url, data in state.get("engagement_finder_data", {}).items()
                            if data.get("raw_content") or data.get("content")
                        }
                    },
                    "source_credibility": {
                        "reference_info": state.get("reference_info", {}),
                        "reference_titles": state.get("reference_titles", {}),
                        "source_metrics": {
                            "total_analyzed": len(state.get("company_brief_data", {})) + 
                                           len(state.get("news_signal_data", {})) +
                                           len(state.get("flw_data", {})) +
                                           len(state.get("contact_finder_data", {})) +
                                           len(state.get("engagement_finder_data", {})),
                            "relevant_sources": len(state.get("curated_company_brief_data", {})) +
                                             len(state.get("curated_news_signal_data", {})) +
                                             len(state.get("curated_flw_data", {})) +
                                             len(state.get("curated_contact_finder_data", {})) +
                                             len(state.get("curated_engagement_finder_data", {}))
                        }
                    },
                    "communication_insights": {
                        "company_voice": {
                            "mission_statement": next((
                                data.get("content") 
                                for data in state.get("company_brief_data", {}).values()
                                if "mission" in (data.get("title", "") + data.get("content", "")).lower()
                            ), ""),
                            "sustainability_language": next((
                                data.get("content")
                                for data in state.get("flw_data", {}).values()
                                if "sustainability" in (data.get("title", "") + data.get("content", "")).lower()
                            ), "")
                        },
                        "key_initiatives": [
                            {
                                "title": data.get("title", ""),
                                "description": data.get("content", ""),
                                "source": url
                            }
                            for url, data in state.get("engagement_finder_data", {}).items()
                            if "initiative" in (data.get("title", "") + data.get("content", "")).lower()
                            or "program" in (data.get("title", "") + data.get("content", "")).lower()
                        ],
                        "notable_quotes": [
                            {
                                "text": data.get("content", ""),
                                "source": url,
                                "context": data.get("title", "")
                            }
                            for url, data in {
                                **state.get("company_brief_data", {}),
                                **state.get("flw_data", {}),
                                **state.get("news_signal_data", {})
                            }.items()
                            if data.get("content") and '"' in data.get("content", "")
                        ]
                    },
                    "research_meta": {
                        "timestamp": datetime.now().isoformat(),
                        "queries_used": state.get("research_queries", {}),
                        "successful_extractions": {
                            "contacts_found": len([
                                1 for data in state.get("contact_finder_data", {}).values()
                                if data.get("extracted_contacts")
                            ]),
                            "initiatives_identified": len([
                                1 for data in state.get("engagement_finder_data", {}).values()
                                if "initiative" in (data.get("title", "") + data.get("content", "")).lower()
                            ])
                        }
                    }
                }
                
                if full_context:
                    try:
                        # Polish the context before upload
                        polisher = ContextPolisher()
                        polished_data = await polisher.polish_context(full_context)
                        
                        # Upload both JSON and PDF versions
                        upload_results = await upload_research_with_pdf(
                            polished_data,
                            google_drive_folder_url,
                            company_name
                        )
                        
                        logger.info(f"Successfully uploaded research files to Google Drive: {upload_results}")
                        
                        # Add file URLs to state for reference
                        state['gdrive_uploads'] = upload_results
                        
                    except Exception as gdrive_exc:
                        logger.error(f"Failed to upload context to Google Drive: {gdrive_exc}", exc_info=True)
                        # Don't stop the flow; log this error in process notes
                        state.setdefault("messages", []).append(AIMessage(content=f"⚠️ Failed to upload context to Google Drive: {gdrive_exc}"))
                else:
                    logger.warning("No enriched context found to upload to Google Drive.")
            else:
                logger.info("No Google Drive URL provided in state, skipping GDrive upload.")
            # --- End Google Drive Logic ---

            # --- 2. Airtable Upload Preparation ---
            # Build Process Notes
            process_notes = []
            queries_found = False
            for message in state.get("messages", []):
                content = getattr(message, 'content', '')
                if isinstance(content, str):
                    if content.startswith("🔍 Subqueries") or content.startswith("📊 Successfully generated all research queries"): # <-- NEW: Check for new generator log
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
                        "editor bypassed" # Updated keyword
                    ]):
                         process_notes.append(content)
            if not process_notes:
                 process_notes.append(f"Final Report Uploaded on {datetime.now().isoformat()} (Job ID: {job_id})")
            process_notes_str = "\n".join(process_notes)

            # Build References (logic unchanged, curator feeds this)
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
                 "company_name": state.get("company"),
                 "company_url": state.get("company_url"),
                 
                 # --- v2 TAG MAPPINGS ---
                 "industries_tags": state.get("airtable_industries", []),
                 "region_tags": state.get("airtable_country_region", []),
                 "revenue_tags": revenue_tag,
                 "refed_alignment_tags": state.get("airtable_refed_alignment", []), # NEW
                 
                 # --- v2 REPORT/BRIEFING MAPPINGS ---
                 "report_markdown": state.get("report", ""),
                 "company_brief_briefing": state.get("company_brief_briefing", ""),         # RENAMED
                 "news_signal_briefing": state.get("news_signal_briefing", ""),         # RENAMED
                 "flw_sustainability_briefing": state.get("flw_sustainability_briefing", ""), # KEPT
                 "contact_briefing": state.get("contact_briefing", ""),               # NEW
                 "engagement_briefing": state.get("engagement_briefing", ""),           # NEW
                 # REMOVED: financial_briefing, industry_briefing
                 
                 # --- NOTES/REFERENCES MAPPINGS ---
                 "process_notes": process_notes_str, 
                 "references_formatted": references_str, 
            }

            # Log data being sent (excluding large fields)
            loggable_report_data = {k: v for k, v in report_data.items() if k not in [
                "report_markdown", "process_notes", "references_formatted",
                "company_brief_briefing", "news_signal_briefing", "flw_sustainability_briefing",
                "contact_briefing", "engagement_briefing"
            ]}
            logger.info(f"DEBUG: Data prepared for Airtable: {loggable_report_data}")

            # Step 1: Upload main company record
            upload_result = upload_to_airtable(report_data, job_id, record_id)
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

        return state

    def _build_workflow(self):
        """Configure the state graph workflow (v2)"""
        self.workflow = StateGraph(ResearchState)

        # Add nodes
        self.workflow.add_node("grounding", self.ground.run)
        self.workflow.add_node("query_generator", self.query_generator.run) # <-- NEW: Add generator node
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
        self.workflow.add_node("raw_compiler", simple_report_compiler_node) # Keep raw compiler
        self.workflow.add_node("tagger", self.tagger.run)
        self.workflow.add_node("airtable_uploader", self.airtable_upload_node)

        # Configure workflow edges
        self.workflow.set_entry_point("grounding")
        self.workflow.set_finish_point("airtable_uploader")
        
        self.workflow.add_edge("grounding", "query_generator") # <-- NEW: Link grounding to generator

        # --- v2: Define 5 parallel research nodes ---
        research_nodes = [
            "company_brief_node", 
            "news_signal_node", 
            "flw_analyzer",
            "contact_finder",
            "engagement_finder"
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
        self.workflow.add_edge("briefing", "raw_compiler") # Briefing output goes to compiler
        self.workflow.add_edge("raw_compiler", "tagger")   # Compiler output (with state['report']) goes to tagger
        # --- END MODIFIED EDGES ---
        
        self.workflow.add_edge("tagger", "airtable_uploader")

    async def run(self, thread: Dict[str, Any]) -> AsyncIterator[Dict[str, Any]]:
        """Execute the research workflow"""
        initial_state_data = self.input_state.copy()
        # --- v2: Pass GDrive URL from thread config ---
        if 'airtable_record_id' in thread.get("configurable", {}):
             initial_state_data['airtable_record_id'] = thread["configurable"]['airtable_record_id']
        if 'google_drive_folder_url' in thread.get("configurable", {}):
             initial_state_data['google_drive_folder_url'] = thread["configurable"]['google_drive_folder_url']
        # --- End v2 ---

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
        # --- NEW: Added 'query_generator' to the order ---
        node_order = [
            "grounding", 
            "query_generator",
            "company_brief_node", # Use one of the parallel nodes as the marker
            "collector", "curator", "enricher", "briefing",
            "raw_compiler", "tagger", "airtable_uploader", "__end__"
        ]
        # --- END NEW ---
        try:
             base_index = -1
             # --- v2: Update parallel node list ---
             if current_node_name in [
                 "company_brief_node", "news_signal_node", "flw_analyzer", 
                 "contact_finder", "engagement_finder"
                ]:
                 base_index = node_order.index("company_brief_node")
             # --- End v2 ---
             elif current_node_name in node_order:
                  base_index = node_order.index(current_node_name)

             if base_index != -1:
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