import logging
import os
import json
from datetime import datetime
from typing import Dict, List, Any
from openai import AsyncOpenAI

# Use relative import to access the ResearchState class
from ..classes import ResearchState
from langchain_core.messages import AIMessage

logger = logging.getLogger(__name__)

class QueryGeneratorNode:
    """
    A new node that generates all search queries for all 5 researcher
    nodes in a single, comprehensive LLM call.
    """

    def __init__(self) -> None:
        self.openai_key = os.getenv("OPENAI_API_KEY")
        if not self.openai_key:
            logger.warning("OPENAI_API_KEY not set; QueryGenerator will be inert until a key is provided.")
            self.openai_client = None
        else:
            self.openai_client = AsyncOpenAI(api_key=self.openai_key)
        logger.info("Query Generator Node initialized.")

    async def generate_queries(self, state: ResearchState) -> ResearchState:
        """
        Generates a structured JSON object of search queries for all 5 categories.
        """
        company = state.get('company', 'Unknown Company')
        industry = state.get('industry', 'Unknown Industry')
        current_year = datetime.now().year

        logger.info(f"Generating all queries for {company}...")

        # --- FIX: Changed keys to match the 'analyst_type' in researcher nodes ---
        system_prompt = """
        You are an expert research analyst for ReFED, an organization focused on
        ending food loss and waste. Your task is to generate a JSON object
        containing lists of search queries for a corporate research agent.
        You must provide exactly 4 search queries for each of the 5 categories.
        The output MUST be a valid JSON object with only these 5 keys:
        "company_brief", "news_signal", "flw_analyzer", "contact_finder", "engagement_finder".
        """

        user_prompt = f"""
        Generate exactly 4 search queries for each category for the company: "{company}"
        (Industry: {industry}). Current year is {current_year}.

        1.  **company_brief**: (4 queries)
            * Focus on high-level financial health and core business.
            * Include 1-2 queries for 'ballpark annual revenue' or 'major financial health signals' for {current_year - 1} or {current_year}.
            * Include 2-3 queries on 'core products and services' or 'primary business model'.

        2.  **news_signal**: (4 queries)
            * Find news from the last 12-18 months (e.g., use "{current_year - 1} {current_year}").
            * Focus on "ReFED-relevant hooks": FLW/Climate/Methane goals, opportunity windows (e.g., "new VP of sustainability"), or risk signals (e.g., "layoffs", "boycotts").

        3.  **flw_analyzer**: (4 queries)
            * Focus on Food Loss & Waste (FLW) and sustainability.
            * Include queries for: 'ESG Report {current_year - 1} {current_year}', 'Annual Impact Report {current_year - 1} {current_year}', methane reduction goals', 'food waste prevention initiatives', 'sustainable packaging', and 'consumer date labeling initiatives'.

        4.  **contact_finder**: (4 queries)
            * Find relevant mid-to-high-level contacts for corporate outreach.
            * **Target these specific roles/departments:** "Corporate Social Responsibility", "Social Impact", "Corporate Giving", "Cause Marketing", "Purpose Marketing", "Sustainability", "Philanthropy", "Community Impact", "Community Partnerships", "Community Relations".
            * **Target these specific levels:** "Manager", "Associate Manager", "Senior Manager", "Director", "Associate Director", "Senior Director", or "Officer" (if a corporate foundation is found).
            * Create queries combining the company name with these roles and levels.

        5.  **engagement_finder**: (4 queries)
            * Find a broad and all-encompassing set of external signals for "common ground" and "entry points".
            * **Query Example 1 (Partnerships/Memberships):** Focus on direct nonprofit partnerships, coalition memberships (e.g., '"U.S. Food Waste Pact"'), or event sponsorships (e.g., '"ReFED Summit"').
            * **Query Example 2 (Investment/Innovation):** Look for corporate venture capital (CVC) investments or grants given by their foundation related to sustainability, ag-tech, or food waste. (e.g., '"{company} venture fund" food tech investment').
            * **Query Example 3 (Supply Chain/Solution Providers):** Search for partnerships with *operational* solution providers. (e.g., '"{company}" partners with Leanpath', '"{company}" adopts Apeel', '"{company}" circular economy supply chain').
            * **Query Example 4 (Policy/Advocacy/People):** Find public policy statements, advocacy, or key board member affiliations. (e.g., '"{company}" supports food donation policy', '"{company}" board member" nonprofit').
        """
        # --- END OF MODIFIED PROMPT ---

        try:
            # Defensive logging: preserve and show local-context flags coming into the generator
            logger.info(f"QueryGenerator: entry flags - use_local_context={state.get('use_local_context')} google_drive_folder_url={state.get('google_drive_folder_url')}")
            if not self.openai_client:
                raise ValueError("OpenAI client not configured. Set OPENAI_API_KEY to enable LLM query generation.")

            response = await self.openai_client.chat.completions.create(
                model="gpt-4o-mini", # Using a reliable model for JSON mode
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.0
            )

            response_content = response.choices[0].message.content
            if not response_content:
                raise ValueError("LLM returned an empty response for query generation.")

            # Parse the JSON response
            queries_json = json.loads(response_content)

            # --- FIX: Validate against the correct keys ---
            required_keys = ["company_brief", "news_signal", "flw_analyzer", "contact_finder", "engagement_finder"]
            
            # Check if all keys exist, are lists, and have at least 4 queries
            if not all(key in queries_json and isinstance(queries_json[key], list) and len(queries_json[key]) >= 4 for key in required_keys):
                logger.warning(f"LLM output did not match expected structure. Got: {response_content}")
                raise ValueError("LLM output did not match the required 5 categories of 4 queries each.")

            # --- FIX: Trim to exactly 4 queries to be safe ---
            trimmed_queries = {key: queries_json[key][:4] for key in required_keys}

            # Create new state with deep copying of research queries
            new_state = dict(state)  # Shallow copy first
            
            # Initialize research_queries as a new dict if it doesn't exist
            if 'research_queries' not in new_state:
                new_state['research_queries'] = {}
            elif not isinstance(new_state['research_queries'], dict):
                new_state['research_queries'] = {}
                
            # Explicitly set each key with a new list to avoid reference issues
            research_queries = new_state['research_queries']
            for key, queries in trimmed_queries.items():
                research_queries[key] = list(queries)  # Create new list for each set of queries
                
            logger.info(f"QueryGenerator: Populated research_queries with keys: {list(research_queries.keys())}")
            for key, queries in research_queries.items():
                logger.info(f"QueryGenerator: {key} has {len(queries)} queries")

            # Add messages if they don't exist
            if 'messages' not in new_state:
                new_state['messages'] = []

            # Debug logging for state handling
            logger.info("=== Query Generator Debug ===")
            logger.info(f"Input state keys: {list(state.keys())}")
            logger.info(f"New state keys: {list(new_state.keys())}")
            logger.info(f"research_queries keys: {list(new_state['research_queries'].keys())}")
            for key, queries in new_state['research_queries'].items():
                logger.info(f"{key}: {len(queries)} queries - {queries}")
            logger.info("=== End Query Generator Debug ===")

            # Format nice message for UI/logs
            log_msg = "Successfully generated all research queries:\n"
            for key, queries in new_state['research_queries'].items():
                log_msg += f"  • {key}: {len(queries)} queries - {queries}\n"

            new_state['messages'].append(AIMessage(content=f"📊 {log_msg}"))
            # Preserve important input flags explicitly to avoid accidental loss during merges
            try:
                new_state['use_local_context'] = state.get('use_local_context', False)
                new_state['google_drive_folder_url'] = state.get('google_drive_folder_url')
            except Exception:
                logger.warning("QueryGenerator: failed to explicitly preserve local-context flags on new_state")
            return new_state

        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode LLM JSON response: {e}\nResponse was: {response_content}")
            raise ValueError(f"Failed to parse LLM JSON for queries: {e}")
        except Exception as e:
            logger.error(f"Error in query generation: {e}", exc_info=True)
            raise


    async def run(self, state: ResearchState) -> ResearchState:
        """
        Entry point for the LangGraph node execution.
        """
        try:
            # Invariant logging: record entry flags
            entry_use_local = bool(state.get('use_local_context'))
            entry_gdrive = state.get('google_drive_folder_url')
            logger.info(f"QueryGenerator.run - entry flags: use_local_context={entry_use_local}, google_drive_folder_url={entry_gdrive}")

            # If local context bypass is enabled, skip expensive LLM query generation.
            if entry_use_local and entry_gdrive:
                logger.info("QueryGenerator: local-mode detected - skipping query generation and returning empty query sets.")
                # Ensure research_queries exists with the correct keys (empty lists)
                state.setdefault('research_queries', {})
                for k in ["company_brief", "news_signal", "flw_analyzer", "contact_finder", "engagement_finder"]:
                    state['research_queries'].setdefault(k, [])
                # Preserve flags explicitly on the returned state
                try:
                    state['use_local_context'] = entry_use_local
                    state['google_drive_folder_url'] = entry_gdrive
                except Exception:
                    logger.warning("QueryGenerator: failed to explicitly preserve local-context flags on state before returning")
                # Add an informational message
                state.setdefault('messages', []).append(AIMessage(content="📌 Local context mode: skipped query generation (using Google Drive research)."))
                logger.info(f"QueryGenerator.run - exit (skipped) - preserved flags: use_local_context={state.get('use_local_context')}, google_drive_folder_url={state.get('google_drive_folder_url')}")
                return state

            # Send status update via WebSocket
            if websocket_manager := state.get('websocket_manager'):
                if job_id := state.get('job_id'):
                    await websocket_manager.send_status_update(
                        job_id=job_id,
                        status="processing",
                        message="Generating all research queries...",
                        result={"step": "Query Generation"}
                    )
            
            # Normal path: generate queries via LLM
            logger.info("QueryGenerator: invoking LLM to generate queries")
            state = await self.generate_queries(state)
            # Post-condition: ensure flags preserved
            logger.info(f"QueryGenerator.run - exit after generate_queries - use_local_context={state.get('use_local_context')}, google_drive_folder_url={state.get('google_drive_folder_url')}")

        except Exception as e:
            error_msg = f"Query Generator node failed: {str(e)}"
            logger.error(f"QueryGeneratorNode.run failed: {e}", exc_info=True)
            state['error'] = error_msg
            state.setdefault('messages', []).append(AIMessage(content=f"⚠️ {error_msg}"))
            
            # Ensure query structure exists to prevent downstream failures
            if 'research_queries' not in state:
                # --- FIX: Populate fallback with the CORRECT keys ---
                state['research_queries'] = {
                    "company_brief": [], "news_signal": [], "flw_analyzer": [],
                    "contact_finder": [], "engagement_finder": []
                }
        
        return state