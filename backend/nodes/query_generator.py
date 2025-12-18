import json
import logging
import os
from datetime import datetime

from langchain_core.messages import AIMessage
from openai import AsyncOpenAI

# Use relative import to access the ResearchState class
from ..classes import ResearchState

logger = logging.getLogger(__name__)


class QueryGeneratorNode:
    """
    A new node that generates all search queries for all 5 researcher
    nodes in a single, comprehensive LLM call.
    """

    def __init__(self) -> None:
        self.openai_key = os.getenv("OPENAI_API_KEY")
        if not self.openai_key:
            raise ValueError("OPENAI_API_KEY environment variable is not set")
        self.openai_client = AsyncOpenAI(api_key=self.openai_key)
        logger.info("Query Generator Node initialized.")

    async def generate_queries(self, state: ResearchState) -> ResearchState:
        """
        Generates a structured JSON object of search queries for all 5 categories.
        """
        company = state.get("company", "Unknown Company")
        industry = state.get("industry", "Unknown Industry")
        current_year = datetime.now().year

        logger.info(f"Generating all queries for {company}...")

        # --- PRIORITY-BASED QUERY DISTRIBUTION ---
        system_prompt = """
        You are an expert research analyst for ReFED, an organization focused on
        ending food loss and waste. Your task is to generate a JSON object
        containing lists of search queries for a corporate research agent.

        IMPORTANT: Generate queries according to these specific counts:
        - company_brief: 4 queries
        - flw_analyzer: 6 queries (EXPANDED - ReFED's core mission)
        - news_signal: 5 queries (EXPANDED - time-sensitive coverage)
        - engagement_finder: 5 queries (EXPANDED - partnership opportunities)
        - contact_finder: 3 queries (REDUCED - simpler extraction task)

        The output MUST be a valid JSON object with only these 5 keys:
        "company_brief", "news_signal", "flw_analyzer", "contact_finder", "engagement_finder".
        """

        user_prompt = f"""
        Generate search queries for the company: "{company}" (Industry: {industry}).
        Current year is {current_year}.

        1.  **company_brief**: (4 queries)
            * Focus on high-level financial health and core business.
            * Include 1-2 queries for 'ballpark annual revenue' or 'major financial health signals' for {current_year - 1} or {current_year}.
            * Include 2-3 queries on 'core products and services' or 'primary business model'.

        2.  **flw_analyzer**: (6 queries - PRIORITY CATEGORY)
            * Focus on Food Loss & Waste (FLW) and sustainability - ReFED's core mission.
            * Must include: ESG/Sustainability Report, Annual Impact Report, food waste initiatives, methane reduction, sustainable packaging, circular economy programs.
            * Example queries: '"{company}" ESG report {current_year - 1}', '"{company}" food waste reduction initiatives', '"{company}" methane emissions goals', '"{company}" sustainable packaging innovation', '"{company}" circular economy {current_year}', '"{company}" food donation program'.

        3.  **news_signal**: (5 queries - EXPANDED)
            * Find company-specific news from the last 12-18 months (e.g., use "{current_year - 1} {current_year}").
            * CRITICAL: Every query MUST include the exact company name in quotes (e.g., '"{company}"').
            * Focus on sustainability initiatives, corporate changes, and risk signals.
            * Must include: sustainability news, FLW initiatives, executive hires (sustainability roles), risk signals (layoffs/boycotts), corporate announcements.
            * DO NOT create generic industry queries - every query must be about this specific company.

        4.  **engagement_finder**: (5 queries - EXPANDED)
            * Find external signals for "common ground" and partnership entry points.
            * Must cover: nonprofit partnerships, coalition memberships, venture investments, solution provider partnerships, policy advocacy.
            * Example queries: '"{company}" nonprofit partnerships food waste', '"{company}" joins "U.S. Food Loss and Waste 2030 Champions"', '"{company}" foundation grants sustainability', '"{company}" partners Leanpath OR Apeel', '"{company}" supports food donation tax policy'.

        5.  **contact_finder**: (3 queries - FOCUSED)
            * Find relevant mid-to-high-level contacts WHO WORK FOR {company} (not partners/nonprofits).
            * **Target roles:** CSR, Sustainability, Social Impact, Philanthropy, Community Relations.
            * **Target levels:** Manager, Director, VP level.
            * **Critical:** Use LinkedIn-focused queries: '"{company}" "Director of Sustainability" LinkedIn', '"{company}" CSR manager site:linkedin.com', '"{company}" sustainability team contact'.
        """
        # --- END OF MODIFIED PROMPT ---

        try:
            response = await self.openai_client.chat.completions.create(
                model="gpt-4o-mini",  # Using a reliable model for JSON mode
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                response_format={"type": "json_object"},
                temperature=0.0,
            )

            response_content = response.choices[0].message.content
            if not response_content:
                raise ValueError("LLM returned an empty response for query generation.")

            # Parse the JSON response
            queries_json = json.loads(response_content)

            # --- FIX: Validate against the correct keys ---
            required_keys = [
                "company_brief",
                "news_signal",
                "flw_analyzer",
                "contact_finder",
                "engagement_finder",
            ]

            # Check if all keys exist, are lists, and have at least 4 queries
            if not all(
                key in queries_json
                and isinstance(queries_json[key], list)
                and len(queries_json[key]) >= 4
                for key in required_keys
            ):
                logger.warning(
                    f"LLM output did not match expected structure. Got: {response_content}"
                )
                raise ValueError(
                    "LLM output did not match the required 5 categories of 4 queries each."
                )

            # --- FIX: Trim to exactly 4 queries to be safe ---
            trimmed_queries = {key: queries_json[key][:4] for key in required_keys}

            # Create new state with deep copying of research queries
            new_state = dict(state)  # Shallow copy first

            # Initialize research_queries as a new dict if it doesn't exist
            if "research_queries" not in new_state:
                new_state["research_queries"] = {}
            elif not isinstance(new_state["research_queries"], dict):
                new_state["research_queries"] = {}

            # Explicitly set each key with a new list to avoid reference issues
            research_queries = new_state["research_queries"]
            for key, queries in trimmed_queries.items():
                research_queries[key] = list(
                    queries
                )  # Create new list for each set of queries

            logger.info(
                f"QueryGenerator: Populated research_queries with keys: {list(research_queries.keys())}"
            )
            for key, queries in research_queries.items():
                logger.info(f"QueryGenerator: {key} has {len(queries)} queries")

            # Add messages if they don't exist
            if "messages" not in new_state:
                new_state["messages"] = []

            # Debug logging for state handling
            logger.info("=== Query Generator Debug ===")
            logger.info(f"Input state keys: {list(state.keys())}")
            logger.info(f"New state keys: {list(new_state.keys())}")
            logger.info(
                f"research_queries keys: {list(new_state['research_queries'].keys())}"
            )
            for key, queries in new_state["research_queries"].items():
                logger.info(f"{key}: {len(queries)} queries - {queries}")
            logger.info("=== End Query Generator Debug ===")

            # Format nice message for UI/logs
            log_msg = "Successfully generated all research queries:\n"
            for key, queries in new_state["research_queries"].items():
                log_msg += f"  • {key}: {len(queries)} queries - {queries}\n"

            new_state["messages"].append(AIMessage(content=f"📊 {log_msg}"))
            return new_state

        except json.JSONDecodeError as e:
            logger.error(
                f"Failed to decode LLM JSON response: {e}\nResponse was: {response_content}"
            )
            raise ValueError(f"Failed to parse LLM JSON for queries: {e}")
        except Exception as e:
            logger.error(f"Error in query generation: {e}", exc_info=True)
            raise

    async def run(self, state: ResearchState) -> ResearchState:
        """
        Entry point for the LangGraph node execution.
        """
        try:
            # Send status update via WebSocket
            if websocket_manager := state.get("websocket_manager"):
                if job_id := state.get("job_id"):
                    await websocket_manager.send_status_update(
                        job_id=job_id,
                        status="processing",
                        message="Generating all research queries...",
                        result={"step": "Query Generation"},
                    )

            state = await self.generate_queries(state)

        except Exception as e:
            error_msg = f"Query Generator node failed: {str(e)}"
            logger.error(f"QueryGeneratorNode.run failed: {e}", exc_info=True)
            state["error"] = error_msg
            state.setdefault("messages", []).append(AIMessage(content=f"⚠️ {error_msg}"))

            # Ensure query structure exists to prevent downstream failures
            if "research_queries" not in state:
                # --- FIX: Populate fallback with the CORRECT keys ---
                state["research_queries"] = {
                    "company_brief": [],
                    "news_signal": [],
                    "flw_analyzer": [],
                    "contact_finder": [],
                    "engagement_finder": [],
                }

        return state
