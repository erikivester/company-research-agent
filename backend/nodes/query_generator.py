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
            raise ValueError("OPENAI_API_KEY environment variable is not set")
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

        system_prompt = """
        You are an expert research analyst. Your task is to generate a JSON object
        containing lists of search queries for a corporate research agent.
        You must provide exactly 4 search queries for each of the 5 categories.
        The output MUST be a valid JSON object with only these 5 keys:
        "company_brief", "news_signal", "flw", "contact", "engagement".
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

        3.  **flw**: (4 queries)
            * Focus on Food Loss & Waste (FLW) and sustainability.
            * Include queries for: 'ESG Report {current_year - 1} {current_year}', 'methane reduction goals', 'food waste prevention initiatives', and 'sustainable packaging'.

        4.  **contact**: (4 queries)
            * Find relevant mid-to-high-level contacts.
            * **Prioritize** "Manager", "Senior Manager", or "Director" level roles in Sustainability, Social Impact, Community Relations, or CSR.
            * **Also include** relevant high-level contacts like "VP of Sustainability" or "Head of Impact".
            * **Exclude** general C-suite (CEO, CFO) unless their role is *directly* sustainability-focused.

        5.  **engagement**: (4 queries)
            * Find external signals of engagement and affiliations.
            * Include queries for: 'memberships' (e.g., '"U.S. Food Waste Pact"'), 'event sponsorships' (e.g., '"ReFED Summit"'), 'sustainability awards', and 'nonprofit partnerships'.
        """

        try:
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

            # Validate the structure
            required_keys = ["company_brief", "news_signal", "flw", "contact", "engagement"]
            if not all(key in queries_json and isinstance(queries_json[key], list) and len(queries_json[key]) == 4 for key in required_keys):
                logger.warning(f"LLM output did not match expected structure. Got: {response_content}")
                raise ValueError("LLM output did not match the required 5 categories of 4 queries each.")

            # Store the structured queries in the state
            state['research_queries'] = queries_json

            # Log a summary for debugging, similar to the logs we reviewed
            log_msg = "Successfully generated all research queries:\n"
            for key, queries in queries_json.items():
                log_msg += f"  • {key}: {len(queries)} queries - {queries}\n"
            logger.info(log_msg)
            state.setdefault('messages', []).append(AIMessage(content=f"📊 {log_msg}"))

        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode LLM JSON response: {e}\nResponse was: {response_content}")
            raise ValueError(f"Failed to parse LLM JSON for queries: {e}")
        except Exception as e:
            logger.error(f"Error in query generation: {e}", exc_info=True)
            raise

        return state

    async def run(self, state: ResearchState) -> ResearchState:
        """
        Entry point for the LangGraph node execution.
        """
        try:
            # Send status update via WebSocket
            if websocket_manager := state.get('websocket_manager'):
                if job_id := state.get('job_id'):
                    await websocket_manager.send_status_update(
                        job_id=job_id,
                        status="processing",
                        message="Generating all research queries...",
                        result={"step": "Query Generation"}
                    )
            
            state = await self.generate_queries(state)

        except Exception as e:
            error_msg = f"Query Generator node failed: {str(e)}"
            logger.error(f"QueryGeneratorNode.run failed: {e}", exc_info=True)
            state['error'] = error_msg
            state.setdefault('messages', []).append(AIMessage(content=f"⚠️ {error_msg}"))
            
            # Ensure query structure exists to prevent downstream failures
            if 'research_queries' not in state:
                state['research_queries'] = {
                    "company_brief": [], "news_signal": [], "flw": [],
                    "contact": [], "engagement": []
                }
        
        return state