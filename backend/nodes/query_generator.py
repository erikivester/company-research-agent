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
        Generates context-aware search queries by analyzing grounding data and prior research.
        """
        company = state.get("company", "Unknown Company")
        industry = state.get("industry", "Unknown Industry")
        hq_location = state.get("hq_location", "")
        current_year = datetime.now().year

        logger.info(f"=== Query Generator Starting for {company} ===")
        logger.info(f"Industry: {industry}, HQ: {hq_location or 'Unknown'}")

        # --- EXTRACT GROUNDING CONTEXT ---
        site_scrape = state.get("site_scrape", {})
        scraped_content = ""
        if site_scrape:
            # Combine raw_content from all scraped pages
            content_parts = []
            for url, page_data in list(site_scrape.items())[:3]:  # Limit to first 3 pages
                if isinstance(page_data, dict) and "raw_content" in page_data:
                    content_parts.append(page_data["raw_content"][:2000])  # First 2k chars per page
            scraped_content = "\n\n---\n\n".join(content_parts)[:5000]  # Max 5k chars total
            logger.info(f"✅ Context available: {len(site_scrape)} scraped pages ({len(scraped_content)} chars)")
        else:
            logger.warning("⚠️ No site_scrape data available - queries will be more generic")

        # --- CHECK FOR PRIOR RESEARCH CONTEXT ---
        use_local_context = state.get("use_local_context", False)
        prior_research_summary = ""

        # Note: Prior research will be loaded by researcher nodes, not here.
        # We just note if the flag is set to inform the LLM to generate gap-filling queries
        if use_local_context:
            prior_research_summary = "PRIOR RESEARCH EXISTS: Generate queries to UPDATE and FILL GAPS in existing research, focusing on recent developments and missing information."
            logger.info("🔄 use_local_context=True - will generate gap-filling queries for research update")
        else:
            logger.info("🆕 use_local_context=False - will generate comprehensive initial research queries")

        # --- PRIORITY-BASED QUERY DISTRIBUTION ---
        system_prompt = """
        You are a Corporate Development & Strategic Partnerships Officer for ReFED.
        Your goal is to conduct due diligence on companies to evaluate them as potential
        funders, partners, or grant recipients.

        Your research must uncover:
        1. Financial Capacity: Can they afford to donate or invest? (Profits, Foundation assets).
        2. Strategic Alignment: Do they care about ReFED's specific mission (Food Waste, Climate, Circularity)?
        3. Reputational Risk: Are they accused of greenwashing or labor violations?
        4. Decision Makers: Who holds the budget for sustainability and philanthropy?

        CRITICAL INSTRUCTION - CONTEXT-AWARE QUERY GENERATION:
        You will receive company website content and metadata. ANALYZE this context to generate
        TARGETED, RELEVANT queries that:
        - Adapt to the company's actual business model (B2B vs B2C, manufacturing vs services, etc.)
        - Build on what's already known from the website (don't ask what we can already see)
        - Prioritize searches likely to yield results based on company type and industry
        - Avoid generic templates that don't match the company's sector
        - Focus on recent/updated information when prior research exists

        CRITICAL SEARCH SYNTAX REQUIREMENT:
        - ALWAYS wrap the exact company name in double quotes: "{company}"
        - This ensures search engines match the exact company, not generic keywords
        - Example: "US Foods" sustainability (CORRECT) vs US Foods sustainability (WRONG - returns generic results)
        - Every single query MUST contain the company name in quotes

        Generate queries according to these specific counts:
        - company_brief: 3 queries (REDUCED - focus on core financials)
        - flw_analyzer: 4 queries (OPTIMIZED - high-value FLW data only)
        - news_signal: 4 queries (OPTIMIZED - key signals only)
        - engagement_finder: 3 queries (REDUCED - most relevant partnerships)
        - contact_finder: 2 queries (REDUCED - key decision-makers only)

        The output MUST be a valid JSON object with only these 5 keys:
        "company_brief", "news_signal", "flw_analyzer", "contact_finder", "engagement_finder".
        Each value must be a list of query strings.
        """

        user_prompt = f"""
        Generate CONTEXT-AWARE due diligence search queries for: "{company}"
        Industry: {industry}
        {f"Headquarters: {hq_location}" if hq_location else ""}
        Current year: {current_year}

        {prior_research_summary}

        COMPANY WEBSITE CONTEXT (from grounding):
        {scraped_content if scraped_content else "No website content available - company may not have a website or scraping failed."}

        ---

        Based on the above context, generate targeted search queries. Use these as EXAMPLES to guide you,
        but ADAPT them based on what you learned from the website context:

        1.  **company_brief**: (3 queries - FOCUSED)
            * **Goal:** Assess financial health and philanthropic capacity.
            * Adapt based on company type:
              - Public companies: Focus on SEC filings, earnings reports, investor relations
              - Private companies: Search for funding rounds, private equity backing, estimated revenue
              - Nonprofits/Foundations: Search for Form 990, grant amounts, endowment size
              - Subsidiaries: Research parent company financials
            * EXAMPLE templates (customize based on context):
              - "{company}" annual revenue net income {current_year - 1}-{current_year}
              - "{company}" corporate foundation assets OR Form 990
              - "{company}" business model OR revenue streams OR funding
            * NOTE: Remove investor presentation query - often unavailable for private companies

        2.  **flw_analyzer**: (4 queries - PRIORITY CATEGORY, OPTIMIZED)
            * **Goal:** Evaluate alignment with ReFED's "Roadmap to 2030" action areas.
            * Adapt based on industry context from website:
              - Food manufacturers: Supply chain waste, upcycling, date labeling
              - Retailers/Grocery: Food donation, waste diversion, consumer education
              - Restaurants/Foodservice: Portion optimization, donation programs
              - Tech companies: Platform solutions for food waste, data transparency
              - Agriculture: On-farm waste, regenerative practices, methane reduction
              - Non-food sector: Look for indirect food waste impact (packaging, logistics, etc.)
            * CRITICAL: Prioritize HTML summaries over PDF reports (PDFs often timeout/fail)
            * EXAMPLE templates (customize based on context):
              - "{company}" sustainability highlights {current_year - 1} food waste metrics
              - "{company}" ESG performance summary food waste reduction OR donation
              - "{company}" Scope 3 emissions report purchased goods waste
              - "{company}" CDP score climate change {current_year - 1} OR sustainability awards
            * SKIP industry-specific queries (methane, regen ag) unless clearly relevant from website context

        3.  **news_signal**: (4 queries - OPTIMIZED)
            * **Goal:** Detect "Trigger Events" (opportunities) and "Red Flags" (risks).
            * Focus on RECENT developments (last 12-24 months)
            * EXAMPLE templates (customize based on context):
              - "{company}" greenwashing OR lawsuit OR controversy OR layoffs {current_year - 1}-{current_year}
              - "{company}" Chief Sustainability Officer OR Head of Social Impact hire {current_year}
              - "{company}" earnings call {current_year} sustainability mentions
              - "{company}" award recognition sustainability ESG {current_year - 1}
            * NOTE: Combine controversy + layoffs into single query to reduce volume

        4.  **engagement_finder**: (3 queries - FOCUSED)
            * **Goal:** Check for participation in ReFED coalitions, peer initiatives, and competitor activity.
            * Adapt based on company location and sector
            * EXAMPLE templates (customize based on context):
              - "{company}" member "Pacific Coast Food Waste Commitment" OR "U.S. Food Waste Pact"
              - "{company}" partnership "World Wildlife Fund" OR "WRAP" OR "Feeding America"
              - "{company}" grants awarded food systems climate equity {current_year - 1}
            * NOTE: Drop public policy query (low hit rate), combine coalition memberships

        5.  **contact_finder**: (2 queries - MINIMAL)
            * **Goal:** Find decision-makers with budget authority for sustainability/philanthropy.
            * Adapt titles based on company size and structure from website
            * EXAMPLE templates (customize based on context):
              - "{company}" "Chief Sustainability Officer" OR "VP Social Impact" OR "Head of ESG" LinkedIn
              - "{company}" "President of Foundation" OR "Director Corporate Responsibility" contact
            * NOTE: Combine similar roles into fewer queries

        IMPORTANT: These are TEMPLATES. Analyze the website context and generate queries that are:
        - Specific to this company's actual business activities
        - Likely to return results based on their sector/size/type
        - Filling gaps in what the website already told us
        - Recent and time-bounded when appropriate
        """
        # --- END OF CONTEXT-AWARE PROMPT ---

        try:
            # Use slightly higher temperature when context is available to encourage adaptation
            temperature = 0.3 if (scraped_content or use_local_context) else 0.1

            logger.info(f"Calling LLM for query generation (temperature={temperature})...")
            response = await self.openai_client.chat.completions.create(
                model="gpt-4o-mini",  # Using a reliable model for JSON mode
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                response_format={"type": "json_object"},
                temperature=temperature,
            )
            logger.info("✅ LLM response received")

            response_content = response.choices[0].message.content
            if not response_content:
                raise ValueError("LLM returned an empty response for query generation.")

            # Parse the JSON response
            queries_json = json.loads(response_content)
            logger.info("✅ JSON parsed successfully")

            # --- FIX: Validate against the correct keys ---
            required_keys = [
                "company_brief",
                "news_signal",
                "flw_analyzer",
                "contact_finder",
                "engagement_finder",
            ]

            # Expected query counts per category (minimum required)
            expected_counts = {
                "company_brief": 3,
                "flw_analyzer": 4,
                "news_signal": 4,
                "engagement_finder": 3,
                "contact_finder": 2
            }

            # Check if all keys exist and are lists
            missing_keys = [key for key in required_keys if key not in queries_json]
            if missing_keys:
                logger.error(f"❌ Missing required keys: {missing_keys}")
                raise ValueError(f"LLM output missing required keys: {missing_keys}")

            # Check if all values are lists with minimum query counts
            invalid_keys = []
            for key in required_keys:
                if not isinstance(queries_json[key], list):
                    invalid_keys.append(f"{key} (not a list)")
                elif len(queries_json[key]) < expected_counts.get(key, 3):
                    invalid_keys.append(f"{key} (got {len(queries_json[key])}, need {expected_counts[key]})")

            if invalid_keys:
                logger.error(f"❌ Invalid structure for keys: {invalid_keys}")
                logger.warning(f"LLM output: {json.dumps(queries_json, indent=2)}")
                raise ValueError(f"LLM output has invalid structure: {invalid_keys}")

            logger.info("✅ Validation passed")

            # --- Quote Validation and Auto-Fix ---
            quote_fixed_count = 0
            for category in required_keys:
                for i, query in enumerate(queries_json[category]):
                    # Check if company name is wrapped in quotes
                    if company != "Unknown Company" and f'"{company}"' not in query:
                        # Auto-fix by replacing first occurrence with quoted version
                        if company in query:
                            queries_json[category][i] = query.replace(company, f'"{company}"', 1)
                            quote_fixed_count += 1
                            logger.warning(f"Auto-fixed missing quotes in {category} query: {query}")
                        else:
                            logger.warning(f"Query missing company name entirely: {query}")

            if quote_fixed_count > 0:
                logger.info(f"✅ Auto-fixed {quote_fixed_count} queries with missing quotes")
            # --- End Quote Validation ---

            # Trim to expected query counts (in case LLM generated more)
            trimmed_queries = {
                key: queries_json[key][:expected_counts[key]]
                for key in required_keys
            }

            # Log summary of generated queries
            logger.info("=== Generated Queries Summary ===")
            for key, queries in trimmed_queries.items():
                logger.info(f"{key}: {len(queries)} queries")
                for i, query in enumerate(queries, 1):
                    logger.debug(f"  {i}. {query}")
            logger.info("=== End Query Summary ===")

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

            # Verify state keys are preserved
            logger.debug(f"State preservation check - Input keys: {len(state.keys())}, Output keys: {len(new_state.keys())}")

            # Format nice message for UI/logs
            context_info = []
            if scraped_content:
                context_info.append(f"{len(site_scrape)} website pages")
            if use_local_context:
                context_info.append("prior research context")

            context_desc = f" (using {', '.join(context_info)})" if context_info else " (no context available)"

            log_msg = f"Successfully generated context-aware research queries{context_desc}:\n"
            for key, queries in new_state["research_queries"].items():
                log_msg += f"  • {key}: {len(queries)} queries\n"
                for i, query in enumerate(queries, 1):
                    log_msg += f"    {i}. {query}\n"

            new_state["messages"].append(AIMessage(content=log_msg))
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
            # Determine context availability for status message
            has_site_scrape = bool(state.get("site_scrape"))
            use_local_context = state.get("use_local_context", False)

            context_msg = "Analyzing website content and generating targeted queries..." if has_site_scrape else "Generating research queries..."
            if use_local_context:
                context_msg = "Generating gap-filling queries based on prior research..."

            # Send status update via WebSocket
            if websocket_manager := state.get("websocket_manager"):
                if job_id := state.get("job_id"):
                    await websocket_manager.send_status_update(
                        job_id=job_id,
                        status="processing",
                        message=context_msg,
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