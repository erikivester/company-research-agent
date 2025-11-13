"""Context analyzer for AI-optimized document formatting."""

import logging
from typing import Any, Dict

from backend.utils.context_polisher import ContextPolisher

logger = logging.getLogger(__name__)


class ContextAnalyzer:
    """Analyzes and structures research context for AI consumption."""

    def __init__(self):
        self.polisher = ContextPolisher()

    async def prepare_context(self, research_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepares research context by:
        1. Adding metadata useful for AI systems
        2. Light formatting of content
        3. Preserving data relationships and context

        Args:
            research_data: Raw research data dictionary

        Returns:
            Enhanced dictionary with AI-optimized structure
        """
        try:
            logger.debug(
                f"Received research data keys: {research_data.keys() if isinstance(research_data, dict) else 'Not a dictionary'}"
            )

            # Start with a fresh structure
            enhanced_data = {
                "company_identity": {
                    "name": "Company",
                    "headquarters": "Not Available",
                    "industry": "Not Available",
                    "region": "Not Available",
                    "scale": "Not Available",
                    "website": "Not Available",
                },
                "research_meta": {
                    "queries_used": {},
                    "successful_extractions": {
                        "total_analyzed": 0,
                        "relevant_sources": 0,
                        "contacts_found": 0,
                    },
                    "timestamp": "",
                    "data_relationships": {  # <-- ADD THIS
                        "queries_to_sources": {}  # <-- AND THIS
                    },
                },
                "official_content": {
                    "company_information": {},
                    "sustainability_reporting": {},
                },
                "recent_developments": {"news_coverage": {}},
                "key_personnel": {"identified_contacts": {}},
                "engagement_signals": {"partnerships_and_initiatives": {}},
                "source_credibility": {"reference_info": {}},
            }

            # Update with actual data if available
            if isinstance(research_data, dict):
                # Extract company identity data
                company_data = {}
                for section in research_data.get("official_content", {}).values():
                    for data in section.values():
                        if (
                            isinstance(data, dict)
                            and data.get("source_type") == "company_website"
                        ):
                            company_data = data
                            break

                # Update company identity
                company_name = (
                    company_data.get("company_name")
                    or research_data.get("company_name")
                    or research_data.get("name", "Company")
                )
                enhanced_data["company_identity"].update(
                    {
                        "name": company_name,
                        "headquarters": company_data.get(
                            "headquarters", "Not Available"
                        ),
                        "industry": company_data.get("industry", "Not Available"),
                        "region": company_data.get("region", "Not Available"),
                        "scale": company_data.get("scale", "Not Available"),
                        "website": company_data.get("website", "Not Available"),
                    }
                )

                # Update with actual data if available
                if "research_meta" in research_data:
                    enhanced_data["research_meta"].update(
                        research_data["research_meta"]
                    )
                    logger.debug(
                        f"Updated research_meta with keys: {research_data['research_meta'].keys()}"
                    )

                # Update content sections
                for section in [
                    "official_content",
                    "recent_developments",
                    "key_personnel",
                    "engagement_signals",
                    "source_credibility",
                ]:
                    if section in research_data:
                        enhanced_data[section].update(research_data[section])
                        logger.debug(
                            f"Updated section '{section}' with keys: {research_data[section].keys()}"
                        )
                    else:
                        logger.warning(f"Missing section in research data: {section}")

                # Include final summary if available
                if "final_summary" in research_data:
                    enhanced_data["final_summary"] = research_data["final_summary"]
                    logger.debug("Added final_summary to enhanced data")
                else:
                    logger.warning("Missing final_summary in research data")

                # Log final structure
                logger.debug(
                    f"Final enhanced data structure keys: {enhanced_data.keys()}"
                )
                logger.debug(
                    f"Number of sources in official_content: {len(enhanced_data['official_content'].get('company_information', {}))}"
                )
                logger.debug(
                    f"Number of news items: {len(enhanced_data['recent_developments'].get('news_coverage', {}))}"
                )
                logger.debug(
                    f"Number of contacts: {len(enhanced_data['key_personnel'].get('identified_contacts', {}))}"
                )

            # Track relationships between queries and their results
            for query_type, queries in (
                research_data.get("research_meta", {}).get("queries_used", {}).items()
            ):
                enhanced_data["research_meta"]["data_relationships"][
                    "queries_to_sources"
                ][
                    query_type
                ] = {}  # <-- FIXED
                for query in queries:
                    matching_sources = []  # <-- Initialize list here

                    # Look for sources that came from this query
                    for section in [
                        "official_content",
                        "recent_developments",
                        "key_personnel",
                        "engagement_signals",
                    ]:
                        for subsection in research_data.get(section, {}).values():
                            for url, data in subsection.items():
                                if data.get("query_context") == query:
                                    matching_sources.append(
                                        {
                                            "url": url,
                                            "relevance": data.get("relevance", 0),
                                            "source_type": data.get(
                                                "source_type", "unknown"
                                            ),
                                        }
                                    )

                    enhanced_data["research_meta"]["data_relationships"][
                        "queries_to_sources"
                    ][query_type][
                        query
                    ] = matching_sources  # <-- FIXED

            # Light content polishing with Gemini
            try:
                polished_sections = await self.polisher.polish_context(
                    {
                        "company_information": research_data.get(
                            "official_content", {}
                        ).get("company_information", {}),
                        "sustainability_data": research_data.get(
                            "official_content", {}
                        ).get("sustainability_reporting", {}),
                        "news_coverage": research_data.get(
                            "recent_developments", {}
                        ).get("news_coverage", {}),
                    }
                )

                # Preserve both raw and polished content
                enhanced_data["raw_content"] = research_data
                enhanced_data["polished_content"] = polished_sections

            except Exception as polish_err:
                logger.error(f"Error during content polishing: {polish_err}")
                enhanced_data["raw_content"] = research_data
                enhanced_data["polishing_error"] = str(polish_err)

            # Add source credibility context
            if "source_credibility" in research_data:
                enhanced_data["source_analysis"] = {
                    "credibility_metrics": research_data["source_credibility"],
                    "source_relationships": {
                        url: {
                            "related_sources": [
                                rel_url
                                for rel_url, rel_data in research_data[
                                    "source_credibility"
                                ]["reference_info"].items()
                                if rel_data["domain"] == data["domain"]
                                and rel_url != url
                            ],
                            "domain_authority": data.get("score", 0),
                        }
                        for url, data in research_data["source_credibility"][
                            "reference_info"
                        ].items()
                    },
                }

            return enhanced_data

        except Exception as e:
            logger.error(f"Error preparing context: {e}")
            # Return original data if enhancement fails
            return {"error": str(e), "original_data": research_data}
