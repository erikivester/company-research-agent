"""Context analyzer for AI-optimized document formatting."""
import logging
from typing import Dict, Any

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
            # Basic metadata enhancement
            enhanced_data = {
                "metadata": {
                    "context_type": "company_research",
                    "data_structure_version": "2.0",
                    "content_sections": [
                        "company_identity",
                        "raw_research",
                        "processed_research",
                        "source_analysis"
                    ],
                    "data_relationships": {
                        "queries_to_sources": {},
                        "sources_to_insights": {},
                        "cross_references": {}
                    }
                }
            }

            # Track relationships between queries and their results
            for query_type, queries in research_data.get('research_meta', {}).get('queries_used', {}).items():
                enhanced_data["metadata"]["data_relationships"]["queries_to_sources"][query_type] = {}
                for query in queries:
                    matching_sources = []
                    
                    # Look for sources that came from this query
                    for section in ['official_content', 'recent_developments', 'key_personnel', 'engagement_signals']:
                        for subsection in research_data.get(section, {}).values():
                            for url, data in subsection.items():
                                if data.get('query_context') == query:
                                    matching_sources.append({
                                        "url": url,
                                        "relevance": data.get('relevance', 0),
                                        "source_type": data.get('source_type', 'unknown')
                                    })
                    
                    enhanced_data["metadata"]["data_relationships"]["queries_to_sources"][query_type][query] = matching_sources

            # Light content polishing with Gemini
            try:
                polished_sections = await self.polisher.polish_context({
                    "company_information": research_data.get('official_content', {}).get('company_information', {}),
                    "sustainability_data": research_data.get('official_content', {}).get('sustainability_reporting', {}),
                    "news_coverage": research_data.get('recent_developments', {}).get('news_coverage', {})
                })
                
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
                                rel_url for rel_url, rel_data in research_data["source_credibility"]["reference_info"].items()
                                if rel_data["domain"] == data["domain"] and rel_url != url
                            ],
                            "domain_authority": data.get("score", 0)
                        }
                        for url, data in research_data["source_credibility"]["reference_info"].items()
                    }
                }

            return enhanced_data

        except Exception as e:
            logger.error(f"Error preparing context: {e}")
            # Return original data if enhancement fails
            return {
                "error": str(e),
                "original_data": research_data
            }