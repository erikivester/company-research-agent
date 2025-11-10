"""Mock data for testing without making real API calls"""
from typing import Dict, Any
from datetime import datetime

# Marker to identify mock data source
MOCK_DATA_MARKER = "[MOCK DATA]"
ENRICHMENT_MARKER = "[ENRICHED]"

# Pre-structured research template that matches our output schema
DEFAULT_COMPANY_DATA = {
    "name": "Walmart",
    "headquarters": "Bentonville, Arkansas, USA",
    "website": "www.walmart.com",
    "industry": "Retail",
    "region": "Global",
    "scale": "Fortune 1",
    "sustainability_role": "Industry Leader",
    "partnership_fit": "High"
}

MOCK_RESEARCH_TEMPLATE = {
    "company_identity": {
        "name": "Walmart",
        "headquarters": "Bentonville, Arkansas, USA",
        "website": "www.walmart.com",
        "industry": "Retail",
        "region": "Global",
        "scale": "Fortune 1",
        "sustainability_role": "Industry Leader",
        "partnership_fit": "High"
    },
    "official_content": {
        "company_information": {
            "revenue_2024": "$611.3B",
            "growth_rate": "6% YoY",
            "market_position": "Global retail leader",
            "stores": "10,500+ globally"
        },
        "sustainability_reporting": {
            "latest_report": "2024 ESG Report",
            "key_commitments": [
                "Zero food waste by 2025",
                "100% renewable energy by 2035",
                "Sustainable packaging initiatives"
            ]
        }
    },
    "recent_developments": {
        "news_coverage": {
            "major_announcements": [
                {
                    "title": "New Sustainability Goals Announced",
                    "date": "2024-Q4",
                    "summary": "Expanded commitment to food waste reduction"
                }
            ],
            "relevant_updates": [
                "23% e-commerce growth in 2024",
                "New AI-powered inventory management system",
                "Expanded food bank partnerships"
            ]
        }
    },
    "key_personnel": {
        "identified_contacts": {
            "sustainability": [
                {
                    "name": "Jane Smith",
                    "title": "Senior Sustainability Manager",
                    "contact": "sustainability@walmart.com"
                }
            ],
            "operations": [
                {
                    "name": "Michael Johnson",
                    "title": "Director of Supply Chain",
                    "contact": "supply.chain@walmart.com"
                }
            ],
            "community": [
                {
                    "name": "Sarah Williams",
                    "title": "Community Relations Manager",
                    "contact": "community@walmart.com"
                }
            ]
        }
    },
    "engagement_signals": {
        "partnerships_and_initiatives": {
            "current_partnerships": [
                "U.S. Food Waste Pact",
                "ReFED Innovation Partner",
                "Food Bank Network"
            ],
            "investment_areas": [
                "Food waste reduction technology",
                "Sustainable packaging",
                "Supply chain optimization"
            ]
        }
    },
    "source_credibility": {
        "reference_info": {
            "official_sources": 3,
            "news_articles": 5,
            "industry_reports": 2
        },
        "reference_titles": {
            "2024 Annual Report": 0.95,
            "ESG Report 2024": 0.92,
            "Q4 Earnings Call": 0.88
        },
        "source_metrics": {
            "total_analyzed": 10,
            "relevant_sources": 8
        }
    },
    "communication_insights": {
        "company_voice": {
            "mission_statement": "Save people money so they can live better",
            "sustainability_language": "Leading retail sustainability through innovation and partnership"
        },
        "key_initiatives": [
            "Project Gigaton - supplier emission reduction",
            "Zero Waste Program",
            "Food Security Initiative"
        ],
        "notable_quotes": [
            {
                "quote": "Our commitment to zero food waste by 2025 is unwavering",
                "attribution": "Senior Sustainability Manager",
                "context": "2024 Sustainability Summit"
            }
        ]
    }
}

MOCK_SEARCH_RESULTS = {
    "company_brief": {
        "Walmart annual revenue 2024 2025": {
            "results": [
                {
                    "title": "Walmart Financial Outlook 2024-2025",
                    "url": "https://example.com/walmart-financial-2024",
                    "content": "Financial Performance (2024-2025):\n" + 
                              "• Revenue: $611.3B in FY2024 (↑6% YoY)\n" +
                              "• 2025 Projection: $630-640B\n" +
                              "• Operating Income: $32.7B\n" +
                              "• E-commerce Growth: +23%\n" +
                              "• Market Position: Global retail leader\n\n" +
                              "Key Growth Drivers:\n" +
                              "• Digital transformation initiatives\n" +
                              "• Expansion of healthcare services\n" +
                              "• Supply chain optimization\n" +
                              "• International market penetration",
                    "score": 0.95,
                    "raw_content": "Walmart (NYSE: WMT) reported strong financial performance in fiscal year 2024, with revenue reaching $611.3 billion, representing a 6% year-over-year growth. The company's strategic initiatives in digital transformation and healthcare services expansion have contributed to this success. Operating income stood at $32.7 billion, while e-commerce sales showed remarkable growth of 23%. Looking ahead to 2025, Walmart projects revenue between $630-640 billion, supported by continued investments in supply chain optimization and international market expansion.",
                    "source_quality": 0.92,
                    "source_type": "official_report",
                    "template_data": {
                        "company_identity": {
                            "name": "Walmart",
                            "headquarters": "Bentonville, Arkansas, USA",
                            "website": "www.walmart.com",
                            "industry": "Retail",
                            "region": "Global",
                            "scale": "Fortune 1",
                            "sustainability_role": "Industry Leader",
                            "partnership_fit": "High"
                        }
                    },
                    "last_updated": datetime.now().isoformat()
                },
                {
                    "title": f"{MOCK_DATA_MARKER} Walmart Q4 2024 Earnings Report",
                    "url": "https://example.com/walmart-q4-2024",
                    "content": f"{MOCK_DATA_MARKER} Q4 2024 Performance Metrics:\n\n" +
                              "• E-commerce: +23% YoY growth\n" +
                              "• Same-store sales: +4.9%\n" +
                              "• Market share: 25.3%\n\n" +
                              f"{ENRICHMENT_MARKER} Strategic Highlights:\n" +
                              "• Digital acceleration program\n" +
                              "• AI-powered inventory management\n" +
                              "• Last-mile delivery optimization\n" +
                              "• Customer experience improvements",
                    "score": 0.92,
                    "raw_content": "Detailed quarterly analysis reveals Walmart's continued dominance...",
                    "source_quality": 0.89,
                    "last_updated": datetime.now().isoformat()
                }
            ]
        },
        "Walmart core products and services": {
            "results": [
                {
                    "title": f"{MOCK_DATA_MARKER} Walmart Business Overview 2024",
                    "url": "https://example.com/walmart-overview",
                    "content": f"{MOCK_DATA_MARKER} Core Business Segments:\n\n" +
                              "• Retail Operations: 10,500+ global stores\n" +
                              "• E-commerce: Multi-channel platform\n" +
                              "• Grocery: Market leader in 15 countries\n" +
                              "• Healthcare: 4,000+ pharmacy locations\n\n" +
                              f"{ENRICHMENT_MARKER} Innovation Initiatives:\n" +
                              "• AI-powered shopping experience\n" +
                              "• Autonomous delivery pilots\n" +
                              "• Healthcare service expansion\n" +
                              "• Sustainable supply chain",
                    "score": 0.88,
                    "raw_content": "Comprehensive analysis of Walmart's business segments and growth initiatives...",
                    "source_quality": 0.91,
                    "last_updated": datetime.now().isoformat()
                }
            ]
        }
    },
    "flw_analyzer": {
        "Walmart ESG Report 2024 2025": {
            "results": [
                {
                    "title": "Walmart Sustainability Report 2024",
                    "url": "https://example.com/walmart-sustainability-2024",
                    "content": "Walmart has committed to zero food waste by 2025 across all operations. The company has implemented advanced inventory management systems and food donation programs.",
                    "score": 0.94,
                    "template_data": {
                        "official_content": {
                            "sustainability_reporting": MOCK_RESEARCH_TEMPLATE["official_content"]["sustainability_reporting"]
                        }
                    }
                }
            ]
        },
        "Walmart food waste prevention initiatives": {
            "results": [
                {
                    "title": "Walmart Food Waste Reduction Program",
                    "url": "https://example.com/walmart-food-waste",
                    "content": "Walmart's food waste prevention program includes AI-powered inventory management, improved cold chain logistics, and partnerships with food banks.",
                    "score": 0.91,
                    "template_data": {
                        "official_content": {
                            "sustainability_reporting": MOCK_RESEARCH_TEMPLATE["official_content"]["sustainability_reporting"]
                        }
                    }
                }
            ]
        }
    },
    "contact_finder": {
        "Walmart Sustainability Manager contacts": {
            "results": [
                {
                    "title": "Jane Smith - Senior Sustainability Manager at Walmart",
                    "url": "https://example.com/walmart-sustainability-team",
                    "content": "Jane Smith serves as Senior Sustainability Manager at Walmart, leading food waste reduction initiatives. Contact: sustainability@walmart.com",
                    "score": 0.89
                },
                {
                    "title": "Michael Johnson - Director of Supply Chain at Walmart",
                    "url": "https://example.com/walmart-operations-team",
                    "content": "Michael Johnson oversees inventory management and waste reduction in logistics as Director of Supply Chain at Walmart. Contact: supply.chain@walmart.com",
                    "score": 0.87
                },
                {
                    "title": "Sarah Williams - Community Relations Manager at Walmart",
                    "url": "https://example.com/walmart-community-team",
                    "content": "Sarah Williams manages food donation partnerships and community engagement initiatives as Community Relations Manager at Walmart. Contact: community@walmart.com",
                    "score": 0.85
                }
            ]
        }
    },
    "news_signal": {
        "Walmart sustainability news 2024 2025": {
            "results": [
                {
                    "title": "Walmart Announces New Sustainability Goals",
                    "url": "https://example.com/walmart-sustainability-news",
                    "content": "Walmart has announced ambitious new sustainability goals for 2025, including 100% renewable energy and zero waste to landfill.",
                    "score": 0.93
                }
            ]
        }
    },
    "engagement_finder": {
        "Walmart nonprofit partnerships 2024 2025": {
            "results": [
                {
                    "title": "Walmart Partners with Food Banks",
                    "url": "https://example.com/walmart-partnerships",
                    "content": "Walmart has expanded its partnership network with food banks and sustainability organizations, committing $25 million to food waste reduction programs.",
                    "score": 0.90
                }
            ]
        }
    }
}

def get_mock_results(query: str, analyst_type: str) -> Dict[str, Any]:
    """Get mock search results for testing."""
    # Create base response structure
    response = {"results": []}
    
    if analyst_type not in MOCK_SEARCH_RESULTS:
        return response

    # Get relevant mock data section
    section_data = None
    for mock_query, mock_data in MOCK_SEARCH_RESULTS[analyst_type].items():
        if any(word.lower() in query.lower() for word in mock_query.split()):
            section_data = mock_data
            break
    
    if not section_data:
        section_data = next(iter(MOCK_SEARCH_RESULTS[analyst_type].values()))

    # Get base company data
    company_data = DEFAULT_COMPANY_DATA.copy()
    
    # Add source type and template data based on analyst type
    if "results" in section_data:
        for result in section_data["results"]:
            # Set appropriate source type
            if analyst_type == "company_brief":
                result["source_type"] = "official_report"
            elif analyst_type == "news_signal":
                result["source_type"] = "news"
            elif analyst_type == "contact_finder":
                result["source_type"] = "contact"
            elif analyst_type == "engagement_finder":
                result["source_type"] = "engagement"
            elif analyst_type == "flw_analyzer":
                result["source_type"] = "sustainability"

            # Ensure raw_content exists
            if "raw_content" not in result or not result["raw_content"]:
                result["raw_content"] = result["content"]
            
            # Add relevant template data based on analyst type
            if analyst_type == "company_brief":
                result["template_data"] = {
                    "company_identity": MOCK_RESEARCH_TEMPLATE["company_identity"],
                    "official_content": MOCK_RESEARCH_TEMPLATE["official_content"]
                }
            elif analyst_type == "news_signal":
                result["template_data"] = {
                    "recent_developments": MOCK_RESEARCH_TEMPLATE["recent_developments"]
                }
            elif analyst_type == "contact_finder":
                result["template_data"] = {
                    "key_personnel": MOCK_RESEARCH_TEMPLATE["key_personnel"]
                }
            elif analyst_type == "engagement_finder":
                result["template_data"] = {
                    "engagement_signals": MOCK_RESEARCH_TEMPLATE["engagement_signals"]
                }
            
            # Add source credibility data
            result["template_data"]["source_credibility"] = {
                "reference_info": MOCK_RESEARCH_TEMPLATE["source_credibility"]["reference_info"],
                "source_metrics": MOCK_RESEARCH_TEMPLATE["source_credibility"]["source_metrics"]
            }

    return section_data