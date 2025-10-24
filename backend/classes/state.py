# Inside backend/classes/state.py
from typing import TypedDict, NotRequired, Required, Dict, List, Any
from backend.services.websocket_manager import WebSocketManager

# Define the input state
class InputState(TypedDict, total=False):
    company: Required[str]
    company_url: NotRequired[str]
    hq_location: NotRequired[str]
    industry: NotRequired[str]
    websocket_manager: NotRequired[WebSocketManager]
    job_id: NotRequired[str]
    airtable_record_id: NotRequired[str]

class ResearchState(InputState):
    site_scrape: Dict[str, Any]
    messages: List[Any]
    # Existing research data fields
    financial_data: Dict[str, Any]
    news_data: Dict[str, Any]
    industry_data: Dict[str, Any]
    company_data: Dict[str, Any]
    flw_data: Dict[str, Any] # <-- ADDED: Data collected by FLWAnalyzer
    # Existing curated data fields
    curated_financial_data: Dict[str, Any]
    curated_news_data: Dict[str, Any]
    curated_industry_data: Dict[str, Any]
    curated_company_data: Dict[str, Any]
    curated_flw_data: Dict[str, Any] # <-- ADDED: Curated data for FLW
    # Existing briefing fields
    financial_briefing: str
    news_briefing: str
    industry_briefing: str
    company_briefing: str
    flw_sustainability_briefing: str # <-- ADDED: Briefing for FLW
    # References and supporting info
    references: List[str]
    reference_info: NotRequired[Dict[str, Dict[str, Any]]]
    reference_titles: NotRequired[Dict[str, str]]
    # Other state fields
    briefings: Dict[str, Any] # Dictionary to hold all generated briefings
    report: str
    # Airtable tag fields
    airtable_industries: NotRequired[List[str]]
    airtable_country_region: NotRequired[List[str]]
    airtable_revenue_band_est: NotRequired[List[str]]
    # Error field (optional)
    error: NotRequired[str]
    # Current node (for WS updates)
    current_node: NotRequired[str]