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
    google_drive_folder_url: NotRequired[str] # <-- ADDED for v2

class ResearchState(InputState):
    site_scrape: Dict[str, Any]
    messages: List[Any]
    
    # --- v2 Research Data Fields ---
    company_brief_data: Dict[str, Any]         # RENAMED from company_data
    news_signal_data: Dict[str, Any]         # RENAMED from news_data
    flw_data: Dict[str, Any]                 # KEPT
    contact_finder_data: Dict[str, Any]      # NEW
    engagement_finder_data: Dict[str, Any]   # NEW
    # REMOVED: financial_data, industry_data

    # --- v2 Curated Data Fields ---
    curated_company_brief_data: Dict[str, Any] # RENAMED from curated_company_data
    curated_news_signal_data: Dict[str, Any] # RENAMED from curated_news_data
    curated_flw_data: Dict[str, Any]         # KEPT
    curated_contact_finder_data: Dict[str, Any]      # NEW
    curated_engagement_finder_data: Dict[str, Any]   # NEW
    # REMOVED: curated_financial_data, curated_industry_data
    
    # --- v2 Briefing Fields ---
    company_brief_briefing: str        # RENAMED from company_briefing
    news_signal_briefing: str        # RENAMED from news_briefing
    flw_sustainability_briefing: str # KEPT
    contact_briefing: str              # NEW
    engagement_briefing: str           # NEW
    # REMOVED: financial_briefing, industry_briefing

    # References and supporting info (Unchanged)
    references: List[str]
    reference_info: NotRequired[Dict[str, Dict[str, Any]]]
    reference_titles: NotRequired[Dict[str, str]]
    
    # Other state fields (Unchanged)
    briefings: Dict[str, Any] # Dictionary to hold all generated briefings
    report: str
    
    # --- v2 Airtable Tag Fields ---
    airtable_industries: NotRequired[List[str]]
    airtable_country_region: NotRequired[List[str]]
    airtable_revenue_band_est: NotRequired[List[str]]
    airtable_refed_alignment: NotRequired[List[str]] # NEW
    
    # Error field (optional)
    error: NotRequired[str]
    
    # Current node (for WS updates)
    current_node: NotRequired[str]
}