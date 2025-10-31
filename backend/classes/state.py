# Inside backend/classes/state.py
from typing import TypedDict, NotRequired, Required, Dict, List, Any, Annotated
import operator
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
    google_drive_folder_url: NotRequired[str] 

# --- UPDATED ResearchState ---

# This is the correct reducer function for pass-through keys
# It just takes the first value (a) and ignores the second (b)
# since they are identical in all parallel branches.
def _first_value(a, b):
    return a

class ResearchState(TypedDict):
    # --- Input fields (pass-through) ---
    # Use Annotated to tell LangGraph how to merge these when parallel branches join.
    # We now use our correct _first_value reducer
    company: Annotated[str, _first_value]
    company_url: Annotated[str, _first_value]
    hq_location: Annotated[str, _first_value]
    industry: Annotated[str, _first_value]
    websocket_manager: Annotated[WebSocketManager, _first_value]
    job_id: Annotated[str, _first_value]
    airtable_record_id: Annotated[str, _first_value]
    google_drive_folder_url: Annotated[str, _first_value]

    # --- Grounding node output (pass-through) ---
    site_scrape: Annotated[Dict[str, Any], _first_value]
    
    # --- Messages (merge strategy: add lists together, this is correct) ---
    messages: Annotated[List[Any], operator.add]
    
    # --- v2 Research Data Fields (written by individual nodes) ---
    # These don't need an annotation because only one node writes to each.
    company_brief_data: NotRequired[Dict[str, Any]]
    news_signal_data: NotRequired[Dict[str, Any]]
    flw_data: NotRequired[Dict[str, Any]]
    contact_finder_data: NotRequired[Dict[str, Any]]
    engagement_finder_data: NotRequired[Dict[str, Any]]

    # --- v2 Curated Data Fields ---
    curated_company_brief_data: NotRequired[Dict[str, Any]]
    curated_news_signal_data: NotRequired[Dict[str, Any]]
    curated_flw_data: NotRequired[Dict[str, Any]]
    curated_contact_finder_data: NotRequired[Dict[str, Any]]
    curated_engagement_finder_data: NotRequired[Dict[str, Any]]
    
    # --- v2 Briefing Fields ---
    company_brief_briefing: NotRequired[str]
    news_signal_briefing: NotRequired[str]
    flw_sustainability_briefing: NotRequired[str]
    contact_briefing: NotRequired[str]
    engagement_briefing: NotRequired[str]

    # References and supporting info
    references: NotRequired[List[str]]
    reference_info: NotRequired[Dict[str, Dict[str, Any]]]
    reference_titles: NotRequired[Dict[str, str]]
    
    # Other state fields
    briefings: NotRequired[Dict[str, Any]]
    report: NotRequired[str]
    
    # --- v2 Airtable Tag Fields ---
    airtable_industries: NotRequired[List[str]]
    airtable_country_region: NotRequired[List[str]]
    airtable_revenue_band_est: NotRequired[List[str]]
    airtable_refed_alignment: NotRequired[List[str]]
    
    # Error field (optional)
    error: NotRequired[str]
    
    # Current node (for WS updates)
    current_node: NotRequired[str]