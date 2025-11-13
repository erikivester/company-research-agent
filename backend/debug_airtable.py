# backend/debug_airtable.py
import logging
from typing import cast

from langchain_core.messages import AIMessage

from backend.classes.state import ResearchState
from backend.graph import Graph  # Use Graph for simplified airtable_upload_node access
from backend.nodes.tagger import Tagger

logger = logging.getLogger(__name__)


# --- Mock WebSocket Manager for testing nodes outside the main flow ---
class DummyWebSocketManager:
    async def send_status_update(
        self, job_id, status, message, result=None, error=None
    ):
        logger.info(f"DUMMY WS: Job {job_id}, Status: {status}, Message: {message}")
        pass  # Do nothing


# --- MOCK STATE SETUP (CORRECTED FOR V2) ---
mock_state_before_tagger: ResearchState = {
    "company": "Sustainable Foods Inc.",
    "company_url": "https://www.sustainablefoods.example",
    "hq_location": "Austin, TX",
    "industry": "Food & Beverage Manufacturing",  # Industry Hint
    "job_id": "test-job-debug-api-1",
    "airtable_record_id": None,  # Set to None to test INSERT
    "google_drive_folder_url": "https://drive.google.com/drive/folders/12PPnCJhI3Z1ZaHXxKqSSLs7Nr0De9w7J",  # Or your folder URL
    "messages": [
        AIMessage(content="Simulated initial message"),
        AIMessage(content="Simulated curation message"),
        AIMessage(content="Simulated briefing message"),
    ],
    # --- V2 BRIEFING KEYS (Corrected) ---
    # Tagger uses these keys
    "company_brief_briefing": """## Company Overview & Financial Health
* Sustainable Foods Inc. is a food manufacturer focused on plant-based alternatives.
* Estimated Annual Revenue: $35 million
* Seed round: $5 million (June 2023)
""",
    "flw_sustainability_briefing": """## FLW and Sustainability
### FLW Initiatives & Reduction Efforts
* Stated goal to reduce food waste by 50% by 2030.
* Operates in the Food & Beverage Manufacturing sector.
""",
    "news_signal_briefing": """## News & Signals
* **General News**: Launched new vegan sausage product (Jan 2024)
* **ReFED Signal**: Mentioned in a panel on food waste reduction.
""",
    "engagement_briefing": "## Engagements & Affiliations\n* Member of the Plant-Based Foods Association.",
    "contact_briefing": "## Potential Contacts\n* Jane Doe: Sustainability Manager",
    # --- END V2 ---
    "report": """# Sustainable Foods Inc. Research Report
# ... (omitted report markdown for brevity)
## References
* Sustainablefoods. "Sustainability at Sustainable Foods Inc." https://www.sustainablefoods.example/sustainability
""",
    "references": ["https://www.sustainablefoods.example/sustainability"],
    "reference_info": {
        "https://www.sustainablefoods.example/sustainability": {
            "title": "Sustainability Efforts",
            "website": "Sustainablefoods",
            "domain": "sustainablefoods.example",
            "score": 0.9,
            "url": "https://www.sustainablefoods.example/sustainability",
        }
    },
    "reference_titles": {
        "https://www.sustainablefoods.example/sustainability": "Sustainability at Sustainable Foods Inc."
    },
    "briefings": {},  # This is a holder, not used by Tagger
}


# --- EXPORTED TEST FUNCTION ---
async def run_airtable_debug_test(record_id: str | None = None):
    """Runs the Tagger and Airtable Upload logic directly."""
    logger.info("--- Starting Airtable Debug Test via API Endpoint ---")

    state = cast(ResearchState, mock_state_before_tagger.copy())
    if record_id:
        state["airtable_record_id"] = record_id
        state["job_id"] = f"test-job-debug-api-UPDATE-{record_id}"
    else:
        state["job_id"] = "test-job-debug-api-INSERT"

    # 1. Simulate Tagger Run (Classification)
    try:
        tagger = Tagger()
        state = await tagger.run(state)
        logger.info(f"Tagger finished. Tags: {state.get('airtable_industries')}")
    except Exception as e:
        logger.error(f"Tagger failed in debug run: {e}")
        # Continue to upload even on Tagger failure to test upload logic
        state.setdefault("airtable_industries", ["Unknown"])
        state.setdefault("airtable_country_region", ["Unknown"])
        state.setdefault("airtable_revenue_band_est", ["Unknown"])
        state.setdefault("airtable_refed_alignment", [])  # v2: Added alignment default

    # 2. Call the dedicated upload node function
    try:

        class MockGraph:
            async def airtable_upload_node(self, state):
                # Directly call the actual upload logic
                return await Graph.airtable_upload_node(None, state)

        state["websocket_manager"] = DummyWebSocketManager()

        graph_instance = MockGraph()
        final_state = await graph_instance.airtable_upload_node(state)

        result_id = final_state.get("airtable_record_id")

        return {
            "status": "Success",
            "message": "Debug test completed.",
            "airtable_record_id": result_id,
            "tags": {
                "industries": final_state.get("airtable_industries"),
                "revenue": final_state.get("airtable_revenue_band_est"),
                "alignment": final_state.get(
                    "airtable_refed_alignment"
                ),  # v2: Added alignment to output
            },
        }

    except Exception as e:
        logger.error(f"Airtable upload node failed in debug run: {e}")
        return {"status": "Failure", "message": f"Airtable upload failed: {str(e)}"}
