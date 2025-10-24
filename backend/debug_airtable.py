# backend/debug_airtable.py
import asyncio
import logging
from typing import cast

from backend.classes.state import ResearchState
from backend.nodes.tagger import Tagger
from backend.graph import Graph # Use Graph for simplified airtable_upload_node access
from backend.services.websocket_manager import WebSocketManager # Mock WebSocket Manager
from langchain_core.messages import AIMessage

logger = logging.getLogger(__name__)

# --- Mock WebSocket Manager for testing nodes outside the main flow ---
# We use a dummy manager since the test environment isn't expected to have a live WS connection
class DummyWebSocketManager:
    async def send_status_update(self, job_id, status, message, result=None, error=None):
        logger.info(f"DUMMY WS: Job {job_id}, Status: {status}, Message: {message}")
        pass # Do nothing

# --- MOCK STATE SETUP (Copy from test_airtable.py) ---
mock_state_before_tagger: ResearchState = {
    'company': 'Sustainable Foods Inc.',
    'company_url': 'https://www.sustainablefoods.example',
    'hq_location': 'Austin, TX',
    'industry': 'Food & Beverage Manufacturing',
    'job_id': 'test-job-debug-api-1',
    'airtable_record_id': None, # Set to None to test INSERT, or a real ID to test UPDATE
    'messages': [
        AIMessage(content="Simulated initial message"), 
        AIMessage(content="Simulated curation message"), 
        AIMessage(content="Simulated briefing message")
    ],
    'financial_briefing': "## Financial Overview\n### Funding & Investment\n* Estimated Annual Revenue: $35 million\n* Seed round: $5 million (June 2023)",
    'company_briefing': "## Company Overview\nSustainable Foods Inc. is a food manufacturer focused on plant-based alternatives...",
    'industry_briefing': "## Industry Overview\n### Market Overview\n* Operates in the plant-based meat alternative market.",
    'news_briefing': "## News\n* **Major Announcements**: Launched new vegan sausage product (Jan 2024)",
    'flw_sustainability_briefing': """## FLW and Sustainability
### FLW Initiatives & Reduction Efforts
* Stated goal to reduce food waste by 50% by 2030.
""",
    'report': """# Sustainable Foods Inc. Research Report
# ... (omitted report markdown for brevity)
## References
* Sustainablefoods. "Sustainability at Sustainable Foods Inc." https://www.sustainablefoods.example/sustainability
""",
    'references': ["https://www.sustainablefoods.example/sustainability"],
    'reference_info': {
         "https://www.sustainablefoods.example/sustainability": {"title": "Sustainability Efforts", "website": "Sustainablefoods", "domain": "sustainablefoods.example", "score": 0.9, "url": "https://www.sustainablefoods.example/sustainability"}
    },
    'reference_titles': {
         "https://www.sustainablefoods.example/sustainability": "Sustainability at Sustainable Foods Inc."
    },
    'briefings': {}
}

# --- EXPORTED TEST FUNCTION ---
async def run_airtable_debug_test(record_id: str | None = None):
    """Runs the Tagger and Airtable Upload logic directly."""
    logger.info("--- Starting Airtable Debug Test via API Endpoint ---")
    
    state = cast(ResearchState, mock_state_before_tagger.copy())
    if record_id:
        state['airtable_record_id'] = record_id
        state['job_id'] = f'test-job-debug-api-UPDATE-{record_id}'
    else:
        state['job_id'] = 'test-job-debug-api-INSERT'
    
    # 1. Simulate Tagger Run (Classification)
    try:
        tagger = Tagger()
        state = await tagger.run(state)
        logger.info(f"Tagger finished. Tags: {state.get('airtable_industries')}")
    except Exception as e:
        logger.error(f"Tagger failed in debug run: {e}")
        # Continue to upload even on Tagger failure to test upload logic
        state.setdefault('airtable_industries', ['Unknown'])
        state.setdefault('airtable_country_region', ['Unknown'])
        state.setdefault('airtable_revenue_band_est', ['Unknown'])

    # 2. Call the dedicated upload node function (must mock its parent class methods)
    try:
        # Since Graph.airtable_upload_node expects a complete state, we pass it.
        # It handles all final data preparation and calls update_to_airtable.
        # We need a dummy Graph instance to call the node function directly.
        
        # Instantiate a minimal Graph class to access the node function
        class MockGraph:
            async def airtable_upload_node(self, state):
                 # Directly call the actual upload logic
                 return await Graph.airtable_upload_node(Graph(), state)

        # We must manually inject websocket_manager for graph.py's node
        state['websocket_manager'] = DummyWebSocketManager()
        
        # Call the upload node function
        graph_instance = MockGraph()
        final_state = await graph_instance.airtable_upload_node(state)
        
        result_id = final_state.get('airtable_record_id')
        
        return {
            "status": "Success",
            "message": "Debug test completed.",
            "airtable_record_id": result_id,
            "tags": {
                 "industries": final_state.get('airtable_industries'),
                 "revenue": final_state.get('airtable_revenue_band_est'),
            }
        }
    
    except Exception as e:
        logger.error(f"Airtable upload node failed in debug run: {e}")
        return {"status": "Failure", "message": f"Airtable upload failed: {str(e)}"}