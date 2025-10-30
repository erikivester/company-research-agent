# test_airtable.py
import os
import asyncio
import logging
from datetime import datetime
from dotenv import load_dotenv
from typing import cast # Import cast for type hinting if needed

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Load Environment Variables ---
dotenv_path = os.path.join(os.path.dirname(__file__), '.env') # Assumes script is in root
if not os.path.exists(dotenv_path):
     dotenv_path = os.path.join(os.path.dirname(__file__), '../.env') # Assumes script is in tests/

if os.path.exists(dotenv_path):
    logger.info(f"Loading environment variables from: {dotenv_path}")
    load_dotenv(dotenv_path=dotenv_path)
else:
    logger.warning(".env file not found, relying on system environment variables.")

# Check if keys are loaded
if not os.getenv("OPENAI_API_KEY"):
    logger.warning("OPENAI_API_KEY not found in environment.")
if not os.getenv("GEMINI_API_KEY"):
    logger.warning("GEMINI_API_KEY not found in environment.")
if not os.getenv("AIRTABLE_API_KEY"):
    logger.warning("AIRTABLE_API_KEY not found in environment.")
if not os.getenv("AIRTABLE_BASE_ID"):
    logger.warning("AIRTABLE_BASE_ID not found in environment.")
if not os.getenv("AIRTABLE_TABLE_NAME"):
    logger.warning("AIRTABLE_TABLE_NAME not found in environment.")
if not os.getenv("TAVILY_API_KEY"):
    logger.warning("TAVILY_API_KEY not found in environment.")


# --- Import Required Project Components ---
try:
    from backend.classes.state import ResearchState
    from backend.nodes.tagger import Tagger
    from backend.airtable_uploader import upload_to_airtable
    from backend.utils.references import format_references_section
    from langchain_core.messages import AIMessage
except ImportError as e:
    logger.error(f"ImportError: {e}. Make sure your Python path includes the project root.")
    exit(1)

# --- v2: Create Mock State (Updated) ---
# Represents state *after* briefing node, *before* tagger node.
mock_state_before_tagger: ResearchState = {
    'company': 'Whole Foods Market', # Updated to match your record
    'company_url': 'https://www.wholefoodsmarket.com',
    'hq_location': 'Austin, TX',
    'industry': 'Grocery & Food Retail',
    'job_id': f'test-job-{datetime.now().strftime("%Y%m%d%H%M%S")}',
    
    # --- Set to a REAL Airtable Record ID to test UPDATE ---
    # --- Set to None to test INSERT ---
    'airtable_record_id': 'recGCrYQTWOYFpmGY', 
    
    'messages': [AIMessage(content="Simulated initial message"), AIMessage(content="Simulated curation message"), AIMessage(content="Simulated briefing message")],

    # --- v2: Sample Briefing Content (Updated to 5 new keys) ---
    'company_brief_briefing': """### Core Business
* Sells natural and organic foods, free from artificial preservatives, colors, and flavors.
### Financial Health
* Acquired by Amazon in 2017.
* Reports strong revenue as part of Amazon's physical stores segment.
* Recently announced 5% price cuts on various items.
""",
    'news_signal_briefing': """* **FLW/Climate Signal:** Whole Foods 2024 Impact Report highlights a 30% reduction in food waste intensity since 2020.
* **Opportunity Signal:** Appointed a new 'VP of Community Engagement' in September 2025.
* **General News:** Launched a new "responsibly farmed" seafood certification in October 2025.
""",
    'flw_sustainability_briefing': """### ESG & Methane Goals
* Parent company Amazon targets net-zero carbon by 2040.
* 2024 Impact Report mentions goals to reduce refrigerant emissions (HFCs).
### FLW Initiatives
* Utilizes AI-powered inventory management to reduce over-ordering.
### Food Rescue & Donation
* Long-standing partnership with Food Donation Connection (FDC).
* Reports donating over 30 million meals in 2024 via its 'Nourishing Our Neighborhoods' program.
### Recycling & Resource Recovery
* Provides composting and recycling for back-of-house food scraps at most stores.
""",
    'contact_briefing': """### Key Contacts
* **Jane Smith:** Senior Manager, Global Sustainability - Leads ESG reporting and waste reduction programs.
* **John Doe:** Director, Community Giving & Partnerships - Manages the 'Nourishing Our Neighborhoods' food donation program.
""",
    'engagement_briefing': """### Engagements & Affiliations
* **Membership:** Signatory of the U.S. Food Waste Pact.
* **Award:** Named one of Fast Company's "Brands That Matter" in 2024.
* **Partnership:** Works with Food Donation Connection (FDC) for food rescue logistics.
""",
    # --- End v2 Briefing Content ---

    'references': ["https://www.wholefoodsmarket.com/mission-values/sustainability"],
    'reference_info': {
         "https://www.wholefoodsmarket.com/mission-values/sustainability": {"title": "Sustainability", "website": "Wholefoodsmarket", "domain": "wholefoodsmarket.com", "score": 0.9, "url": "https://www.wholefoodsmarket.com/mission-values/sustainability"}
    },
    'reference_titles': {
         "https://www.wholefoodsmarket.com/mission-values/sustainability": "Sustainability at Whole Foods"
    },

    'report': """# Whole Foods Market Research Report (Raw)
## Company Overview & Financial Health
### Core Business
* Sells natural and organic foods...
### Financial Health
* Acquired by Amazon in 2017...
## FLW & Sustainability
### ESG & Methane Goals
* Parent company Amazon targets net-zero carbon by 2040...
### FLW Initiatives
* Utilizes AI-powered inventory management...
### Food Rescue & Donation
* Long-standing partnership with Food Donation Connection...
## News & Signals
* **FLW/Climate Signal:** Whole Foods 2024 Impact Report highlights...
* **Opportunity Signal:** Appointed a new 'VP of Community Engagement'...
## Engagements & Affiliations
### Engagements & Affiliations
* **Membership:** Signatory of the U.S. Food Waste Pact.
## Potential Contacts
### Key Contacts
* **Jane Smith:** Senior Manager, Global Sustainability...
## References
* Wholefoodsmarket. "Sustainability." https://www.wholefoodsmarket.com/mission-values/sustainability
""",
    'briefings': {
        "company_brief": "## Company Overview...",
        "news_signal": "* **FLW/Climate Signal:**...",
        "flw": "### ESG & Methane Goals...",
        "contact": "### Key Contacts...",
        "engagement": "### Engagements & Affiliations..."
        },
    
    # --- v2: Mock fields no longer in state (for completeness) ---
    'financial_data': {}, 'news_data': {}, 'industry_data': {}, 'company_data': {},
    'curated_financial_data': {}, 'curated_news_data': {}, 'curated_industry_data': {}, 'curated_company_data': {},
    'financial_briefing': "", 'industry_briefing': "", 'company_briefing': "", 'news_briefing': ""
}

async def main_test(record_id_override: str = None):
    global mock_state_before_tagger 

    # Override the mock state's record_id if one is passed (e.g., from the API)
    if record_id_override:
        logger.info(f"Overriding mock record_id with provided ID: {record_id_override}")
        mock_state_before_tagger['airtable_record_id'] = record_id_override
    
    # --- 1. Test Tagger ---
    logger.info("--- Testing Tagger Node (v2) ---")
    try:
        tagger = Tagger()
        state_after_tagger = await tagger.run(cast(ResearchState, mock_state_before_tagger.copy()))

        logger.info("State inspection after Tagger run:")
        industries = state_after_tagger.get('airtable_industries')
        region = state_after_tagger.get('airtable_country_region')
        revenue = state_after_tagger.get('airtable_revenue_band_est')
        alignment = state_after_tagger.get('airtable_refed_alignment') # <-- v2: Check new field

        logger.info(f"  > Industries Found: {industries} (Type: {type(industries)})")
        logger.info(f"  > Region Found: {region} (Type: {type(region)})")
        logger.info(f"  > Revenue Found: {revenue} (Type: {type(revenue)})")
        logger.info(f"  > ReFED Alignment Found: {alignment} (Type: {type(alignment)})") # <-- v2: Log new field

        if not industries or not region or not revenue or not alignment:
            logger.warning("Tagger did not find all expected classifications. Check Tagger logs/OpenAI response.")
        
        mock_state_before_tagger.update(state_after_tagger)

    except Exception as e:
        logger.error(f"Error running Tagger node: {e}", exc_info=True)
        return

    logger.info("-" * 30)

    # --- 2. Test Airtable Uploader (Simulating graph.py's airtable_upload_node data prep) ---
    logger.info("--- Testing Airtable Upload Logic (v2) ---")
    try:
        state_for_upload = cast(ResearchState, mock_state_before_tagger)
        logger.info("Preparing data for Airtable upload (simulating graph.py)...")

        # --- Simulate Process Notes Generation ---
        process_notes_test = []
        for msg in state_for_upload.get("messages", []):
             content = getattr(msg, 'content', '')
             if isinstance(content, str):
                 process_notes_test.append(content)
        if not process_notes_test:
            process_notes_test = [f"Test Upload on {datetime.now().isoformat()}"]
        process_notes_str = "\n".join(process_notes_test)[:10000]
        logger.info(f"Simulated Process Notes generated ({len(process_notes_str)} chars).")

        # --- Simulate Reference Formatting ---
        references_str_test = ""
        references_list_test = state_for_upload.get("references", [])
        if references_list_test:
            ref_info = state_for_upload.get("reference_info", {})
            ref_titles = state_for_upload.get("reference_titles", {})
            try:
                references_str_test = format_references_section(references_list_test, ref_info, ref_titles)
                references_str_test = references_str_test.replace("## References\n", "").strip()[:10000]
                logger.info(f"References formatted for upload ({len(references_str_test)} chars).")
            except Exception as ref_e:
                logger.error(f"Error formatting references for upload test: {ref_e}")
                references_str_test = "[Error formatting references]"
        else:
             logger.info("No references found in state to format.")

        # --- v2: Prepare report_data dict (keys MUST match graph.py's airtable_upload_node) ---
        revenue_tag_list = state_for_upload.get("airtable_revenue_band_est", [])
        revenue_tag = revenue_tag_list[0] if isinstance(revenue_tag_list, list) and revenue_tag_list else None

        report_data_for_upload = {
            "company_name": state_for_upload.get("company"),
            "company_url": state_for_upload.get("company_url"),
            "report_markdown": state_for_upload.get("report"),
            
            # --- v2 Briefings ---
            "company_brief_briefing": state_for_upload.get("company_brief_briefing"),
            "news_signal_briefing": state_for_upload.get("news_signal_briefing"),
            "flw_sustainability_briefing": state_for_upload.get("flw_sustainability_briefing"),
            "contact_briefing": state_for_upload.get("contact_briefing"),
            "engagement_briefing": state_for_upload.get("engagement_briefing"),
            
            # --- Process & References ---
            "process_notes": process_notes_str,
            "references_formatted": references_str_test,
            
            # --- v2 Tags ---
            "industries_tags": state_for_upload.get("airtable_industries", []),
            "region_tags": state_for_upload.get("airtable_country_region", []),
            "revenue_tags": revenue_tag,
            "refed_alignment_tags": state_for_upload.get("airtable_refed_alignment", [])
        }
        # --- END v2 REVISION ---

        # Log prepared data keys/values (excluding long text fields)
        loggable_report_data = {k: v for k, v in report_data_for_upload.items() if not isinstance(v, str) or len(v) < 100}
        logger.info(f"Data prepared *for input to* upload function: {loggable_report_data}")

        # Call the uploader function directly
        upload_result = upload_to_airtable(
            report_data=report_data_for_upload,
            job_id=state_for_upload.get("job_id", "test-job-error"),
            record_id=state_for_upload.get("airtable_record_id")
        )

        logger.info("\nAirtable Upload Function Result:")
        logger.info(upload_result)

        if upload_result.get("status") != "Success":
            logger.error(f"Airtable upload/update failed: {upload_result.get('error')}")
        else:
            logger.info(f"Airtable operation successful. Record ID: {upload_result.get('airtable_record_id')}")
            if state_for_upload.get("airtable_record_id") is None:
                logger.info(f"^^^ NOTE: Test performed an INSERT. To test UPDATE, set 'airtable_record_id' in mock_state_before_tagger to '{upload_result.get('airtable_record_id')}' and rerun.")
            else:
                logger.info(f"^^^ NOTE: Test performed an UPDATE on record {state_for_upload.get('airtable_record_id')}.")


    except Exception as e:
        logger.error(f"Error running Airtable upload logic: {e}", exc_info=True)
        return {"status": "Failure", "error": str(e)}

    logger.info("-" * 30)
    logger.info("Test finished.")
    return upload_result


if __name__ == "__main__":
    required_keys = ["OPENAI_API_KEY", "GEMINI_API_KEY", "AIRTABLE_API_KEY", "AIRTABLE_BASE_ID", "AIRTABLE_TABLE_NAME", "TAVILY_API_KEY"]
    missing_keys = [key for key in required_keys if not os.getenv(key)]

    if missing_keys:
        print(f"\nERROR: Missing required environment variables: {', '.join(missing_keys)}")
        print("Please ensure your .env file is correctly set up and loaded.\n")
    else:
        # Pass the record ID from an environment variable if you want to
        # e.g., TEST_RECORD_ID='recGCrYQTWOYFpmGY' python test_airtable.py
        record_id_from_env = os.getenv("TEST_RECORD_ID")
        asyncio.run(main_test(record_id_override=record_id_from_env))