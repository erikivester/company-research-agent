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
# Assumes .env file is in the project root relative to this script if run from root
# Or in the parent directory if script is in a 'tests' subdirectory
dotenv_path = os.path.join(os.path.dirname(__file__), '../.env') # Assumes script is in tests/
if not os.path.exists(dotenv_path):
    dotenv_path = os.path.join(os.path.dirname(__file__), '.env') # Assumes script is in root

if os.path.exists(dotenv_path):
    logger.info(f"Loading environment variables from: {dotenv_path}")
    load_dotenv(dotenv_path=dotenv_path)
else:
    logger.warning(".env file not found, relying on system environment variables.")

# Check if keys are loaded
if not os.getenv("OPENAI_API_KEY"):
    logger.warning("OPENAI_API_KEY not found in environment.")
if not os.getenv("GEMINI_API_KEY"): # Added check for Gemini key
    logger.warning("GEMINI_API_KEY not found in environment.")
if not os.getenv("AIRTABLE_API_KEY"):
    logger.warning("AIRTABLE_API_KEY not found in environment.")
if not os.getenv("AIRTABLE_BASE_ID"):
    logger.warning("AIRTABLE_BASE_ID not found in environment.")
if not os.getenv("AIRTABLE_TABLE_NAME"):
    logger.warning("AIRTABLE_TABLE_NAME not found in environment.")
# Note: TAVILY_API_KEY is also needed for the full flow but maybe not strictly for tagger/uploader test
if not os.getenv("TAVILY_API_KEY"):
    logger.warning("TAVILY_API_KEY not found in environment.")


# --- Import Required Project Components ---
try:
    # Adjusted imports based on typical structure where tests might be outside backend
    from backend.classes.state import ResearchState
    from backend.nodes.tagger import Tagger
    from backend.airtable_uploader import upload_to_airtable
    from backend.utils.references import format_references_section
    from langchain_core.messages import AIMessage
except ImportError as e:
    logger.error(f"ImportError: {e}. Make sure your Python path includes the project root.")
    logger.error("Try running 'python tests/test_airtable.py' from the root directory.")
    exit(1)

# --- Create Mock State ---
# Represents state *after* briefing node, *before* tagger node.
# Includes sample FLW data and briefing.
mock_state_before_tagger: ResearchState = {
    'company': 'Sustainable Foods Inc.',
    'company_url': 'https://www.sustainablefoods.example',
    'hq_location': 'Austin, TX',
    'industry': 'Food & Beverage Manufacturing',
    'job_id': 'test-job-flw-003',
    # --- IMPORTANT: Set this to a REAL Airtable Record ID to test UPDATE, otherwise leave as None to test INSERT ---
    'airtable_record_id': None, # e.g., 'recXXXXXXXXXXXXXX'
    # ---
    'messages': [AIMessage(content="Simulated initial message"), AIMessage(content="Simulated curation message"), AIMessage(content="Simulated briefing message")],

    'site_scrape': {
        'https://www.sustainablefoods.example/sustainability': {'raw_content': 'Sustainable Foods Inc. is committed to reducing food waste by 50% by 2030... We partner with Feeding America.'},
        'https://www.sustainablefoods.example/packaging': {'raw_content': 'Our new packaging uses 80% recycled PET...'}
    },

    # --- Sample Research Data (Simplified - Tagger uses briefings) ---
    'financial_data': {}, 'news_data': {}, 'industry_data': {}, 'company_data': {},
    'flw_data': {
         'https://www.sustainablefoods.example/sustainability': {'url': 'https://www.sustainablefoods.example/sustainability', 'raw_content': '...reducing food waste...', 'score': 0.9, 'source': 'company_website'},
         'https://envnews.example/sfi_report': {'url': 'https://envnews.example/sfi_report', 'raw_content': 'Sustainable Foods Inc. released its annual impact report...', 'score': 0.8, 'source': 'web_search'}
    },

    # --- Sample Curated Data (Simplified - Tagger uses briefings) ---
    'curated_financial_data': {}, 'curated_news_data': {}, 'curated_industry_data': {}, 'curated_company_data': {},
    'curated_flw_data': {
        'https://www.sustainablefoods.example/sustainability': {'url': 'https://www.sustainablefoods.example/sustainability', 'raw_content': 'Sustainable Foods Inc. is committed to reducing food waste by 50% by 2030... We partner with Feeding America.', 'score': 0.9, 'evaluation': {'overall_score': 0.9}, 'source': 'company_website', 'doc_type': 'flw'},
    },

    # --- Sample Briefing Content ---
    'financial_briefing': "## Financial Overview\n### Funding & Investment\n* Seed round: $5 million (June 2023)\n* Investors: Green Ventures",
    'company_briefing': "## Company Overview\nSustainable Foods Inc. is a food manufacturer focused on plant-based alternatives...\n### Core Product/Service\n* Plant-Based Burger Patties\n* Vegan Sausage Links",
    'industry_briefing': "## Industry Overview\n### Market Overview\n* Operates in the plant-based meat alternative market.",
    'news_briefing': "## News\n* **Major Announcements**: Launched new vegan sausage product (Jan 2024)\n* **Partnerships**: Partnered with National Grocers chain (Dec 2023)",
    'flw_sustainability_briefing': """## FLW and Sustainability
### FLW Initiatives & Reduction Efforts
* Stated goal to reduce food waste by 50% by 2030.
* Partners with Feeding America for food donations.
### Packaging
* Uses packaging with 80% recycled PET content.
""",

    # --- Sample Reference Data ---
    'references': ["https://www.sustainablefoods.example/sustainability"],
    'reference_info': {
         "https://www.sustainablefoods.example/sustainability": {"title": "Sustainability Efforts", "website": "Sustainablefoods", "domain": "sustainablefoods.example", "score": 0.9, "url": "https://www.sustainablefoods.example/sustainability"}
    },
    'reference_titles': {
         "https://www.sustainablefoods.example/sustainability": "Sustainability at Sustainable Foods Inc."
    },

    # --- Sample Final Report (Before Tagger/Upload) ---
    'report': """# Sustainable Foods Inc. Research Report

## Company Overview
Sustainable Foods Inc. is a food manufacturer focused on plant-based alternatives...
### Core Product/Service
* Plant-Based Burger Patties
* Vegan Sausage Links

## Industry Overview
### Market Overview
* Operates in the plant-based meat alternative market.

## Financial Overview
### Funding & Investment
* Seed round: $5 million (June 2023)
* Investors: Green Ventures

## FLW and Sustainability
### FLW Initiatives & Reduction Efforts
* Stated goal to reduce food waste by 50% by 2030.
* Partners with Feeding America for food donations.
### Packaging
* Uses packaging with 80% recycled PET content.

## News
* **Major Announcements**: Launched new vegan sausage product (Jan 2024)
* **Partnerships**: Partnered with National Grocers chain (Dec 2023)

## References
* Sustainablefoods. "Sustainability at Sustainable Foods Inc." https://www.sustainablefoods.example/sustainability
""",

    # --- Fields needed by type hints but maybe not used directly here ---
    'briefings': {
        "financial": "## Financial Overview\n...",
        "company": "## Company Overview\n...",
        "industry": "## Industry Overview\n...",
        "news": "## News\n...",
        "flw": "## FLW and Sustainability\n..."
        },
}

async def main_test():
    global mock_state_before_tagger # Allow modification

    # --- 1. Test Tagger ---
    logger.info("--- Testing Tagger Node ---")
    try:
        tagger = Tagger()
        state_after_tagger = await tagger.run(cast(ResearchState, mock_state_before_tagger.copy()))

        logger.info("State inspection after Tagger run:")
        industries = state_after_tagger.get('airtable_industries')
        region = state_after_tagger.get('airtable_country_region')
        revenue = state_after_tagger.get('airtable_revenue_band_est')

        logger.info(f"  > Industries Found: {industries} (Type: {type(industries)})")
        logger.info(f"  > Region Found: {region} (Type: {type(region)})")
        logger.info(f"  > Revenue Found: {revenue} (Type: {type(revenue)})")

        if not industries or not region or not revenue:
            logger.warning("Tagger did not find all expected classifications. Check Tagger logs/OpenAI response.")
        if not isinstance(industries, list) or not isinstance(region, list) or not isinstance(revenue, list):
             logger.warning("One or more tagger outputs are not lists. Check Tagger node logic.")

        mock_state_before_tagger.update(state_after_tagger)

    except Exception as e:
        logger.error(f"Error running Tagger node: {e}", exc_info=True)
        return

    logger.info("-" * 30)

    # --- 2. Test Airtable Uploader (Simulating graph.py's airtable_upload_node data prep) ---
    logger.info("--- Testing Airtable Upload Logic ---")
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

        # --- Prepare report_data dict (keys MUST match expected INPUT keys for upload_to_airtable) ---
        revenue_tag_list = state_for_upload.get("airtable_revenue_band_est", [])
        revenue_tag = revenue_tag_list[0] if isinstance(revenue_tag_list, list) and revenue_tag_list else None

        # --- REVISED report_data_for_upload ---
        # Use keys expected by the upload_to_airtable function's internal mapping
        report_data_for_upload = {
            "company_name": state_for_upload.get("company"),             # Use internal key
            "company_url": state_for_upload.get("company_url"),           # Use internal key
            "report_markdown": state_for_upload.get("report"),            # Use internal key
            "financial_briefing": state_for_upload.get("financial_briefing"), # Use internal key
            "industry_briefing": state_for_upload.get("industry_briefing"),   # Use internal key
            "company_briefing": state_for_upload.get("company_briefing"),     # Use internal key
            "news_briefing": state_for_upload.get("news_briefing"),         # Use internal key
            "flw_sustainability_briefing": state_for_upload.get("flw_sustainability_briefing"), # Use internal key for FLW
            "process_notes": process_notes_str,                       # Use prepared string
            "references_formatted": references_str_test,              # Use prepared string
            # Pass the tag data using the keys the function expects for mapping
            "industries_tags": state_for_upload.get("airtable_industries", []),
            "region_tags": state_for_upload.get("airtable_country_region", []),
            "revenue_tags": revenue_tag # Already extracted correctly
        }
        # --- END REVISION ---

        # Log prepared data keys/values (excluding long text fields)
        loggable_report_data = {k: v for k, v in report_data_for_upload.items() if k not in [
            'report_markdown', 'process_notes', 'references_formatted',
            'financial_briefing', 'industry_briefing', 'company_briefing',
            'news_briefing', 'flw_sustainability_briefing'
            ]}
        # Update log message to reflect these are *input* keys now
        logger.info(f"Data prepared *for input to* upload function: {loggable_report_data}")

        # Call the uploader function directly
        upload_result = upload_to_airtable(
            report_data=report_data_for_upload, # Pass the revised dictionary
            job_id=state_for_upload.get("job_id", "test-job-error"),
            record_id=state_for_upload.get("airtable_record_id")
        )

        logger.info("\nAirtable Upload Function Result:")
        logger.info(upload_result)

        if upload_result.get("status") != "Success":
            logger.error(f"Airtable upload/update failed: {upload_result.get('error')}")
        else:
            logger.info(f"Airtable operation successful. Record ID: {upload_result.get('airtable_record_id')}")
            # IMPORTANT reminder on testing INSERT vs UPDATE
            if state_for_upload.get("airtable_record_id") is None:
                logger.info(f"^^^ NOTE: Test performed an INSERT. To test UPDATE, set 'airtable_record_id' in mock_state_before_tagger to '{upload_result.get('airtable_record_id')}' and rerun.")

    except Exception as e:
        logger.error(f"Error running Airtable upload logic: {e}", exc_info=True)

    logger.info("-" * 30)
    logger.info("Test finished.")


if __name__ == "__main__":
    required_keys = ["OPENAI_API_KEY", "GEMINI_API_KEY", "AIRTABLE_API_KEY", "AIRTABLE_BASE_ID", "AIRTABLE_TABLE_NAME", "TAVILY_API_KEY"]
    missing_keys = [key for key in required_keys if not os.getenv(key)]

    if missing_keys:
        print(f"\nERROR: Missing required environment variables: {', '.join(missing_keys)}")
        print("Please ensure your .env file is correctly set up and loaded.\n")
    else:
        asyncio.run(main_test())# test_airtable.py
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
# Assumes .env file is in the project root relative to this script if run from root
# Or in the parent directory if script is in a 'tests' subdirectory
dotenv_path = os.path.join(os.path.dirname(__file__), '../.env') # Assumes script is in tests/
if not os.path.exists(dotenv_path):
    dotenv_path = os.path.join(os.path.dirname(__file__), '.env') # Assumes script is in root

if os.path.exists(dotenv_path):
    logger.info(f"Loading environment variables from: {dotenv_path}")
    load_dotenv(dotenv_path=dotenv_path)
else:
    logger.warning(".env file not found, relying on system environment variables.")

# Check if keys are loaded
if not os.getenv("OPENAI_API_KEY"):
    logger.warning("OPENAI_API_KEY not found in environment.")
if not os.getenv("GEMINI_API_KEY"): # Added check for Gemini key
    logger.warning("GEMINI_API_KEY not found in environment.")
if not os.getenv("AIRTABLE_API_KEY"):
    logger.warning("AIRTABLE_API_KEY not found in environment.")
if not os.getenv("AIRTABLE_BASE_ID"):
    logger.warning("AIRTABLE_BASE_ID not found in environment.")
if not os.getenv("AIRTABLE_TABLE_NAME"):
    logger.warning("AIRTABLE_TABLE_NAME not found in environment.")
# Note: TAVILY_API_KEY is also needed for the full flow but maybe not strictly for tagger/uploader test
if not os.getenv("TAVILY_API_KEY"):
    logger.warning("TAVILY_API_KEY not found in environment.")


# --- Import Required Project Components ---
try:
    # Adjusted imports based on typical structure where tests might be outside backend
    from backend.classes.state import ResearchState
    from backend.nodes.tagger import Tagger
    from backend.airtable_uploader import upload_to_airtable
    from backend.utils.references import format_references_section
    from langchain_core.messages import AIMessage
except ImportError as e:
    logger.error(f"ImportError: {e}. Make sure your Python path includes the project root.")
    logger.error("Try running 'python tests/test_airtable.py' from the root directory.")
    exit(1)

# --- Create Mock State ---
# Represents state *after* briefing node, *before* tagger node.
# Includes sample FLW data and briefing.
mock_state_before_tagger: ResearchState = {
    'company': 'Sustainable Foods Inc.',
    'company_url': 'https://www.sustainablefoods.example',
    'hq_location': 'Austin, TX',
    'industry': 'Food & Beverage Manufacturing',
    'job_id': 'test-job-flw-003',
    # --- IMPORTANT: Set this to a REAL Airtable Record ID to test UPDATE, otherwise leave as None to test INSERT ---
    'airtable_record_id': None, # e.g., 'recXXXXXXXXXXXXXX'
    # ---
    'messages': [AIMessage(content="Simulated initial message"), AIMessage(content="Simulated curation message"), AIMessage(content="Simulated briefing message")],

    'site_scrape': {
        'https://www.sustainablefoods.example/sustainability': {'raw_content': 'Sustainable Foods Inc. is committed to reducing food waste by 50% by 2030... We partner with Feeding America.'},
        'https://www.sustainablefoods.example/packaging': {'raw_content': 'Our new packaging uses 80% recycled PET...'}
    },

    # --- Sample Research Data (Simplified - Tagger uses briefings) ---
    'financial_data': {}, 'news_data': {}, 'industry_data': {}, 'company_data': {},
    'flw_data': {
         'https://www.sustainablefoods.example/sustainability': {'url': 'https://www.sustainablefoods.example/sustainability', 'raw_content': '...reducing food waste...', 'score': 0.9, 'source': 'company_website'},
         'https://envnews.example/sfi_report': {'url': 'https://envnews.example/sfi_report', 'raw_content': 'Sustainable Foods Inc. released its annual impact report...', 'score': 0.8, 'source': 'web_search'}
    },

    # --- Sample Curated Data (Simplified - Tagger uses briefings) ---
    'curated_financial_data': {}, 'curated_news_data': {}, 'curated_industry_data': {}, 'curated_company_data': {},
    'curated_flw_data': {
        'https://www.sustainablefoods.example/sustainability': {'url': 'https://www.sustainablefoods.example/sustainability', 'raw_content': 'Sustainable Foods Inc. is committed to reducing food waste by 50% by 2030... We partner with Feeding America.', 'score': 0.9, 'evaluation': {'overall_score': 0.9}, 'source': 'company_website', 'doc_type': 'flw'},
    },

    # --- Sample Briefing Content ---
    'financial_briefing': "## Financial Overview\n### Funding & Investment\n* Estimated Annual Revenue: $35 million\n* Seed round: $5 million (June 2023)\n* Investors: Green Ventures",
    'company_briefing': "## Company Overview\nSustainable Foods Inc. is a food manufacturer focused on plant-based alternatives...\n### Core Product/Service\n* Plant-Based Burger Patties\n* Vegan Sausage Links",
    'industry_briefing': "## Industry Overview\n### Market Overview\n* Operates in the plant-based meat alternative market.",
    'news_briefing': "## News\n* **Major Announcements**: Launched new vegan sausage product (Jan 2024)\n* **Partnerships**: Partnered with National Grocers chain (Dec 2023)",
    'flw_sustainability_briefing': """## FLW and Sustainability
### FLW Initiatives & Reduction Efforts
* Stated goal to reduce food waste by 50% by 2030.
* Partners with Feeding America for food donations.
### Packaging
* Uses packaging with 80% recycled PET content.
""",

    # --- Sample Reference Data ---
    'references': ["https://www.sustainablefoods.example/sustainability"],
    'reference_info': {
         "https://www.sustainablefoods.example/sustainability": {"title": "Sustainability Efforts", "website": "Sustainablefoods", "domain": "sustainablefoods.example", "score": 0.9, "url": "https://www.sustainablefoods.example/sustainability"}
    },
    'reference_titles': {
         "https://www.sustainablefoods.example/sustainability": "Sustainability at Sustainable Foods Inc."
    },

    # --- Sample Final Report (Before Tagger/Upload) ---
    'report': """# Sustainable Foods Inc. Research Report

## Company Overview
Sustainable Foods Inc. is a food manufacturer focused on plant-based alternatives...
### Core Product/Service
* Plant-Based Burger Patties
* Vegan Sausage Links

## Industry Overview
### Market Overview
* Operates in the plant-based meat alternative market.

## Financial Overview
### Funding & Investment
* Seed round: $5 million (June 2023)
* Investors: Green Ventures

## FLW and Sustainability
### FLW Initiatives & Reduction Efforts
* Stated goal to reduce food waste by 50% by 2030.
* Partners with Feeding America for food donations.
### Packaging
* Uses packaging with 80% recycled PET content.

## News
* **Major Announcements**: Launched new vegan sausage product (Jan 2024)
* **Partnerships**: Partnered with National Grocers chain (Dec 2023)

## References
* Sustainablefoods. "Sustainability at Sustainable Foods Inc." https://www.sustainablefoods.example/sustainability
""",

    # --- Fields needed by type hints but maybe not used directly here ---
    'briefings': {
        "financial": "## Financial Overview\n...",
        "company": "## Company Overview\n...",
        "industry": "## Industry Overview\n...",
        "news": "## News\n...",
        "flw": "## FLW and Sustainability\n..."
        },
}

async def main_test():
    global mock_state_before_tagger # Allow modification

    # --- 1. Test Tagger ---
    logger.info("--- Testing Tagger Node ---")
    try:
        tagger = Tagger()
        state_after_tagger = await tagger.run(cast(ResearchState, mock_state_before_tagger.copy()))

        logger.info("State inspection after Tagger run:")
        industries = state_after_tagger.get('airtable_industries')
        region = state_after_tagger.get('airtable_country_region')
        revenue = state_after_tagger.get('airtable_revenue_band_est')

        logger.info(f"  > Industries Found: {industries} (Type: {type(industries)})")
        logger.info(f"  > Region Found: {region} (Type: {type(region)})")
        logger.info(f"  > Revenue Found: {revenue} (Type: {type(revenue)})")

        if not industries or not region or not revenue:
            logger.warning("Tagger did not find all expected classifications. Check Tagger logs/OpenAI response.")
        if not isinstance(industries, list) or not isinstance(region, list) or not isinstance(revenue, list):
             logger.warning("One or more tagger outputs are not lists. Check Tagger node logic.")

        mock_state_before_tagger.update(state_after_tagger)

    except Exception as e:
        logger.error(f"Error running Tagger node: {e}", exc_info=True)
        return

    logger.info("-" * 30)

    # --- 2. Test Airtable Uploader (Simulating graph.py's airtable_upload_node data prep) ---
    logger.info("--- Testing Airtable Upload Logic ---")
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

        # --- Prepare report_data dict (keys MUST match expected INPUT keys for upload_to_airtable) ---
        revenue_tag_list = state_for_upload.get("airtable_revenue_band_est", [])
        revenue_tag = revenue_tag_list[0] if isinstance(revenue_tag_list, list) and revenue_tag_list else None

        # --- REVISED report_data_for_upload ---
        # Use keys expected by the upload_to_airtable function's internal mapping
        report_data_for_upload = {
            "company_name": state_for_upload.get("company"),             # Use internal key
            "company_url": state_for_upload.get("company_url"),           # Use internal key
            "report_markdown": state_for_upload.get("report"),            # Use internal key
            "financial_briefing": state_for_upload.get("financial_briefing"), # Use internal key
            "industry_briefing": state_for_upload.get("industry_briefing"),   # Use internal key
            "company_briefing": state_for_upload.get("company_briefing"),     # Use internal key
            "news_briefing": state_for_upload.get("news_briefing"),         # Use internal key
            "flw_sustainability_briefing": state_for_upload.get("flw_sustainability_briefing"), # Use internal key for FLW
            "process_notes": process_notes_str,                       # Use prepared string
            "references_formatted": references_str_test,              # Use prepared string
            # Pass the tag data using the keys the function expects for mapping
            "industries_tags": state_for_upload.get("airtable_industries", []),
            "region_tags": state_for_upload.get("airtable_country_region", []),
            "revenue_tags": revenue_tag # Already extracted correctly
        }
        # --- END REVISION ---

        # Log prepared data keys/values (excluding long text fields)
        loggable_report_data = {k: v for k, v in report_data_for_upload.items() if k not in [
            'report_markdown', 'process_notes', 'references_formatted',
            'financial_briefing', 'industry_briefing', 'company_briefing',
            'news_briefing', 'flw_sustainability_briefing'
            ]}
        # Update log message to reflect these are *input* keys now
        logger.info(f"Data prepared *for input to* upload function: {loggable_report_data}")

        # Call the uploader function directly
        upload_result = upload_to_airtable(
            report_data=report_data_for_upload, # Pass the revised dictionary
            job_id=state_for_upload.get("job_id", "test-job-error"),
            record_id=state_for_upload.get("airtable_record_id")
        )

        logger.info("\nAirtable Upload Function Result:")
        logger.info(upload_result)

        if upload_result.get("status") != "Success":
            logger.error(f"Airtable upload/update failed: {upload_result.get('error')}")
        else:
            logger.info(f"Airtable operation successful. Record ID: {upload_result.get('airtable_record_id')}")
            # IMPORTANT reminder on testing INSERT vs UPDATE
            if state_for_upload.get("airtable_record_id") is None:
                logger.info(f"^^^ NOTE: Test performed an INSERT. To test UPDATE, set 'airtable_record_id' in mock_state_before_tagger to '{upload_result.get('airtable_record_id')}' and rerun.")

    except Exception as e:
        logger.error(f"Error running Airtable upload logic: {e}", exc_info=True)

    logger.info("-" * 30)
    logger.info("Test finished.")


if __name__ == "__main__":
    required_keys = ["OPENAI_API_KEY", "GEMINI_API_KEY", "AIRTABLE_API_KEY", "AIRTABLE_BASE_ID", "AIRTABLE_TABLE_NAME", "TAVILY_API_KEY"]
    missing_keys = [key for key in required_keys if not os.getenv(key)]

    if missing_keys:
        print(f"\nERROR: Missing required environment variables: {', '.join(missing_keys)}")
        print("Please ensure your .env file is correctly set up and loaded.\n")
    else:
        asyncio.run(main_test())