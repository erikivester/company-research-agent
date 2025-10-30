# backend/airtable_uploader.py
import os
import logging
from airtable import Airtable
from datetime import datetime
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# --- Re-defined Helper (for internal use by other nodes to update status) ---
def update_airtable_record(record_id: str, fields_to_update: Dict[str, Any]):
    """Updates specific fields of an existing Airtable record."""
    if not record_id:
        logger.warning("Airtable update skipped: No record ID provided.")
        return {"status": "Skipped", "error": "No record ID"}

    airtable_key = os.getenv('AIRTABLE_API_KEY')
    base_id = os.getenv('AIRTABLE_BASE_ID')
    table_name = os.getenv('AIRTABLE_TABLE_NAME')

    if not all([airtable_key, base_id, table_name]):
        logger.warning(f"Airtable update skipped: Environment variables not fully set.")
        return {"status": "Skipped", "error": "Airtable environment variables not set."}

    try:
        airtable = Airtable(
            base_id=base_id,
            table_name=table_name,
            api_key=airtable_key
        )

        # --- v2 MODIFICATION: Add 'ReFED Alignment' to multi-select list ---
        multi_select_fields = ['Industries', 'Country/Region', 'ReFED Alignment'] 
        for field in multi_select_fields:
            if field in fields_to_update:
                value = fields_to_update[field]
                if value is None:
                    fields_to_update[field] = [] 
                elif not isinstance(value, list):
                    try:
                        fields_to_update[field] = list(value) if value else []
                    except TypeError:
                        fields_to_update[field] = [str(value)] if value else []

        # Remove fields with None values before updating, but keep empty lists/strings
        fields_to_send_update = {k: v for k, v in fields_to_update.items() if v is not None}

        logger.info(f"DEBUG: Fields being sent for UPDATE: {fields_to_send_update.keys()}")

        updated_record = airtable.update(record_id, fields_to_send_update)
        logger.info(f"Successfully updated Airtable record {record_id} with fields: {list(fields_to_send_update.keys())}")
        return {"status": "Success", "airtable_record_id": record_id}

    except Exception as e:
        logger.error(f"Airtable status update failed for record {record_id}: {str(e)}")
        return {"status": "Failure", "error": f"Airtable update failed: {str(e)}"}

# --- NEW/MODIFIED Core Logic for UPSERT ---
def _find_record_by_company(airtable: Airtable, company_name: str) -> Optional[str]:
    """Searches Airtable for a record matching the Organization name."""
    if not company_name:
        return None
        
    try:
        # Airtable filtering requires a formula
        # FIX: Escape single quotes
        company_name_safe = company_name.replace("'", "\\'")
        filter_formula = f"{{Organization}} = '{company_name_safe}'"
        
        # Limit to 1 record and only retrieve the ID
        records = airtable.get_all(
            view='Grid view', # Use a valid view name, 'Grid view' is common default
            max_records=1,
            fields=['Organization'], 
            filter_by_formula=filter_formula
        )
        
        if records and records[0].get('id'):
            record_id = records[0]['id']
            logger.info(f"Existing Airtable record found for '{company_name}': {record_id}")
            return record_id
        
        logger.info(f"No existing Airtable record found for '{company_name}'.")
        return None
        
    except Exception as e:
        logger.error(f"Error searching for Airtable record by company '{company_name}': {e}")
        return None

def upload_to_airtable(report_data: Dict[str, Any], job_id: str, record_id: str = None):
    """
    (v2) Connects to Airtable and performs an UPSERT (Update or Insert).
    Maps all new v2 fields to their Airtable Column Names.
    """
    airtable_key = os.getenv('AIRTABLE_API_KEY')
    base_id = os.getenv('AIRTABLE_BASE_ID')
    table_name = os.getenv('AIRTABLE_TABLE_NAME')
    company_name = report_data.get('company_name', 'N/A')

    if not all([airtable_key, base_id, table_name]):
        logger.warning("Airtable upload/update skipped: Environment variables not fully set.")
        return {"status": "Skipped", "error": "Airtable environment variables not set."}

    try:
        airtable = Airtable(base_id, table_name, airtable_key)
    except Exception as e:
        logger.error(f"Airtable initialization failed: {str(e)}")
        return {"status": "Failure", "error": f"Airtable initialization failed: {str(e)}"}


    # --- 1. v2: Map all fields to Airtable format ---
    # NOTE: The keys on the *left* (e.g., 'Industries') are your *Airtable Column Names*.
    # The keys on the *right* (e.g., 'industries_tags') are the *internal Python keys* from graph.py.
    fields_to_send = {
        'Organization': company_name, 
        'Website': report_data.get('company_url', ''),
        
        # --- v2 Tags ---
        'Industries': report_data.get('industries_tags', []),
        'Country/Region': report_data.get('region_tags', []),
        'Revenue Band (est.)': report_data.get('revenue_tags'),
        'ReFED Alignment': report_data.get('refed_alignment_tags', []), # <-- NEW
        
        # --- v2 Briefings & Report ---
        'Markdown Report': (report_data.get('report_markdown') or '')[:10000],
        'Company Briefing': (report_data.get('company_brief_briefing') or '')[:8000],         # <-- RENAMED/NEW
        'News & Signals Briefing': (report_data.get('news_signal_briefing') or '')[:8000],   # <-- RENAMED/NEW
        'FLW and Sustainability Briefing': (report_data.get('flw_sustainability_briefing') or '')[:8000], # <-- KEPT
        'Potential Contacts Briefing': (report_data.get('contact_briefing') or '')[:8000],     # <-- NEW
        'Engagements Briefing': (report_data.get('engagement_briefing') or '')[:8000],          # <-- NEW
        # --- REMOVED: Financial Briefing, Industry Briefing, News Briefing (old) ---

        # --- Meta Fields ---
        'Research Status': 'Completed', 
        'Process Notes': (report_data.get('process_notes') or '')[:10000],
        'References': (report_data.get('references_formatted') or '')[:10000]
    }
    
    # Clean out None values before sending, but keep empty lists/strings
    fields_payload = {}
    for k, v in fields_to_send.items():
        if v is not None:
             fields_payload[k] = v
        # v2: Ensure all multi-select lists are sent even if empty
        elif k in ['Industries', 'Country/Region', 'ReFED Alignment']:
            fields_payload[k] = []
            
    logger.info(f"DEBUG: Final payload keys being sent: {fields_payload.keys()}")


    # --- 2. Determine Record ID (Search/Upsert Logic) ---
    final_record_id = record_id
    if not final_record_id and company_name != 'N/A':
        # Search Airtable by Organization Name only if record_id wasn't provided (e.g., first run)
        final_record_id = _find_record_by_company(airtable, company_name)


    # --- 3. Execute UPSERT ---
    if final_record_id:
        # UPDATE: Record found, update all fields
        logger.info(f"Performing UPDATE on Airtable record {final_record_id} for job {job_id}")
        
        update_result = update_airtable_record(final_record_id, fields_payload)
        
        if update_result.get("status") == "Success":
            logger.info(f"Airtable UPDATE successful: {final_record_id}")
            return {"status": "Success", "airtable_record_id": final_record_id, "operation": "UPDATE"}
        else:
            return update_result
            
    else:
        # INSERT: No existing record found, insert a new one
        logger.warning(f"No existing record found for job {job_id}, attempting INSERT as new record.")
        try:
            record = airtable.insert(fields_payload)
            logger.info(f"Successfully inserted final data as new record: {record['id']}")
            return {"status": "Success", "airtable_record_id": record['id'], "operation": "INSERT"}
        except Exception as e:
             logger.error(f"Airtable INSERT failed for job {job_id}: {str(e)}")
             return {"status": "Failure", "error": f"Airtable final insert failed: {str(e)}"}