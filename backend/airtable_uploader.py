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
def _find_contact_record(contacts_airtable: Airtable, name: str, company_record_id: str) -> Optional[str]:
    """Searches for a contact by name that's already linked to the given company."""
    try:
        # Build a formula that checks both name and company link
        name_safe = name.replace("'", "\\'")
        filter_formula = f"AND({{Name}} = '{name_safe}', FIND('{company_record_id}', {{Organization}}) > 0)"
        
        records = contacts_airtable.get_all(
            view='Grid view',
            max_records=1,
            fields=['Name'],
            formula=filter_formula
        )
        
        if records and records[0].get('id'):
            return records[0]['id']
        return None
        
            except Exception as e:
                logger.error(f"Error searching for contact record: {e}")
                return None

def _format_contacts_lookup(contacts_airtable: Airtable, company_record_id: str) -> str:
    """
    Generates a formatted list of all contacts linked to a company for the lookup field.
    
    Args:
        contacts_airtable: Airtable instance for the contacts table
        company_record_id: The Airtable record ID of the company
    
    Returns:
        Formatted string containing all linked contacts
    """
    try:
        # Build formula to find all contacts linked to this company
        filter_formula = f"FIND('{company_record_id}', {{Organization}}) > 0"
        
        records = contacts_airtable.get_all(
            view='Grid view',
            fields=['Name', 'Title'],
            formula=filter_formula
        )
        
        if not records:
            return ""
            
        # Format each contact as "Name (Title)"
        contact_lines = []
        for record in records:
            fields = record.get('fields', {})
            name = fields.get('Name', '')
            title = fields.get('Title', '')
            if name:
                contact_lines.append(f"{name}{f' ({title})' if title else ''}")
                
        return "\n".join(contact_lines)
        
    except Exception as e:
        logger.error(f"Error generating contacts lookup: {e}")
        return "[Error retrieving contacts]"

def create_and_link_contacts(contacts_json: str, company_record_id: str) -> Dict[str, Any]:
    """
    Creates or updates contact records in a separate Airtable table and links them to the company.
    Also updates the Key Contacts lookup field in the company record.
    
    Args:
        contacts_json: JSON string containing an array of contact objects
        company_record_id: The Airtable record ID of the company to link contacts to
    
    Returns:
        Dict with status information about the operation
    """def create_and_link_contacts(contacts_json: str, company_record_id: str) -> Dict[str, Any]:
    """
    Creates or updates contact records in a separate Airtable table and links them to the company.
    
    Args:
        contacts_json: JSON string containing an array of contact objects
        company_record_id: The Airtable record ID of the company to link contacts to
    
    Returns:
        Dict with status information about the operation
    """
    airtable_key = os.getenv('AIRTABLE_API_KEY')
    base_id = os.getenv('AIRTABLE_BASE_ID')
    contacts_table = os.getenv('AIRTABLE_CONTACTS_TABLE_NAME')

    if not all([airtable_key, base_id, contacts_table]):
        logger.warning("Contacts upload skipped: Environment variables not fully set")
        return {"status": "Skipped", "error": "Required environment variables not set"}

    try:
        # Parse contacts JSON
        contacts = json.loads(contacts_json)
        if not isinstance(contacts, list):
            raise ValueError("Contacts data must be a JSON array")
        
        contacts_airtable = Airtable(
            base_id=base_id,
            table_name=contacts_table,
            api_key=airtable_key
        )

        results = {
            "total": len(contacts),
            "new": 0,
            "existing": 0,
            "errors": 0,
            "details": []
        }

        for contact in contacts:
            try:
                name = contact.get('name')
                if not name:
                    continue
                
                # Check if contact already exists and is linked
                existing_id = _find_contact_record(contacts_airtable, name, company_record_id)
                
                if existing_id:
                    # Contact exists and is already linked
                    logger.info(f"Contact {name} already exists and is linked to company")
                    results["existing"] += 1
                    results["details"].append({
                        "name": name,
                        "status": "existing",
                        "record_id": existing_id
                    })
                else:
                    # Create new contact record
                    fields = {
                        "Name": name,
                        "Title": contact.get('title', ''),
                        "Summary": contact.get('summary', ''),
                        "Organization": [company_record_id]  # Link to company record
                    }
                    
                    new_record = contacts_airtable.insert(fields)
                    results["new"] += 1
                    results["details"].append({
                        "name": name,
                        "status": "created",
                        "record_id": new_record['id']
                    })
                    logger.info(f"Created new contact record for {name}")
                    
            except Exception as e:
                logger.error(f"Error processing contact {contact.get('name', 'Unknown')}: {e}")
                results["errors"] += 1
                results["details"].append({
                    "name": contact.get('name', 'Unknown'),
                    "status": "error",
                    "error": str(e)
                })

        # After all contacts are processed, update the lookup field in the company record
        try:
            # Get formatted contacts list
            contacts_lookup = _format_contacts_lookup(contacts_airtable, company_record_id)
            
            # Get Airtable instance for main company table
            company_airtable = Airtable(
                base_id=base_id,
                table_name=os.getenv('AIRTABLE_TABLE_NAME'),
                api_key=airtable_key
            )
            
            # Update the Key Contacts lookup field
            company_airtable.update(
                company_record_id,
                {'Key Contacts': contacts_lookup}
            )
            logger.info(f"Updated company record with Key Contacts lookup field")
            
        except Exception as lookup_exc:
            logger.error(f"Error updating Key Contacts lookup field: {lookup_exc}")
            results["lookup_field_error"] = str(lookup_exc)

        logger.info(f"Contacts processing completed: {results['new']} new, {results['existing']} existing, {results['errors']} errors")
        return {
            "status": "Success",
            "results": results
        }

    except json.JSONDecodeError as e:
        logger.error(f"Invalid contacts JSON data: {e}")
        return {"status": "Failure", "error": f"Invalid JSON data: {str(e)}"}
    except Exception as e:
        logger.error(f"Error processing contacts: {e}")
        return {"status": "Failure", "error": str(e)}

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
            formula=filter_formula  # <-- THIS LINE IS THE FIX (was filter_by_formula)
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
        logger.warning(f"Airtable upload/update skipped: Environment variables not fully set. Check keys.")
        logger.warning(f"DEBUG: Key: {airtable_key}, Base: {base_id}, Table: {table_name}")
        return {"status": "Skipped", "error": "Airtable environment variables not set."}

    try:
        airtable = Airtable(base_id, table_name, airtable_key)
    except Exception as e:
        logger.error(f"Airtable initialization failed: {str(e)}")
        return {"status": "Failure", "error": f"Airtable initialization failed: {str(e)}"}


    # --- 1. v2: Map all fields to Airtable format ---
    fields_to_send = {
        'Organization': company_name, 
        'Website': report_data.get('company_url', ''),
        'Industries': report_data.get('industries_tags', []),
        'Country/Region': report_data.get('region_tags', []),
        'Revenue Band (est.)': report_data.get('revenue_tags'),
        'ReFED Alignment': report_data.get('refed_alignment_tags', []), 
        'Markdown Report': (report_data.get('report_markdown') or '')[:10000],
        'Company Briefing': (report_data.get('company_brief_briefing') or '')[:8000],
        'News & Signals Briefing': (report_data.get('news_signal_briefing') or '')[:8000],
        'FLW and Sustainability Briefing': (report_data.get('flw_sustainability_briefing') or '')[:8000],
        'Engagements Briefing': (report_data.get('engagement_briefing') or '')[:8000],
        'Research Status': 'Completed', 
        'Process Notes': (report_data.get('process_notes') or '')[:10000],
        'References': (report_data.get('references_formatted') or '')[:10000]
    }
    
    fields_payload = {}
    for k, v in fields_to_send.items():
        if v is not None:
             fields_payload[k] = v
        elif k in ['Industries', 'Country/Region', 'ReFED Alignment']:
            fields_payload[k] = []
            
    logger.info(f"DEBUG: Final payload keys being sent: {fields_payload.keys()}")


    # --- 2. Determine Record ID (Search/Upsert Logic) ---
    final_record_id = record_id
    if not final_record_id and company_name != 'N/A':
        final_record_id = _find_record_by_company(airtable, company_name)


    # --- 3. Execute UPSERT ---
    if final_record_id:
        logger.info(f"Performing UPDATE on Airtable record {final_record_id} for job {job_id}")
        update_result = update_airtable_record(final_record_id, fields_payload)
        
        if update_result.get("status") == "Success":
            logger.info(f"Airtable UPDATE successful: {final_record_id}")
            return {"status": "Success", "airtable_record_id": final_record_id, "operation": "UPDATE"}
        else:
            return update_result
            
    else:
        logger.warning(f"No existing record found for job {job_id}, attempting INSERT as new record.")
        try:
            record = airtable.insert(fields_payload)
            logger.info(f"Successfully inserted final data as new record: {record['id']}")
            return {"status": "Success", "airtable_record_id": record['id'], "operation": "INSERT"}
        except Exception as e:
             logger.error(f"Airtable INSERT failed for job {job_id}: {str(e)}")
             return {"status": "Failure", "error": f"Airtable final insert failed: {str(e)}"}