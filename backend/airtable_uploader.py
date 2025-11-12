# backend/airtable_uploader.py
import os
import json
import logging
import time
from airtable import Airtable
from datetime import datetime
from typing import Dict, Any, Optional, List
from .utils.airtable_mappings import REVENUE_BAND_MAPPINGS

logger = logging.getLogger(__name__)

# --- Re-defined Helper (for internal use by other nodes to update status) ---
def update_airtable_record(record_id: str, fields_to_update: Dict[str, Any], retries: int = 3, delay: int = 2):
    """
    Updates specific fields of an existing Airtable record with retry logic.
    """
    if not record_id:
        logger.warning("Airtable update skipped: No record ID provided.")
        return {"status": "Skipped", "error": "No record ID"}

    airtable_key = os.getenv('AIRTABLE_API_KEY')
    base_id = os.getenv('AIRTABLE_BASE_ID')
    table_name = os.getenv('AIRTABLE_TABLE_NAME')

    if not all([airtable_key, base_id, table_name]):
        logger.warning("Airtable update skipped: Environment variables not fully set.")
        return {"status": "Skipped", "error": "Airtable environment variables not set."}

    for attempt in range(retries):
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

            # Remove fields with None values and protect the Contacts field
            fields_to_send_update = {k: v for k, v in fields_to_update.items() 
                                   if v is not None and k != 'Contacts'}  # Never update Contacts field here

            logger.info(f"DEBUG: Fields being sent for UPDATE: {fields_to_send_update.keys()}")

            updated_record = airtable.update(record_id, fields_to_send_update)
            logger.info(f"Successfully updated Airtable record {record_id} with fields: {list(fields_to_send_update.keys())}")
            return {"status": "Success", "airtable_record_id": record_id}

        except Exception as e:
            logger.error(f"Airtable status update failed for record {record_id} on attempt {attempt + 1}/{retries}: {str(e)}")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                # Re-raise the exception on the final attempt
                raise e
    
    # This part should not be reached if retries > 0
    return {"status": "Failure", "error": "Airtable update failed after multiple retries."}

# --- NEW/MODIFIED Core Logic for UPSERT ---
def _find_contact_record(contacts_airtable: Airtable, name: str, company_record_id: str, company_name: str) -> Optional[str]:
    """Searches for a contact by name that's already linked to the given company."""
    try:
        # Build a formula that checks name and company link
        name_safe = name.replace("'", "\\'")
        filter_formula = f"AND({{Name}} = '{name_safe}', FIND('{company_record_id}', ARRAYJOIN({{Organization}})) > 0)"
        
        records = contacts_airtable.get_all(
            view='Grid view',
            max_records=1,
            fields=['Name', 'Organization'],
            formula=filter_formula
        )
        
        if records and records[0].get('id'):
            record_id = records[0]['id']
            # Ensure Company Name is set correctly
            current_company = records[0].get('fields', {}).get('Company Name', '')
            if current_company != company_name:
                contacts_airtable.update(record_id, {'Company Name': company_name})
                logger.info(f"Updated Company Name for contact {name} to {company_name}")
            return record_id
        return None
        
    except Exception as e:
        logger.error(f"Error searching for contact record: {e}")
        return None

def _get_contact_record_ids(contacts_airtable: Airtable, company_record_id: str) -> List[str]:
    """
    Gets the record IDs of all contacts for a company.
    
    Args:
        contacts_airtable: Airtable instance for the contacts table
        company_record_id: The Airtable record ID of the company
    
    Returns:
        List of contact record IDs
    """
    try:
        # Build formula to find all contacts linked to this company
        filter_formula = f"FIND('{company_record_id}', ARRAYJOIN({{Organization}})) > 0"
        
        records = contacts_airtable.get_all(
            view='Grid view',
            fields=['Name', 'Title', 'Organization'],
            formula=filter_formula
        )
        
        # Extract unique record IDs for linking
        contact_ids = list(set(record['id'] for record in records if record.get('id')))
        logger.info(f"Found {len(contact_ids)} unique linked contact records for company {company_record_id}")
        return contact_ids
        
    except Exception as e:
        logger.error(f"Error getting contact record IDs: {e}")
        return []

def create_and_link_contacts(contacts_json: str, company_record_id: str) -> Dict[str, Any]:
    """
    Creates or updates contact records in a separate Airtable table and links them to the company.
    Also updates the Key Contacts lookup field in the company record.
    
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
        logger.info(f"Attempting to parse contacts JSON: {contacts_json}")
        # Clean the input string - remove markdown code block markers, json markers, and whitespace
        cleaned_json = contacts_json.strip()
        if cleaned_json.startswith('```'):
            # Remove opening markdown block and any 'json' language identifier
            first_newline = cleaned_json.find('\n')
            if first_newline != -1:
                cleaned_json = cleaned_json[first_newline:].strip()
            # Remove closing markdown block if it exists
            if '```' in cleaned_json:
                cleaned_json = cleaned_json[:cleaned_json.rfind('```')]
        cleaned_json = cleaned_json.strip()
        logger.info(f"Cleaned contacts JSON: {cleaned_json}")
        
        logger.info(f"Cleaned JSON string: {cleaned_json}")
        
        # Parse contacts JSON
        contacts = json.loads(cleaned_json)
        if not isinstance(contacts, list):
            logger.error(f"Invalid contacts format - expected list but got {type(contacts)}")
            raise ValueError("Contacts data must be a JSON array")
        logger.info(f"Successfully parsed {len(contacts)} contacts from JSON")
        
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
        
        # --- FIX: Instantiate company_airtable ONCE before the loop ---
        company_airtable = Airtable(
            base_id=base_id,
            table_name=os.getenv('AIRTABLE_TABLE_NAME'),
            api_key=airtable_key
        )
        company_record = company_airtable.get(company_record_id)
        company_name = company_record['fields'].get('Organization', 'Unknown')
        # --- END FIX ---

        for contact in contacts:
            try:
                name = contact.get('name')
                if not name:
                    continue
                
                # Check if contact already exists and is linked
                existing_id = _find_contact_record(contacts_airtable, name, company_record_id, company_name)
                
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
                    # Create new contact record with Organization as a linked record
                    fields = {
                        "Name": name,
                        "Title": contact.get('title', ''),
                        "Summary": contact.get('summary', ''),
                        "Organization": [company_record_id]  # Must be an array of record IDs for linked record field
                    }
                    
                    new_record = contacts_airtable.insert(fields)
                    new_record_id = new_record['id']
                    logger.info(f"Created new contact record with ID: {new_record_id}")
                    
                    # --- FIX: REMOVED redundant company update block ---
                    # We will do a single update AFTER the loop
                    
                    results["new"] += 1
                    results["details"].append({
                        "name": name,
                        "status": "created",
                        "record_id": new_record_id
                    })
                    logger.info(f"Created new contact record for {name} and linked to company")
                    
            except Exception as e:
                logger.error(f"Error processing contact {contact.get('name', 'Unknown')}: {e}")
                results["errors"] += 1
                results["details"].append({
                    "name": contact.get('name', 'Unknown'),
                    "status": "error",
                    "error": str(e)
                })

        # --- FIX: This block is now DE-INDENTED to run ONCE after the loop ---
        try:
            # Get all contact record IDs that should be linked
            contact_record_ids = []
            for detail in results["details"]:
                if detail.get("record_id"):
                    contact_record_ids.append(detail["record_id"])
                    
            if contact_record_ids:
                # Get existing contacts to merge with new ones
                # We already fetched company_record and company_airtable
                existing_contacts = company_record.get('fields', {}).get('Contacts', [])
                
                # Combine existing and new contacts, ensure uniqueness
                all_contacts = list(set(existing_contacts + contact_record_ids))
                
                # Update the Contacts linked records field
                company_airtable.update(
                    company_record_id,
                    {'Contacts': all_contacts}
                )
                logger.info(f"Final update: Company record now has {len(all_contacts)} linked contacts")
            else:
                logger.warning("No new or existing contact record IDs found to link to company")
                
        except Exception as lookup_exc:
            logger.error(f"Error in final contact link verification: {lookup_exc}")
            results["lookup_field_error"] = str(lookup_exc)
        # --- END FIX ---

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
        return {"status": "Skipped", "error": "ADbug.logirtable environment variables not set."}

    try:
        airtable = Airtable(base_id, table_name, airtable_key)
    except Exception as e:
        logger.error(f"Airtable initialization failed: {str(e)}")
        return {"status": "Failure", "error": f"Airtable initialization failed: {str(e)}"}


    # --- 1. v2: Map all fields to Airtable format ---
    # Map revenue band to exact Airtable option
    revenue_tag = report_data.get('revenue_tags')
    if revenue_tag:
        revenue_tag = REVENUE_BAND_MAPPINGS.get(revenue_tag, revenue_tag)

    # Determine Record ID first
    final_record_id = record_id
    if not final_record_id and company_name != 'N/A':
        final_record_id = _find_record_by_company(airtable, company_name)

    fields_to_send = {
        'Organization': company_name, 
        'Website': report_data.get('company_url', ''),
        'Industries': report_data.get('industries_tags', []),
        'Country/Region': report_data.get('region_tags', []),
        'Revenue Band (est.)': revenue_tag,
        'ReFED Alignment': report_data.get('refed_alignment_tags', []), 
        'Markdown Report': (report_data.get('report_markdown') or '')[:10000],
        'Company Briefing': (report_data.get('company_brief_briefing') or '')[:8000],
        'News & Signals Briefing': (report_data.get('news_signal_briefing') or '')[:8000],
        'FLW and Sustainability Briefing': (report_data.get('flw_sustainability_briefing') or '')[:8000],
        'Engagements Briefing': (report_data.get('engagement_briefing') or '')[:8000],
        'Research Status': 'Completed', 
        'Process Notes': (report_data.get('process_notes') or '')[:10000],
        'References': (report_data.get('references_formatted') or '')[:10000]
        # Contacts field is managed separately by create_and_link_contacts
    }
    
    fields_payload = {}
    for k, v in fields_to_send.items():
        if v is not None:
             fields_payload[k] = v
        elif k in ['Industries', 'Country/Region', 'ReFED Alignment']:
            fields_payload[k] = []
            
    logger.info(f"DEBUG: Final payload keys being sent: {fields_payload.keys()}")


    # Record ID was determined earlier when getting contacts


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