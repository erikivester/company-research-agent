import asyncio
import logging
import os
import uuid
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import uvicorn
from dotenv import load_dotenv
import prometheus_client
from prometheus_client import start_http_server
import httpx

# Load environment variables from .env file FIRST, before any backend imports
env_path = Path(__file__).parent / '.env'
if env_path.exists():
    load_dotenv(dotenv_path=env_path, override=True)

from backend.utils.monitoring import metrics_collector, performance_monitor, setup_logging
from backend.utils.status_constants import ResearchStatus
from backend.config import config  # Import the singleton config instance
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Body, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel
from pydantic import Field
from typing import Optional

from backend.graph import Graph
from backend.services.mongodb import MongoDBService
from backend.services.pdf_service import PDFService
from backend.services.websocket_manager import WebSocketManager
from backend.services.email_generator import EmailGeneratorService
from backend.classes.email_models import EmailGenerationRequest, EmailGenerationResponse
from backend.classes.auth_models import TokenRequest, Token
from backend.utils.email_templates import get_template_manager
from backend.utils.api_key import verify_api_key

# ⬇️ This import remains as it's used by the GRAPH, not directly here ⬇️
from backend.airtable_uploader import update_airtable_record
from backend.utils.gdrive_uploader import upload_context_to_gdrive, inspect_drive_folder  # Add GDrive import
# --- FIX: ADDED THIS IMPORT BACK ---
from backend.debug_airtable import run_airtable_debug_test 
# --- END FIX ---

# Set email templates folder ID
os.environ["EMAIL_TEMPLATES_FOLDER_ID"] = "1tt4LLouNP2FgHcguIKlnRzRb3j5jE8LH"

# Configure logging using our custom configuration
setup_logging(
    log_level=os.getenv("LOG_LEVEL", "INFO"),
    log_file=os.getenv("LOG_FILE", "logs/app.log")
)
logger = logging.getLogger(__name__)

from backend.utils.security import (
    JWTBearer, SecurityConfig, limiter,
    sanitize_input, validate_folder_url, get_current_user
)
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from fastapi.security import HTTPBearer

# Initialize FastAPI with security config
security_config = SecurityConfig(
    secret_key=config.JWT_SECRET_KEY,
    allowed_origins=config.ALLOWED_ORIGINS,
    rate_limit_requests=config.RATE_LIMIT_REQUESTS,
    rate_limit_period=config.RATE_LIMIT_PERIOD
)

app = FastAPI(
    title="Company Research and Outreach API",
    description="""
    API for generating highly personalized outreach emails using AI. 
    Combines email templates, research context, and Airtable data.
    
    ## Features
    
    * AI-powered email generation
    * Template management
    * Research context integration
    * Airtable integration
    
    ## Authentication
    
    All endpoints require JWT authentication. Include the token in the Authorization header:
    ```
    Authorization: Bearer your-jwt-token
    ```
    
    ## Rate Limiting
    
    Endpoints are rate-limited to ensure fair usage. Default limit is 100 requests per hour.
    """,
    version="1.0.0",
    contact={
        "name": "API Support",
        "email": "support@example.com"
    },
    license_info={
        "name": "MIT",
        "url": "https://opensource.org/licenses/MIT"
    }
)

# Add rate limiter
app.state.limiter = limiter
app.add_middleware(SlowAPIMiddleware)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for Airtable compatibility
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

manager = WebSocketManager()
pdf_service = PDFService({"pdf_output_dir": "pdfs"})
# Initialize services
manager = WebSocketManager()
pdf_service = PDFService({"pdf_output_dir": "pdfs"})
# Email service will be initialized lazily when needed

job_status = defaultdict(lambda: {
    "status": "pending",
    "result": None,
    "error": None,
    "debug_info": [],
    "company": None,
    "report": None,
    "last_update": datetime.now().isoformat()
})

mongodb = None
if mongo_uri := os.getenv("MONGODB_URI"):
    try:
        mongodb = MongoDBService(mongo_uri)
        logger.info("MongoDB integration enabled")
    except Exception as e:
        logger.warning(f"Failed to initialize MongoDB: {e}. Continuing without persistence.")

class ResearchRequest(BaseModel):
    company: str
    company_url: str | None = None
    industry: str | None = None
    hq_location: str | None = None

# --- v2 MODIFIED: Pydantic Model for Webhook Input ---
class AirtableWebhookInput(ResearchRequest):
    """
    Extends ResearchRequest. The webhook payload from Airtable is expected
    to match the fields defined in the base ResearchRequest model.
    This model adds an alias so 'recordId' in the payload is mapped to 'airtable_record_id'.
    """
    airtable_record_id: Optional[str] = Field(default=None, alias="recordId")
    use_local_context: Optional[bool] = False
    google_drive_folder_url: Optional[str] = None
# --- END v2 MODIFICATION ---

class PDFGenerationRequest(BaseModel):
    report_content: str
    company_name: str | None = None

# ----------------------------------------------------
# 🟢 CONCURRENCY CONTROL SETUP
# ----------------------------------------------------
# Define the maximum number of research jobs allowed to run concurrently.
# CONSERVATIVE: Set to 3 to ensure high-quality research with ample API quota per job
# With rate limiting (80 Tavily RPM), 3 concurrent jobs get ~27 RPM each
# This prevents API throttling and ensures each job completes quickly with full data
# Memory estimate: 3 jobs × ~80-120MB = 240-360MB peak (very safe)
MAX_CONCURRENT_JOBS = 2
MAX_QUEUE_SIZE = 20  # Maximum jobs that can be queued/running at once
job_semaphore = asyncio.Semaphore(MAX_CONCURRENT_JOBS)

# Track job queue metrics
job_queue_stats = {
    "total_queued": 0,
    "total_completed": 0,
    "total_failed": 0,
    "total_rejected": 0,
    "current_running": 0,
    "peak_concurrent": 0,
}

# Track active job tasks for cancellation
active_job_tasks = {}

# ----------------------------------------------------
# 🟢 SEMAPHORE WRAPPER FOR BACKGROUND TASK
# ----------------------------------------------------
# Helper to perform the synchronous Airtable update call in a separate thread
async def _update_airtable_status_queued(record_id: str, status_text: str):
    """Helper to call the synchronous update function in a separate thread."""
    if not record_id:
        logger.warning("Airtable status update skipped: No record ID provided.")
        return
    try:
        await asyncio.to_thread(update_airtable_record, record_id, {'Research Status': status_text})
        logger.debug(f"Airtable status update successful for record {record_id} to {status_text}")
    except Exception as e:
        logger.error(f"Airtable status update failed for record {record_id} to {status_text}: {e}", exc_info=True)
        metrics_collector.track_error("airtable_update")


# --- v2 MODIFIED: Added google_drive_folder_url parameter ---
def _log_task_exception(task):
    """Callback to log any exceptions from background tasks."""
    try:
        task.result()
    except Exception as e:
        logger.error(f"🔥 Background task failed with exception: {e}", exc_info=True)
        import sys
        sys.stdout.flush()
        sys.stderr.flush()


async def run_job_with_semaphore(
    job_id: str,
    data: ResearchRequest,
    airtable_record_id: str | None,
    google_drive_folder_url: str | None,
    use_local_context: bool = False  # <-- NEW: flag from Airtable
):
    """Acquire semaphore, run research logic, release semaphore with metrics tracking."""
    import sys

    # Track queue position
    job_queue_stats["total_queued"] += 1
    queue_position = job_queue_stats["total_queued"]

    try:
        print(f"\n🚀 STARTING JOB: {job_id} for {data.company} (Queue #{queue_position})", flush=True)

        # Acquire semaphore
        await job_semaphore.acquire()

        # Update metrics
        job_queue_stats["current_running"] += 1
        job_queue_stats["peak_concurrent"] = max(
            job_queue_stats["peak_concurrent"],
            job_queue_stats["current_running"]
        )

        print(f"✅ Semaphore acquired for {data.company}", flush=True)
        logger.info(f"🚀 SEMAPHORE ACQUIRED: Job {job_id} starting research for {data.company}")
        logger.info(f"   Running: {job_queue_stats['current_running']}/{MAX_CONCURRENT_JOBS}, Queued: {queue_position}, Peak: {job_queue_stats['peak_concurrent']}")
        sys.stdout.flush()

        try:
            logger.info(f"🔍 Starting research pipeline for {data.company}...")
            sys.stdout.flush()
            await process_research(job_id, data, airtable_record_id, google_drive_folder_url, use_local_context)

            # Track success
            job_queue_stats["total_completed"] += 1
            logger.info(f"✅ Job {job_id} completed successfully. Total completed: {job_queue_stats['total_completed']}")

        except Exception as e:
            logger.error(f"❌ Job {job_id} failed during execution: {e}", exc_info=True)
            job_queue_stats["total_failed"] += 1
            metrics_collector.track_error("job_execution")

        finally:
            # Always release resources
            job_semaphore.release()
            job_queue_stats["current_running"] -= 1

            # Clean up from active jobs tracking
            if job_id in active_job_tasks:
                del active_job_tasks[job_id]

            logger.info(f"✅ SEMAPHORE RELEASED: Job {job_id} finished. Running: {job_queue_stats['current_running']}/{MAX_CONCURRENT_JOBS}")
            sys.stdout.flush()

    except Exception as e:
        logger.error(f"💥 CRITICAL ERROR in run_job_with_semaphore for job {job_id}: {e}", exc_info=True)
        job_queue_stats["total_failed"] += 1
        sys.stdout.flush()

        # Best-effort release
        try:
            job_semaphore.release()
            job_queue_stats["current_running"] = max(0, job_queue_stats["current_running"] - 1)
        except Exception:
            pass


@app.options("/research")
async def preflight():
    response = JSONResponse(content=None, status_code=200)
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    return response

# --- v2 MODIFIED: process_research signature ---
async def process_research(
    job_id: str, 
    data: ResearchRequest, 
    airtable_record_id: str | None = None, 
    google_drive_folder_url: str | None = None,
    use_local_context: bool = False  # <-- NEW: flag to skip Tavily
):
    try:
        with metrics_collector.generation_timer(template_type="research"):
            if mongodb:
                # Include airtable_record_id in MongoDB job details
                job_details = data.dict()
                job_details['airtable_record_id'] = airtable_record_id
                job_details['google_drive_folder_url'] = google_drive_folder_url # Add GDrive URL to log
                job_details['use_local_context'] = use_local_context  # <-- NEW
                mongodb.create_job(job_id, job_details)
            
            await asyncio.sleep(1)  # Allow WebSocket connection

            await manager.send_status_update(job_id, status="processing", message="Starting research")

            # --- v2 MODIFIED: Pass google_drive_folder_url to Graph constructor ---
            graph = Graph(
                company=data.company,
                url=data.company_url,
                industry=data.industry,
                hq_location=data.hq_location,
                websocket_manager=manager,
                job_id=job_id,
                google_drive_folder_url=google_drive_folder_url, # <-- PASS GDrive URL
                use_local_context=use_local_context  # <-- NEW: pass flag to Graph
            )

            # --- FIX: Pass ALL input data to the Graph's thread config as top-level keys ---
            # LangGraph/StateGraph expects the config keys at the top-level so nodes
            # can access them directly via state.get('company'), etc.
            thread_config = {
                # Add all the fields from the 'data' object
                "company": data.company,
                "company_url": data.company_url,
                "industry": data.industry,
                "hq_location": data.hq_location,
                "job_id": job_id, # Pass the job_id as well
                "use_local_context": use_local_context  # <-- NEW: expose in state
            }

            # Add the optional fields only if they exist
            if airtable_record_id:
                thread_config["airtable_record_id"] = airtable_record_id
            if google_drive_folder_url:
                thread_config["google_drive_folder_url"] = google_drive_folder_url
            # --- End Fix ---

            # Get the final state from the graph
            # Since we're using mode="values", each iteration yields the full state snapshot
            # We keep the last one which is the final complete state
            state = {}
            async for s in graph.run(thread=thread_config):
                # Each s is now a complete state snapshot (not a delta)
                # Keep updating to get the final state
                if s:
                    state = s  # Replace with latest full state instead of merging
            if not state:
                state = {}

            # DEBUG: Log what keys are in the final state and whether report exists
            logger.info(f"Final accumulated state keys: {list(state.keys())}")
            logger.info(f"'report' in state: {'report' in state}")
            if 'report' in state:
                report_len = len(state.get('report', ''))
                logger.info(f"Report length in state: {report_len}")

            # Look for the compiled report. 'editor' key is no longer used, but keeping check is safe.
            report_content = state.get('report') or (state.get('editor') or {}).get('report')
            
            # Airtable upload is handled inside the graph.run() call

            if report_content:
                logger.info(f"Found report in final state (length: {len(report_content)})")

                # Update job status and MongoDB
                job_status[job_id].update({
                    "status": "completed",
                    "report": report_content,
                    "company": data.company,
                    "last_update": datetime.now().isoformat()
                })
                if mongodb:
                    mongodb.update_job(job_id=job_id, status="completed")
                    mongodb.store_report(job_id=job_id, report_data={"report": report_content})
                
                # Simplified final WebSocket message
                await manager.send_status_update(
                    job_id=job_id,
                    status="completed",
                    message="Research completed successfully.",
                    result={
                        "report": report_content,
                        "company": data.company
                    }
                )
            else:
                logger.error(f"Research completed without finding report. State keys: {list(state.keys())}")
                
                # Check if there was a specific error in the state
                error_message = "No report found"
                if error := state.get('error'):
                    error_message = f"Error: {error}"
                
                await manager.send_status_update(
                    job_id=job_id,
                    status="failed",
                    message="Research completed but no report was generated",
                    error=error_message
                )
    except Exception as e:
        logger.error(f"Research failed: {str(e)}")
        await manager.send_status_update(
            job_id=job_id,
            status="failed",
            message=f"Research failed: {str(e)}",
            error=str(e)
        )
        if mongodb:
            mongodb.update_job(job_id=job_id, status="failed", error=str(e))

    except Exception as e:
        logger.error(f"Research failed: {str(e)}")
        await manager.send_status_update(
            job_id=job_id,
            status="failed",
            message=f"Research failed: {str(e)}",
            error=str(e)
        )
        if mongodb:
            mongodb.update_job(job_id=job_id, status="failed", error=str(e))
# --- END CORE RESEARCH LOGIC ---


@app.post("/research")
async def research(data: ResearchRequest):
    try:
        logger.info(f"Received research request for {data.company}")
        job_id = str(uuid.uuid4())
        # --- v2 MODIFIED: Pass google_drive_folder_url=None for UI runs ---
        task = asyncio.create_task(
            run_job_with_semaphore(
                job_id,
                data,
                airtable_record_id=None,
                google_drive_folder_url=None,
            )
        )
        task.add_done_callback(_log_task_exception)
        active_job_tasks[job_id] = {"task": task, "company": data.company}

        response = JSONResponse(content={
            "status": "accepted",
            "job_id": job_id,
            "message": "Research started. Connect to WebSocket for updates.",
            "websocket_url": f"/research/ws/{job_id}"
        })
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
        return response

    except Exception as e:
        logger.error(f"Error initiating research: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

# --- v2 MODIFIED: Webhook Endpoint for Airtable Automation ---
@app.post("/webhook/start-research")
async def start_research_webhook(data: AirtableWebhookInput):
    """
    Accepts a POST request (e.g., from an Airtable Automation webhook) 
    and queues the research pipeline using the semaphore.
    """
    job_id = str(uuid.uuid4())
    
    try:
        logger.info(f"📥 WEBHOOK RECEIVED: company='{data.company}', airtable_record_id={data.airtable_record_id}")

        if not data.company or not data.company.strip():
            logger.warning(f"⚠️ Webhook rejected: Missing or empty company name. Received: '{data.company}'")
            if data.airtable_record_id:
                asyncio.create_task(_update_airtable_status_queued(data.airtable_record_id, ResearchStatus.FAILED_MISSING_COMPANY))
            raise HTTPException(
                status_code=422, 
                detail="Company name is required and cannot be empty."
            )
        
        company_value = data.company.strip()

        # Check if queue is full
        current_queue_size = len(active_job_tasks)
        if current_queue_size >= MAX_QUEUE_SIZE:
            job_queue_stats["total_rejected"] += 1
            logger.warning(f"⚠️ Queue full ({current_queue_size}/{MAX_QUEUE_SIZE}), rejecting {company_value}")
            if data.airtable_record_id:
                asyncio.create_task(_update_airtable_status_queued(
                    data.airtable_record_id,
                    f"Failed: Queue Full ({current_queue_size}/{MAX_QUEUE_SIZE})"
                ))
            raise HTTPException(
                status_code=503,
                detail=f"Research queue is full ({current_queue_size}/{MAX_QUEUE_SIZE} jobs). Please try again later."
            )

        logger.info(f"✅ Webhook accepted for {company_value} (Airtable ID: {data.airtable_record_id}, Job ID: {job_id})")

        research_data = ResearchRequest(
            company=company_value,
            company_url=data.company_url,
            industry=data.industry,
            hq_location=data.hq_location
        )

        if data.airtable_record_id:
             asyncio.create_task(_update_airtable_status_queued(data.airtable_record_id, ResearchStatus.QUEUED))

        logger.info(f"🎯 Queuing research job for {company_value}... (Queue: {current_queue_size + 1}/{MAX_QUEUE_SIZE})")
        if data.use_local_context:
            logger.info(f"📂 Local context mode enabled for {company_value} - will use existing files")

        task = asyncio.create_task(
            run_job_with_semaphore(
                job_id,
                research_data,
                data.airtable_record_id,
                data.google_drive_folder_url,
                data.use_local_context
            )
        )
        task.add_done_callback(_log_task_exception)
        active_job_tasks[job_id] = {"task": task, "company": company_value, "airtable_record_id": data.airtable_record_id}

        return {
            "status": "Accepted",
            "message": f"Research for {company_value} queued or started. Job ID: {job_id}",
            "job_id": job_id
        }

    except Exception as e:
        logger.error(f"Error initiating research via webhook: {str(e)}", exc_info=True)
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=str(e))
# --- END MODIFIED ENDPOINT ---

# --- Debug Endpoints ---
@app.post("/webhook/debug/gdrive-test")
async def debug_gdrive_test(
    data: dict = Body(default={
        "folder_url": "https://drive.google.com/drive/folders/10OH-9dquxNwIj2EDVrpdTDtM4cgLQB5C"
    })
):
    """Debug endpoint for testing Google Drive uploads with minimal test data."""
    try:
        folder_url = data.get("folder_url")
        test_content = {
            "test_timestamp": datetime.now().isoformat(),
            "company": "Debug Test",
            "test_data": {
                "message": "This is a test upload from debug endpoint",
                "source": "debug/gdrive-test"
            }
        }

        filename = f"debug_upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        await upload_context_to_gdrive(test_content, folder_url, filename)
        
        return {
            "status": "success",
            "message": f"Test file '{filename}' uploaded successfully",
            "filename": filename,
            "folder_url": folder_url
        }
    except Exception as e:
        logger.error(f"GDrive debug upload failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to upload test file: {str(e)}"
        )

@app.post("/webhook/debug/mock-research")
async def debug_mock_research(
    data: dict = Body(default={
        "company": "Test Company",
        "folder_url": "https://drive.google.com/drive/folders/10OH-9dquxNwIj2EDVrpdTDtM4cgLQB5C"
    })
):
    """Debug endpoint for testing research pipeline with mock data."""
    try:
        company = data.get("company", "Test Company")
        folder_url = data.get("folder_url")
        
        mock_data = {
            "company": company,
            "timestamp": datetime.now().isoformat(),
            "company_brief_data": {
                "https://example.com/test": {
                    "title": "Test Document",
                    "content": "Test content for verification",
                    "score": 0.95,
                    "raw_content": "Extended test content",
                    "evaluation": {"overall_score": 0.95}
                }
            },
            "research_queries": {
                "company_brief": [
                    f"{company} annual revenue 2024 2025",
                    f"{company} business model"
                ]
            }
        }

        filename = f"{company.lower().replace(' ', '_')}_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        await upload_context_to_gdrive(mock_data, folder_url, filename)
        
        return {
            "status": "success",
            "message": f"Mock research data for {company} uploaded as '{filename}'",
            "filename": filename,
            "folder_url": folder_url
        }
    except Exception as e:
        logger.error(f"Mock research upload failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to upload mock research: {str(e)}"
        )
    

@app.post("/webhook/debug/gdrive-folder-info")
async def debug_gdrive_folder_info(
    data: dict = Body(default={
        "folder_url": "https://drive.google.com/drive/folders/10OH-9dquxNwIj2EDVrpdTDtM4cgLQB5C"
    })
):
    """Return metadata about a Drive folder so we can confirm Shared Drive membership and driveId."""
    try:
        folder_url = data.get("folder_url")
        if not folder_url:
            raise HTTPException(status_code=422, detail="folder_url is required")

        # Call the blocking helper in a thread to avoid blocking the event loop
        info = await asyncio.to_thread(inspect_drive_folder, folder_url)

        # Add a convenience check to see if our service account appears in permissions
        sa_email = None
        # Try to read from credentials file/env for a quick hint (non-fatal)
        try:
            import json as _json
            sa_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "gdrive_credentials.json")
            if os.path.exists(sa_file):
                _j = _json.load(open(sa_file))
                sa_email = _j.get('client_email')
        except Exception:
            sa_email = None

        is_service_account_member = False
        if sa_email:
            for p in info.get('permissions', []):
                if p.get('emailAddress') == sa_email:
                    is_service_account_member = True
                    break

        return {
            "status": "success",
            "folder_info": info,
            "service_account_email": sa_email,
            "service_account_is_member": is_service_account_member
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"GDrive folder inspection failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
# --- End Debug Endpoints ---

# --- FIX: ADDED THIS NEW ENDPOINT ---
@app.post("/webhook/debug/run-final-nodes")
async def debug_run_final_nodes(record_id: Optional[str] = None):
    """
    Triggers the Tagger and Airtable/GDrive Upload nodes using mock data.
    This tests the final part of the graph without using Tavily credits.
    """
    try:
        logger.info("--- Triggering final nodes (Tagger, Uploader) via debug endpoint ---")
        # This function runs the Tagger and the Graph's airtable_upload_node
        result = await run_airtable_debug_test(record_id)
        return result
    except Exception as e:
        logger.error(f"Error during final nodes debug run: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to run final nodes: {str(e)}"
        )
# --- END FIX ---

# Get email service instance
def get_email_service() -> EmailGeneratorService:
    """Get the singleton email service instance."""
    return EmailGeneratorService()

@app.options("/templates")
async def templates_options():
    """Handle CORS preflight requests for /templates endpoint."""
    response = JSONResponse(content=None)
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "*"
    return response

@app.get("/templates",
         responses={
             200: {
                 "description": "List of available templates",
                 "content": {
                     "application/json": {
                         "example": {
                             "CGF_METHANE_CALL": "Template for methane reduction initiatives",
                             "SUSTAINABILITY_INTRO": "Introduction template for sustainability prospects"
                         }
                     }
                 }
             },
             429: {"description": "Rate limit exceeded"},
             500: {"description": "Internal server error"}
         },
         tags=["Email Templates"])
@limiter.limit(f"{security_config.rate_limit_requests}/hour")
async def list_email_templates(request: Request):
    """
    Get a list of available email templates.
    
    Rate limited and requires JWT authentication.
    
    This endpoint returns a list of all available email templates with their descriptions.
    Templates are stored in a shared Google Drive folder and are dynamically updated.
    
    Returns:
        Dict[str, str]: Dictionary mapping template types to their descriptions
        
    Raises:
        HTTPException: If templates cannot be fetched or rate limit is exceeded
    """
    try:
        logger.info("Listing templates (requested by: Airtable extension)")
        
        # Get template manager and refresh templates from Google Drive
        template_manager = get_template_manager()
        await template_manager.refresh_templates()
        
        templates = template_manager.list_templates()
        logger.info(f"Found {len(templates)} templates")

        # Clean BOM characters from template descriptions
        cleaned_templates = {
            key: value.replace('\ufeff', '').strip() 
            for key, value in templates.items()
        }

        # Enable CORS for Airtable
        response = JSONResponse(content=cleaned_templates)
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "*"
        return response
        
    except RateLimitExceeded as e:
        logger.warning("Rate limit exceeded for Airtable extension request")
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded. Please try again later."
        )
    except Exception as e:
        logger.error(f"Error listing templates: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch email templates")

@app.options("/generate-outreach")
async def generate_outreach_options():
    """Handle CORS preflight requests for /generate-outreach endpoint."""
    response = JSONResponse(content=None)
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "*"
    response.headers["Access-Control-Max-Age"] = "3600"
    return response

@app.post("/generate-outreach", 
         responses={
             200: {"description": "Successfully generated email"},
             404: {"description": "Template not found"},
             422: {"description": "Invalid input data"},
             429: {"description": "Rate limit exceeded"},
             500: {"description": "Internal server error"}
         },
         tags=["Email Generation"])
@limiter.limit(f"{security_config.rate_limit_requests}/hour")
async def generate_outreach_email(
    request: Request,
    data: EmailGenerationRequest
):
    """
    Generate a personalized outreach email by synthesizing template, research, and Airtable context.
    
    Rate limited and requires JWT authentication.
    
    This endpoint combines three sources of information:
    1. Email template from a shared Google Drive folder
    2. Research context from a prospect-specific Google Drive folder
    3. Context from an Airtable record
    
    The AI model processes these inputs to create a compelling, personalized email.
    
    Args:
        data: The request data containing template type, contact info, and context
        current_user: Authenticated user information (from JWT token)
        
    Returns:
        EmailGenerationResponse containing the generated email and usage info
        
    Raises:
        HTTPException: For various error conditions (see response codes)
    """
    try:
        # Sanitize and validate inputs
        contact_name = sanitize_input(data.contact_name)
        if not contact_name:
            raise HTTPException(
                status_code=422,
                detail="Contact name is required and cannot be empty"
            )
            
        # Validate Google Drive folder URL
        if not validate_folder_url(str(data.google_drive_folder_url)):
            raise HTTPException(
                status_code=422,
                detail="Invalid Google Drive folder URL format"
            )
            
        # Log request
        logger.info(
            f"Generating outreach email for contact: {contact_name} "
            f"using template: {data.template_type}"
        )
        
        # Get email service instance
        email_service = get_email_service()
        
        # 1. Fetch email template
        template_result = await email_service.fetch_template(data.template_type)
        if not template_result:
            raise HTTPException(
                status_code=404,
                detail=f"Email template '{data.template_type}' not found"
            )
        template_name, template_content = template_result
        
        # 2. Fetch research context
        research_context = await email_service.fetch_research_context(
            str(data.google_drive_folder_url)
        )
        if not research_context:
            logger.warning(f"No research context found in folder: {data.google_drive_folder_url}")
        
        # 3. Generate email using template, research, and Airtable context
        # Sanitize Airtable context fields
        airtable_context = {
            k: sanitize_input(v) if isinstance(v, str) else v 
            for k, v in data.airtable_context.dict().items()
        }
        
        email_text, subject_line = await email_service.generate_email(
            template_content=template_content,
            research_context=research_context,
            airtable_context=airtable_context,
            contact_name=contact_name
        )
        
        if not email_text:
            raise HTTPException(
                status_code=500,
                detail="Failed to generate email content"
            )
        
        # 4. Return generated email with context usage info
        response = JSONResponse(content=EmailGenerationResponse(
            email_text=email_text,
            subject=subject_line,
            template_used=template_name,
            context_used={
                "template": bool(template_content),
                "research": bool(research_context),
                "airtable": True
            }
        ).dict())

        # Add CORS headers for Airtable
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "*"
        return response

    except HTTPException:
        raise
    except RateLimitExceeded as e:
        logger.warning("Rate limit exceeded for Airtable extension request")
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded. Please try again later."
        )
    except Exception as e:
        logger.error(
            f"Error generating outreach email: {e}", 
            exc_info=True
        )
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
async def ping():
    return {"message": "Alive"}

@app.get("/research/pdf/{filename}")
async def get_pdf(filename: str):
    pdf_path = os.path.join("pdfs", filename)
    if not os.path.exists(pdf_path):
        raise HTTPException(status_code=404, detail="PDF not found")
    return FileResponse(pdf_path, media_type='application/pdf', filename=filename)

@app.websocket("/research/ws/{job_id}")
async def websocket_endpoint(websocket: WebSocket, job_id: str):
    try:
        await websocket.accept()
        await manager.connect(websocket, job_id)

        if job_id in job_status:
            status = job_status[job_id]
            await manager.send_status_update(
                job_id,
                status=status["status"],
                message="Connected to status stream",
                error=status["error"],
                result=status["result"]
            )

        while True:
            try:
                await websocket.receive_text()
            except WebSocketDisconnect:
                manager.disconnect(websocket, job_id)
                break

    except Exception as e:
        logger.error(f"WebSocket error for job {job_id}: {str(e)}", exc_info=True)
        manager.disconnect(websocket, job_id)

@app.get("/research/{job_id}")
async def get_research(job_id: str):
    if not mongodb:
        raise HTTPException(status_code=501, detail="Database persistence not configured")
    job = mongodb.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Research job not found")
    return job

@app.get("/research/{job_id}/report")
async def get_research_report(job_id: str):
    if not mongodb:
        if job_id in job_status:
            result = job_status[job_id]
            if report := result.get("report"):
                return {"report": report}
        raise HTTPException(status_code=404, detail="Report not found")

    report = mongodb.get_report(job_id)
    if not report:
        raise HTTPException(status_code=404, detail="Research report not found")
    return report

@app.delete("/research/{job_id}")
async def cancel_research(job_id: str):
    """Cancel a running or queued research job."""
    if job_id not in active_job_tasks:
        raise HTTPException(status_code=404, detail="Job not found or already completed")

    job_info = active_job_tasks[job_id]
    task = job_info["task"]
    company = job_info.get("company", "Unknown")
    airtable_record_id = job_info.get("airtable_record_id")

    # Cancel the task
    task.cancel()

    # Update Airtable if applicable
    if airtable_record_id:
        try:
            await asyncio.to_thread(
                update_airtable_record,
                airtable_record_id,
                {'Research Status': ResearchStatus.CANCELLED}
            )
        except Exception as e:
            logger.warning(f"Failed to update Airtable status for cancelled job {job_id}: {e}")

    # Remove from tracking
    del active_job_tasks[job_id]

    logger.info(f"❌ Job {job_id} cancelled for {company}")

    return {
        "status": "cancelled",
        "job_id": job_id,
        "company": company,
        "message": f"Research job for {company} has been cancelled"
    }

@app.get("/jobs/active")
async def get_active_jobs():
    """Get list of all active (running or queued) jobs."""
    jobs = []
    for job_id, info in active_job_tasks.items():
        jobs.append({
            "job_id": job_id,
            "company": info.get("company", "Unknown"),
            "airtable_record_id": info.get("airtable_record_id"),
            "cancelled": info["task"].cancelled(),
            "done": info["task"].done()
        })

    return {
        "active_jobs": jobs,
        "total_active": len(jobs),
        "queue_stats": job_queue_stats
    }

@app.post("/generate-pdf")
async def generate_pdf(data: PDFGenerationRequest):
    """Generate a PDF from markdown content and stream it to the client."""
    try:
        success, result = pdf_service.generate_pdf_stream(data.report_content, data.company_name)
        if success:
            pdf_buffer, filename = result
            return StreamingResponse(
                pdf_buffer,
                media_type='application/pdf',
                headers={
                    'Content-Disposition': f'attachment; filename="{filename}"'
                }
            )
        else:
            raise HTTPException(status_code=500, detail=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/metrics")
async def metrics():
    """Endpoint for Prometheus metrics."""
    return JSONResponse(content=performance_monitor.get_metrics())

@app.get("/health")
async def health():
    """
    Health check endpoint for monitoring.
    Returns system health, uptime, queue status, and API rate limit usage.
    """
    # Calculate queue health
    slots_available = job_semaphore._value
    utilization = ((MAX_CONCURRENT_JOBS - slots_available) / MAX_CONCURRENT_JOBS) * 100

    # Get rate limiter stats
    from backend.utils.rate_limiter import get_all_limiter_stats
    rate_limiter_stats = get_all_limiter_stats()

    current_queue_size = len(active_job_tasks)
    queue_capacity_percent = (current_queue_size / MAX_QUEUE_SIZE) * 100

    return {
        "status": "healthy",
        "uptime": str(performance_monitor.get_uptime()),
        "version": "1.0.0",
        "queue": {
            "slots_available": slots_available,
            "max_concurrent": MAX_CONCURRENT_JOBS,
            "utilization_percent": round(utilization, 1),
            "current_running": job_queue_stats["current_running"],
            "current_queue_size": current_queue_size,
            "max_queue_size": MAX_QUEUE_SIZE,
            "queue_capacity_percent": round(queue_capacity_percent, 1),
            "total_queued": job_queue_stats["total_queued"],
            "total_completed": job_queue_stats["total_completed"],
            "total_failed": job_queue_stats["total_failed"],
            "total_rejected": job_queue_stats["total_rejected"],
            "peak_concurrent": job_queue_stats["peak_concurrent"],
            "success_rate": round(
                (job_queue_stats["total_completed"] / max(job_queue_stats["total_queued"], 1)) * 100, 1
            ) if job_queue_stats["total_queued"] > 0 else 0
        },
        "api_rate_limits": rate_limiter_stats
    }

def start():
    """Start the application with monitoring."""
    # Start Prometheus metrics server
    start_http_server(8001)  # Metrics available on port 8001
    
    # Start the FastAPI application
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)

@app.post("/auth/token", 
         responses={
             200: {"description": "Successfully created auth token"},
             401: {"description": "Invalid API key"},
             500: {"description": "Internal server error"}
         },
         tags=["Authentication"])
async def get_auth_token(data: TokenRequest):
    """
    Create a new JWT auth token using an API key.
    
    Args:
        data: TokenRequest containing email and API key
        
    Returns:
        Token containing the JWT access token
        
    Raises:
        HTTPException: If API key verification fails
    """
    try:
        if not verify_api_key(data.api_key):
            raise HTTPException(
                status_code=401,
                detail="Invalid API key"
            )
            
        # Create token with user email embedded
        token_data = {"email": data.email}
        token = create_access_token(
            data=token_data,
            secret_key=config.JWT_SECRET_KEY
        )
        
        return Token(access_token=token)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating auth token: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

if __name__ == "__main__":
    start()

# ===== AIRTABLE BLOCK PROXY =====
# Proxy endpoint to forward requests to the local Airtable block dev server
# This allows Airtable (https://airtable.com) to access the block via the ngrok HTTPS URL
@app.api_route("/block/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"])
async def proxy_to_block_server(path: str, request: Request):
    """
    Reverse proxy to forward all /block/* requests to the local Airtable block dev server.
    This resolves CORS issues by exposing the block server through the existing ngrok HTTPS tunnel.
    """
    # Target URL for the local block development server
    target_url = f"http://localhost:9000/{path}"
    
    # Get query parameters
    query_params = str(request.url.query)
    if query_params:
        target_url += f"?{query_params}"
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            # Forward the request to the local block server
            response = await client.request(
                method=request.method,
                url=target_url,
                headers={k: v for k, v in request.headers.items() 
                        if k.lower() not in ['host', 'connection']},
                content=await request.body(),
                follow_redirects=True
            )
            
            # Create response with the same status code and content
            proxy_response = JSONResponse(
                content=response.json() if response.headers.get('content-type', '').startswith('application/json') else response.text,
                status_code=response.status_code
            )
            
            # Copy relevant headers from the block server response
            for header, value in response.headers.items():
                if header.lower() not in ['content-encoding', 'content-length', 'transfer-encoding', 'connection']:
                    proxy_response.headers[header] = value
            
            # Ensure CORS headers are set
            proxy_response.headers["Access-Control-Allow-Origin"] = "*"
            proxy_response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, PATCH, OPTIONS"
            proxy_response.headers["Access-Control-Allow-Headers"] = "*"
            
            return proxy_response
            
    except httpx.RequestError as e:
        logger.error(f"Error proxying request to block server: {e}")
        raise HTTPException(
            status_code=502, 
            detail=f"Failed to connect to block development server: {str(e)}"
        )