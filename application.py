import asyncio
import logging
import os
import uuid
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel

from backend.graph import Graph
from backend.services.mongodb import MongoDBService
from backend.services.pdf_service import PDFService
from backend.services.websocket_manager import WebSocketManager

# ⬇️ This import remains as it's used by the GRAPH, not directly here ⬇️
from backend.airtable_uploader import update_airtable_record
from backend.debug_airtable import run_airtable_debug_test
# ⬆️ END AIRTABLE IMPORTS ⬆️

# Load environment variables from .env file at startup
env_path = Path(__file__).parent / '.env'
if env_path.exists():
    load_dotenv(dotenv_path=env_path, override=True)

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
logger.addHandler(console_handler)

app = FastAPI(title="Tavily Company Research API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

manager = WebSocketManager()
pdf_service = PDFService({"pdf_output_dir": "pdfs"})

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

# --- NEW: Pydantic Model for Webhook Input ---
class AirtableWebhookInput(ResearchRequest):
    """Extends ResearchRequest to include the Airtable Record ID."""
    airtable_record_id: str | None = None
# --- END NEW MODEL ---

class PDFGenerationRequest(BaseModel):
    report_content: str
    company_name: str | None = None

# ----------------------------------------------------
# 🟢 CONCURRENCY CONTROL SETUP
# ----------------------------------------------------
# Define the maximum number of research jobs allowed to run concurrently.
MAX_CONCURRENT_JOBS = 5 
job_semaphore = asyncio.Semaphore(MAX_CONCURRENT_JOBS)

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


async def run_job_with_semaphore(job_id: str, data: ResearchRequest, airtable_record_id: str | None):
    """Acquires semaphore, runs the core research logic, and releases semaphore."""
    
    # 1. Acquire the semaphore (blocks if limit reached)
    await job_semaphore.acquire()
    logger.info(f"SEMAPHORE ACQUIRED: Job {job_id} starting. {job_semaphore._value} slots remaining.")

    try:
        # 2. **CRITICAL:** Update status from 'Queued' to 'In Progress' (via Grounding node)
        # We don't need an explicit update here as the GroundingNode handles the first "In Progress" status.
        
        # 3. Run the actual research logic
        await process_research(job_id, data, airtable_record_id)
        
    except Exception as e:
        logger.error(f"Job {job_id} failed during execution: {e}")
    finally:
        # 4. Release the semaphore (executed even if process_research fails)
        job_semaphore.release()
        logger.info(f"SEMAPHORE RELEASED: Job {job_id} finished. {job_semaphore._value} slots available.")


@app.options("/research")
async def preflight():
    response = JSONResponse(content=None, status_code=200)
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    return response

# --- CORE RESEARCH LOGIC: process_research (No functional change) ---
async def process_research(job_id: str, data: ResearchRequest, airtable_record_id: str | None = None):
    try:
        if mongodb:
            # Include airtable_record_id in MongoDB job details
            job_details = data.dict()
            job_details['airtable_record_id'] = airtable_record_id
            mongodb.create_job(job_id, job_details)
            
        await asyncio.sleep(1)  # Allow WebSocket connection

        await manager.send_status_update(job_id, status="processing", message="Starting research")

        graph = Graph(
            company=data.company,
            url=data.company_url,
            industry=data.industry,
            hq_location=data.hq_location,
            websocket_manager=manager,
            job_id=job_id
        )

        # CRITICAL: Pass airtable_record_id to the Graph's thread config
        thread_config = {}
        if airtable_record_id:
             thread_config = {"configurable": {"airtable_record_id": airtable_record_id}}

        state = {}
        async for s in graph.run(thread=thread_config): # Pass the config here
            state.update(s)
        
        # Look for the compiled report in either location.
        report_content = state.get('report') or (state.get('editor') or {}).get('report')
        
        # Airtable upload is now handled inside the graph.run() call above.
        # We don't need to call upload_to_airtable here anymore.

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
                message="Research completed successfully.", # <-- Simplified message
                result={
                    "report": report_content,
                    "company": data.company
                    # Airtable status is now handled internally by the graph node's logging
                }
            )
        else:
            logger.error(f"Research completed without finding report. State keys: {list(state.keys())}")
            logger.error(f"Editor state: {state.get('editor', {})}")
            
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
# --- END CORE RESEARCH LOGIC ---


@app.post("/research")
async def research(data: ResearchRequest):
    try:
        logger.info(f"Received research request for {data.company}")
        job_id = str(uuid.uuid4())
        # Pass UI-initiated runs directly to the semaphore queue
        asyncio.create_task(run_job_with_semaphore(job_id, data, airtable_record_id=None)) 

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

# --- MODIFIED: Webhook Endpoint for Airtable Automation ---
@app.post("/webhook/start-research")
async def start_research_webhook(data: AirtableWebhookInput):
    """
    Accepts a POST request (e.g., from an Airtable Automation webhook) 
    and queues the research pipeline using the semaphore.
    """
    try:
        logger.info(f"Received webhook request for {data.company} (Airtable ID: {data.airtable_record_id})")
        
        job_id = str(uuid.uuid4())
        
        research_data = ResearchRequest(
            company=data.company,
            company_url=data.company_url,
            industry=data.industry,
            hq_location=data.hq_location
        )
        
        # 1. CRITICAL: Immediately update Airtable status to "Queued" 
        if data.airtable_record_id:
             asyncio.create_task(_update_airtable_status_queued(data.airtable_record_id, "Queued"))

        # 2. Start the job using the SEMAPHORE WRAPPER (This is now the non-blocking part)
        asyncio.create_task(run_job_with_semaphore(job_id, research_data, data.airtable_record_id))

        return {
            "status": "Accepted",
            "message": f"Research for {data.company} queued or started. Job ID: {job_id}",
            "job_id": job_id
        }

    except Exception as e:
        logger.error(f"Error initiating research via webhook: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
# --- END MODIFIED ENDPOINT ---

# 🟢 NEW DEBUG ENDPOINT: /debug/airtable-test
@app.post("/debug/airtable-test")
async def debug_airtable_test(record_id: str | None = None):
    """
    Triggers the logic from test_airtable.py with mock data.
    
    Pass ?record_id=recXXXXX in the query or body to test an UPDATE.
    If no ID is passed, it tests an INSERT.
    """
    # The record_id can be passed via a query parameter or an empty Pydantic model body
    # We accept it as a query parameter for simplicity here.
    return await run_airtable_debug_test(record_id)

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

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)