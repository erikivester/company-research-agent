from fastapi import APIRouter, HTTPException
from typing import Dict, Any
import logging
from datetime import datetime

from .utils.gdrive_uploader import upload_context_to_gdrive

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/webhook/debug/gdrive-test")
async def test_gdrive_upload(folder_url: str, test_content: Dict[str, Any] = None):
    """Debug endpoint for testing Google Drive uploads."""
    try:
        if test_content is None:
            test_content = {
                "test_timestamp": datetime.now().isoformat(),
                "test_data": {
                    "message": "This is a test upload",
                    "source": "debug endpoint"
                }
            }

        filename = f"debug_upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        await upload_context_to_gdrive(test_content, folder_url, filename)
        
        return {
            "status": "success",
            "message": f"Test file '{filename}' uploaded successfully",
            "filename": filename
        }
    except Exception as e:
        logger.error(f"GDrive debug upload failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to upload test file: {str(e)}"
        )