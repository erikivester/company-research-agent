import asyncio
import json
import logging
from datetime import datetime
from backend.utils.gdrive_uploader import upload_context_to_gdrive
from backend.classes import ResearchState

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_gdrive_upload():
    # Simulate a complete research context with all expected data structures
    test_context = {
        "company": "Whole Foods",
        "timestamp": datetime.now().isoformat(),
        "company_brief_data": {
            "https://example.com/wholefoodsinfo": {
                "title": "Whole Foods Market Company Overview",
                "content": "Test content about Whole Foods Market...",
                "score": 0.95,
                "raw_content": "Extended test content about Whole Foods Market...",
                "evaluation": {"overall_score": 0.95}
            }
        },
        "curated_company_brief_data": {
            "https://example.com/wholefoodsinfo": {
                "title": "Whole Foods Market Company Overview",
                "content": "Curated test content...",
                "score": 0.95,
                "raw_content": "Extended curated content...",
                "evaluation": {"overall_score": 0.95}
            }
        },
        "news_signal_data": {
            "https://example.com/wholefoodsnews": {
                "title": "Whole Foods Market Sustainability Initiative",
                "content": "Test news content...",
                "score": 0.88,
                "raw_content": "Extended test news content...",
                "evaluation": {"overall_score": 0.88}
            }
        },
        "research_queries": {
            "company_brief": [
                "Whole Foods annual revenue 2024 2025",
                "Whole Foods major financial health signals 2024 2025"
            ],
            "news_signal": [
                "Whole Foods FLW climate goals news 2024 2025",
                "Whole Foods sustainability news 2024 2025"
            ]
        },
        "briefings": {
            "company_brief": "Company Overview: Whole Foods Market...",
            "news_signal": "Recent News: Whole Foods has announced..."
        },
        "classifications": {
            "industry": "Retail - Grocery",
            "region": "North America",
            "revenue_band": "$10B-$50B"
        }
    }

    # Your Google Drive folder URL
    folder_url = "https://drive.google.com/drive/folders/10OH-9dquxNwIj2EDVrpdTDtM4cgLQB5C"
    
    # Create a test filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"test_research_whole_foods_{timestamp}.json"

    try:
        print(f"\n🔍 Testing GDrive upload with complete research context:")
        print(f"- Folder URL: {folder_url}")
        print(f"- Filename: {filename}")
        print("- Content sections included:")
        for key in test_context.keys():
            print(f"  • {key}")
        
        print("\n📤 Uploading...")
        
        # Attempt the upload
        await upload_context_to_gdrive(test_context, folder_url, filename)
        print("\n✅ Upload successful! Check your Google Drive folder.")
        
        # Print the path to look for
        print(f"\n🔍 Look for file: {filename}")
        
    except Exception as e:
        print(f"\n❌ Upload failed: {str(e)}")
        logger.error("Upload failed", exc_info=True)
        raise

def print_test_instructions():
    print("""
🔧 Test Setup Instructions:
--------------------------
1. Make sure your Google credentials are set:
   export GOOGLE_APPLICATION_CREDENTIALS="./gdrive_credentials.json"

2. Run this test file:
   python3 test_gdrive_upload.py

3. Check your Google Drive folder for the uploaded file.

💡 This test:
------------
- Creates a complete mock research context
- Includes all expected data structures
- Uses real timestamps
- Preserves proper nesting and data formats
- Tests the actual upload functionality
""")

if __name__ == "__main__":
    print_test_instructions()
    print("\n🚀 Starting Google Drive upload test...")
    asyncio.run(test_gdrive_upload())