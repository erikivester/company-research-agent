import asyncio
import os
import sys
from pathlib import Path

# Add the parent directory to PYTHONPATH
sys.path.append(str(Path(__file__).parent.parent))

from backend.utils.email_templates import EmailTemplateManager

async def test_template_reading():
    print("Initializing template manager...")
    template_manager = EmailTemplateManager()
    
    try:
        print("Refreshing templates...")
        await template_manager.refresh_templates()
        
        templates = template_manager.list_templates()
        print("\nFound templates:")
        if templates:
            for template_type, description in templates.items():
                print(f"\nTemplate Type: {template_type}")
                print(f"Description: {description}")
        else:
            print("No templates found in the folder")
            
    except Exception as e:
        print(f"\nError occurred: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(test_template_reading())