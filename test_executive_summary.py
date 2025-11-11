#!/usr/bin/env python3
"""
Test script for executive summary generation.
Tests the new AI-powered 1-2 page executive summary feature.
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from backend.nodes.executive_summary import ExecutiveSummaryNode
from backend.classes.state import ResearchState


def load_research_data(json_path: str) -> dict:
    """Load research data from JSON file."""
    logger.info(f"Loading research data from: {json_path}")
    with open(json_path, 'r') as f:
        data = json.load(f)
    logger.info(f"Loaded {len(str(data))} characters of research data")
    return data


def create_test_state(research_data: dict) -> ResearchState:
    """Create a test state from loaded research data."""
    
    # Extract briefings from the data if they exist
    state = {
        'company': research_data.get('company_identity', {}).get('name', 'Unknown Company'),
        'company_url': research_data.get('company_identity', {}).get('website', ''),
        'industry': research_data.get('company_identity', {}).get('industry', 'Unknown'),
        'hq_location': research_data.get('company_identity', {}).get('headquarters', 'Unknown'),
        
        # These would normally come from the briefing node
        # For now, we'll create simple briefings from the raw data
        'company_brief_briefing': _create_company_briefing(research_data),
        'news_signal_briefing': _create_news_briefing(research_data),
        'flw_sustainability_briefing': _create_flw_briefing(research_data),
        'contact_briefing': _create_contact_briefing(research_data),
        'engagement_briefing': _create_engagement_briefing(research_data),
        
        'messages': [],
        'websocket_manager': None,
        'job_id': None
    }
    
    return state


def _create_company_briefing(data: dict) -> str:
    """Create a simple company briefing from official content."""
    company_info = data.get('official_content', {}).get('company_information', {})
    
    parts = []
    for url, content in list(company_info.items())[:3]:  # Just first 3 sources
        title = content.get('title', 'Company Information')
        text = content.get('content', content.get('raw_content', ''))[:500]
        parts.append(f"**{title}**\n{text}\n")
    
    return '\n'.join(parts) if parts else "Limited company information available."


def _create_news_briefing(data: dict) -> str:
    """Create a simple news briefing from recent developments."""
    news = data.get('recent_developments', {}).get('news_coverage', {})
    
    parts = []
    for url, content in list(news.items())[:3]:  # Just first 3 news items
        title = content.get('title', 'News Item')
        text = content.get('content', content.get('raw_content', ''))[:500]
        parts.append(f"**{title}**\n{text}\n")
    
    return '\n'.join(parts) if parts else "No recent news available."


def _create_flw_briefing(data: dict) -> str:
    """Create a simple FLW briefing from sustainability reporting."""
    sustainability = data.get('official_content', {}).get('sustainability_reporting', {})
    
    parts = []
    for url, content in list(sustainability.items())[:3]:  # Just first 3 sources
        title = content.get('title', 'Sustainability Report')
        text = content.get('content', content.get('raw_content', ''))[:500]
        parts.append(f"**{title}**\n{text}\n")
    
    return '\n'.join(parts) if parts else "Limited sustainability information available."


def _create_contact_briefing(data: dict) -> str:
    """Create a simple contact briefing from key personnel."""
    contacts = data.get('key_personnel', {}).get('identified_contacts', {})
    
    parts = []
    for url, content in list(contacts.items())[:2]:  # Just first 2 sources
        extracted = content.get('extracted_contacts', [])
        if extracted:
            parts.append("**Identified Contacts:**")
            for contact in extracted[:5]:  # Max 5 contacts
                name = contact.get('name', 'Unknown')
                title = contact.get('title', 'Unknown Title')
                parts.append(f"- {name}, {title}")
    
    return '\n'.join(parts) if parts else "No contacts identified."


def _create_engagement_briefing(data: dict) -> str:
    """Create a simple engagement briefing from partnerships."""
    partnerships = data.get('engagement_signals', {}).get('partnerships_and_initiatives', {})
    
    parts = []
    for url, content in list(partnerships.items())[:3]:  # Just first 3 sources
        title = content.get('title', 'Partnership/Initiative')
        text = content.get('content', content.get('raw_content', ''))[:500]
        parts.append(f"**{title}**\n{text}\n")
    
    return '\n'.join(parts) if parts else "Limited partnership information available."


async def test_executive_summary():
    """Test executive summary generation with real data."""
    
    # Load the actual research JSON
    json_path = "/Users/erikivester/company-research-agent/archer_daniel_midlands_research_20251110_214430.json"
    
    if not os.path.exists(json_path):
        logger.error(f"Research JSON not found at: {json_path}")
        return
    
    # Load and prepare data
    research_data = load_research_data(json_path)
    state = create_test_state(research_data)
    
    logger.info(f"Testing executive summary for: {state['company']}")
    logger.info(f"Industry: {state['industry']}")
    logger.info(f"Location: {state['hq_location']}")
    
    # Initialize executive summary node
    exec_node = ExecutiveSummaryNode()
    
    # Generate summary
    logger.info("Generating executive summary...")
    updated_state = await exec_node.generate_executive_summary(state)
    
    # Get the generated summary
    summary = updated_state.get('executive_summary', '')
    pdf_buffer = updated_state.get('executive_summary_pdf')
    
    if summary:
        logger.info(f"✅ Successfully generated executive summary ({len(summary)} chars)")
        
        # Save to file
        output_path = f"/Users/erikivester/company-research-agent/pdfs/executive_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
        with open(output_path, 'w') as f:
            f.write(summary)
        
        logger.info(f"✅ Saved executive summary to: {output_path}")
        
        # Save PDF if generated
        if pdf_buffer:
            pdf_path = output_path.replace('.md', '.pdf')
            with open(pdf_path, 'wb') as f:
                f.write(pdf_buffer.getvalue())
            logger.info(f"✅ Saved executive summary PDF to: {pdf_path}")
            
            # Open the PDF
            import subprocess
            subprocess.run(['open', pdf_path])
        
        # Print preview
        print("\n" + "="*80)
        print("EXECUTIVE SUMMARY PREVIEW")
        print("="*80)
        print(summary[:1000] + "..." if len(summary) > 1000 else summary)
        print("="*80 + "\n")
        
        return output_path
    else:
        logger.error("❌ Failed to generate executive summary")
        return None


if __name__ == "__main__":
    asyncio.run(test_executive_summary())
