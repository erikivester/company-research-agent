#!/usr/bin/env python3
"""
Test script for enhanced PDF generation.

This script loads your actual research data and generates a PDF
to verify the visual output and formatting.
"""
import asyncio
import io
import json
import logging
from datetime import datetime
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_research_data(json_path: str):
    """Load research data from a JSON file."""
    logger.info(f"Loading research data from: {json_path}")
    with open(json_path, 'r') as f:
        data = json.load(f)
    logger.info(f"Loaded data with keys: {list(data.keys())}")
    
    # Add a final_summary if it's missing by generating one from the data
    if 'final_summary' not in data or not data.get('final_summary'):
        logger.info("Generating final_summary from available data...")
        data['final_summary'] = generate_summary_from_data(data)
    
    return data


def generate_summary_from_data(data: dict) -> dict:
    """Generate a markdown summary from the research data, prioritizing briefings."""
    summary_parts = []
    
    # Check if we have communication_insights (which contains the briefings)
    comm_insights = data.get('communication_insights', {})
    
    # Try to get the individual briefings
    company_brief = comm_insights.get('company_brief_briefing', '')
    news_brief = comm_insights.get('news_signal_briefing', '')
    flw_brief = comm_insights.get('flw_sustainability_briefing', '')
    contact_brief = comm_insights.get('contact_briefing', '')
    engagement_brief = comm_insights.get('engagement_briefing', '')
    
    # If we have briefings, use them
    if any([company_brief, news_brief, flw_brief, contact_brief, engagement_brief]):
        if company_brief:
            summary_parts.append("## Company Brief\n")
            summary_parts.append(company_brief)
            summary_parts.append("\n\n")
        
        if news_brief:
            summary_parts.append("## News & Signals\n")
            summary_parts.append(news_brief)
            summary_parts.append("\n\n")
        
        if flw_brief:
            summary_parts.append("## Food Loss & Waste / Sustainability\n")
            summary_parts.append(flw_brief)
            summary_parts.append("\n\n")
        
        if contact_brief:
            summary_parts.append("## Key Contacts\n")
            # Contact brief might be JSON, try to format it
            try:
                import json
                contacts = json.loads(contact_brief) if isinstance(contact_brief, str) else contact_brief
                if isinstance(contacts, list):
                    for contact in contacts:
                        if isinstance(contact, dict):
                            name = contact.get('name', 'Unknown')
                            title = contact.get('title', '')
                            summary_parts.append(f"* **{name}** - {title}\n")
                else:
                    summary_parts.append(str(contact_brief))
            except:
                summary_parts.append(str(contact_brief))
            summary_parts.append("\n\n")
        
        if engagement_brief:
            summary_parts.append("## Partnerships & Engagement\n")
            summary_parts.append(engagement_brief)
            summary_parts.append("\n\n")
    
    # Fallback: generate from raw data if no briefings
    if not summary_parts:
        # Company Brief section
        company_info = data.get('official_content', {}).get('company_information', {})
        if company_info:
            summary_parts.append("## Company Overview\n")
            for url, info in list(company_info.items())[:3]:  # First 3 sources
                if isinstance(info, dict) and info.get('content'):
                    content = info['content']
                    # Clean up content
                    content = content.replace('*', '').replace('#', '')[:300]
                    summary_parts.append(f"* {content}...\n")
            summary_parts.append("\n")
        
        # Recent Developments
        news = data.get('recent_developments', {}).get('news_coverage', {})
        if news:
            summary_parts.append("## Recent Developments\n")
            for url, info in list(news.items())[:3]:
                if isinstance(info, dict):
                    title = info.get('title', 'News Item')
                    content = info.get('content', '')[:150]
                    summary_parts.append(f"* **{title}:** {content}...\n")
            summary_parts.append("\n")
        
        # Key Personnel
        contacts = data.get('key_personnel', {}).get('identified_contacts', {})
        if contacts:
            summary_parts.append("## Key Personnel\n")
            for url, info in list(contacts.items())[:5]:
                if isinstance(info, dict):
                    summary_parts.append(f"* {info.get('title', info.get('content', 'Contact'))}\n")
            summary_parts.append("\n")
        
        # Engagement Signals
        engagements = data.get('engagement_signals', {}).get('partnerships_and_initiatives', {})
        if engagements:
            summary_parts.append("## Partnerships & Engagement\n")
            for url, info in list(engagements.items())[:3]:
                if isinstance(info, dict) and info.get('content'):
                    content = info['content'][:200]
                    summary_parts.append(f"* {content}...\n")
    
    markdown_report = ''.join(summary_parts) if summary_parts else "## Research Summary\n\nResearch data compiled from multiple sources."
    
    return {
        'markdown_report': markdown_report
    }


async def test_pdf_generation(json_file: str):
    """Test the PDF generation with actual research data."""
    try:
        # Import the PDF generation functions
        from backend.utils.executive_summary_pdf import create_executive_summary_pdf
        from backend.utils.enhanced_pdf import create_enhanced_research_pdf
        
        logger.info("Loading research data from JSON file...")
        research_data = load_research_data(json_file)
        
        company_name = research_data.get('company_identity', {}).get('name', 'Company')
        
        output_files = []
        
        # Generate Executive Summary (1-2 pages)
        logger.info("Generating Executive Summary PDF (1-2 pages)...")
        summary_buffer = io.BytesIO()
        await create_executive_summary_pdf(research_data, summary_buffer)
        
        # Save executive summary
        output_dir = Path(__file__).parent / "pdfs"
        output_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        summary_file = output_dir / f"{company_name.lower().replace(' ', '_')}_executive_summary_{timestamp}.pdf"
        
        summary_buffer.seek(0)
        with open(summary_file, 'wb') as f:
            f.write(summary_buffer.read())
        
        logger.info(f"✅ Executive Summary generated: {summary_file}")
        logger.info(f"📄 File size: {summary_file.stat().st_size / 1024:.2f} KB")
        output_files.append(summary_file)
        
        # Generate Detailed Report (multi-page) - optional
        logger.info("\nGenerating Detailed Report PDF (multi-page)...")
        detailed_buffer = io.BytesIO()
        await create_enhanced_research_pdf(research_data, detailed_buffer)
        
        # Save detailed report
        detailed_file = output_dir / f"{company_name.lower().replace(' ', '_')}_detailed_report_{timestamp}.pdf"
        
        detailed_buffer.seek(0)
        with open(detailed_file, 'wb') as f:
            f.write(detailed_buffer.read())
        
        logger.info(f"✅ Detailed Report generated: {detailed_file}")
        logger.info(f"📄 File size: {detailed_file.stat().st_size / 1024:.2f} KB")
        output_files.append(detailed_file)
        
        return output_files
        
    except Exception as e:
        logger.error(f"❌ PDF generation failed: {e}", exc_info=True)
        raise


async def main():
    """Main entry point."""
    import sys
    
    print("=" * 70)
    print("PDF Generation Test - Executive Summary & Detailed Report")
    print("=" * 70)
    print()
    
    # Default to the ADM research file, or accept command line argument
    if len(sys.argv) > 1:
        json_file = sys.argv[1]
    else:
        json_file = "archer_daniel_midlands_research_20251110_214430.json"
    
    json_path = Path(json_file)
    if not json_path.exists():
        print(f"❌ Error: JSON file not found: {json_path}")
        print(f"\nUsage: python {sys.argv[0]} [path/to/research.json]")
        sys.exit(1)
    
    print(f"Using research data: {json_path}")
    print()
    
    output_files = await test_pdf_generation(str(json_path))
    
    print()
    print("=" * 70)
    print("Test completed!")
    print("=" * 70)
    print()
    print("Generated PDFs:")
    for i, file in enumerate(output_files, 1):
        print(f"{i}. {file}")
    print()
    print("Next steps:")
    print("1. Open the Executive Summary (1-2 pages, AI-generated)")
    print("2. Compare with the Detailed Report (multi-page, comprehensive)")
    print("3. The JSON file remains the source of truth")
    print()
    print(f"Open files with: open {output_files[0].parent}/")


if __name__ == "__main__":
    asyncio.run(main())
