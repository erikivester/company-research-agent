"""Enhanced PDF generation with AI-optimized context and formatting"""
import io
import json
import logging
from datetime import datetime
from typing import Dict, Any, List

from reportlab.lib import colors
from .context_analyzer import ContextAnalyzer
from reportlab.lib.enums import TA_LEFT, TA_CENTER
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle,
    PageBreak, Image, ListFlowable, ListItem
)

logger = logging.getLogger(__name__)

def format_source_content(url: str, data: Dict[str, Any], body_style: ParagraphStyle) -> List[Any]:
    """Helper function to format source content with metadata."""
    elements = []
    
    # Source header
    header_text = f"Source: {url}"
    if data.get("title"):
        header_text = f"{data['title']}\n{header_text}"
    elements.append(Paragraph(header_text, body_style))
    
    # Metadata table
    meta_data = [
        ["Score", f"{data.get('relevance', 0):.2f}"],
        ["Query", data.get("query_context", "N/A")],
        ["Timestamp", data.get("timestamp", "N/A")],
        ["Source Type", data.get("source_type", "N/A")]
    ]
    t = Table(meta_data, colWidths=[1.5*inch, 4*inch])
    t.setStyle(TableStyle([
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('BACKGROUND', (0, 0), (0, -1), colors.lightgrey),
        ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 0), (-1, -1), 8),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('PADDING', (0, 0), (-1, -1), 4),
    ]))
    elements.append(t)
    elements.append(Spacer(1, 0.1*inch))
    
    # Raw content section
    if data.get("raw_content"):
        elements.append(Paragraph("Raw Content:", body_style))
        elements.append(Paragraph(data["raw_content"], body_style))
        elements.append(Spacer(1, 0.1*inch))
    
    # Processed content section
    if data.get("content"):
        elements.append(Paragraph("Processed Content:", body_style))
        elements.append(Paragraph(data["content"], body_style))
    
    elements.append(Spacer(1, 0.2*inch))
    return elements

async def create_enhanced_research_pdf(research_data: Dict[str, Any], output: io.BytesIO) -> None:
    """
    Creates a comprehensive PDF optimized for AI consumption with enhanced context.
    
    Args:
        research_data: The complete research data dictionary
        output: BytesIO object to write the PDF to
    """
    # First, enhance the context using the analyzer
    analyzer = ContextAnalyzer()
    enhanced_data = await analyzer.prepare_context(research_data)
    
    # Add the enhanced data as a JSON attachment to the PDF
    json_enhanced = json.dumps(enhanced_data, indent=2)
    
    # Track data relationships and context preservation
    context_metadata = {
        "version": "2.0",
        "timestamp": datetime.now().isoformat(),
        "context_preservation": {
            "raw_data_included": True,
            "relationship_mapping": True,
            "source_credibility": True,
            "query_context": True
        },
        "enhancement_status": "polished" if enhanced_data.get("polished_content") else "raw"
    }
    # Document setup
    doc = SimpleDocTemplate(
        output,
        pagesize=A4,
        rightMargin=0.75*inch,
        leftMargin=0.75*inch,
        topMargin=0.75*inch,
        bottomMargin=0.75*inch
    )

    # Styles
    styles = getSampleStyleSheet()
    
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        spaceAfter=30,
        alignment=TA_CENTER,
        textColor=colors.HexColor('#2C3E50')
    )
    
    section_style = ParagraphStyle(
        'Section',
        parent=styles['Heading2'],
        fontSize=16,
        spaceBefore=20,
        spaceAfter=10,
        textColor=colors.HexColor('#2980B9')
    )
    
    subsection_style = ParagraphStyle(
        'Subsection',
        parent=styles['Heading3'],
        fontSize=14,
        spaceBefore=15,
        spaceAfter=8,
        textColor=colors.HexColor('#34495E')
    )
    
    body_style = ParagraphStyle(
        'CustomBody',
        parent=styles['Normal'],
        fontSize=10,
        leading=14,
        alignment=TA_LEFT,
        textColor=colors.HexColor('#2C3E50')
    )

    table_style = TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#F4F6F7')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.HexColor('#2C3E50')),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 10),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -1), 10),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('TOPPADDING', (0, 0), (-1, 0), 12),
        ('BOTTOMPADDING', (0, 1), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#E8ECF0')),
    ])

    # Build the document
    story = []
    
    # Title Page
    company = research_data['company_identity']['name']
    story.append(Paragraph(f"{company} Research Report", title_style))
    story.append(Spacer(1, 20))
    
    # Table of Contents
    story.append(Paragraph("Table of Contents", section_style))
    toc_items = [
        "1. Company Profile",
        "2. Research Process & Queries",
        "3. Raw Research Data",
        "   3.1 Company Information",
        "   3.2 News & Recent Developments",
        "   3.3 Sustainability & FLW Data",
        "   3.4 Contact Information",
        "   3.5 Engagement & Partnerships",
        "4. Source Analysis",
        "5. Final Research Summary"
    ]
    for item in toc_items:
        story.append(Paragraph(item, body_style))
    story.append(PageBreak())
    
    # Company Identity Table
    identity_data = research_data['company_identity']
    company_info = [
        ['Company Profile', ''],
        ['Headquarters', identity_data.get('headquarters', 'Not Available')],
        ['Industry', identity_data.get('industry', 'Not Available')],
        ['Region', identity_data.get('region', 'Not Available')],
        ['Scale', identity_data.get('scale', 'Not Available')],
        ['Website', identity_data.get('website', 'Not Available')],
    ]
    
    t = Table(company_info, colWidths=[2*inch, 4*inch])
    t.setStyle(table_style)
    story.append(t)
    story.append(PageBreak())

    # Executive Summary Section
    story.append(Paragraph("Executive Summary", section_style))
    
    # Company Information
    if research_data['official_content']['company_information']:
        story.append(Paragraph("Company Overview", subsection_style))
        for url, info in research_data['official_content']['company_information'].items():
            if info.get('content'):
                story.append(Paragraph(info['content'], body_style))
                story.append(Spacer(1, 10))

    # Recent Developments
    if research_data['recent_developments']['news_coverage']:
        story.append(Paragraph("Recent Developments", section_style))
        for url, news in research_data['recent_developments']['news_coverage'].items():
            if news.get('content'):
                story.append(Paragraph(f"• {news['content']}", body_style))
                story.append(Spacer(1, 8))
    
    story.append(PageBreak())

    # Key Personnel Section
    if research_data['key_personnel']['identified_contacts']:
        story.append(Paragraph("Key Personnel", section_style))
        for url, contact in research_data['key_personnel']['identified_contacts'].items():
            if contact.get('content'):
                story.append(Paragraph(f"• {contact['content']}", body_style))
                story.append(Spacer(1, 8))

    # Engagement Signals
    if research_data['engagement_signals']['partnerships_and_initiatives']:
        story.append(Paragraph("Engagement & Partnerships", section_style))
        for url, initiative in research_data['engagement_signals']['partnerships_and_initiatives'].items():
            if initiative.get('content'):
                story.append(Paragraph(f"• {initiative['content']}", body_style))
                story.append(Spacer(1, 8))

    # Source Credibility
    story.append(PageBreak())
    story.append(Paragraph("Research Sources", section_style))
    
    sources = research_data['source_credibility']['reference_info']
    if sources:
        source_data = [['Source', 'Credibility Score']]
        for url, info in sources.items():
            source_data.append([
                info['title'],
                f"{info['score']:.2f}"
            ])
        t = Table(source_data, colWidths=[5*inch, 1*inch])
        t.setStyle(table_style)
        story.append(t)

    # Research Process & Queries
    story.append(Paragraph("2. Research Process & Queries", section_style))
    meta = research_data['research_meta']
    
    # Query details by type
    story.append(Paragraph("Research Queries by Category:", subsection_style))
    for query_type, queries in meta['queries_used'].items():
        story.append(Paragraph(f"\n{query_type.replace('_', ' ').title()}:", body_style))
        for query in queries:
            story.append(Paragraph(f"• {query}", body_style))
    story.append(PageBreak())
    
    # Raw Research Data Sections
    story.append(Paragraph("3. Raw Research Data", section_style))
    
    # 3.1 Company Information
    story.append(Paragraph("3.1 Company Information", subsection_style))
    for url, data in research_data['official_content']['company_information'].items():
        story.extend(format_source_content(url, data, body_style))
    story.append(PageBreak())
    
    # 3.2 News & Recent Developments
    story.append(Paragraph("3.2 News & Recent Developments", subsection_style))
    for url, data in research_data['recent_developments']['news_coverage'].items():
        story.extend(format_source_content(url, data, body_style))
    story.append(PageBreak())
    
    # 3.3 Sustainability & FLW Data
    story.append(Paragraph("3.3 Sustainability & FLW Data", subsection_style))
    for url, data in research_data['official_content']['sustainability_reporting'].items():
        story.extend(format_source_content(url, data, body_style))
    story.append(PageBreak())
    
    # 3.4 Contact Information
    story.append(Paragraph("3.4 Contact Information", subsection_style))
    for url, data in research_data['key_personnel']['identified_contacts'].items():
        story.extend(format_source_content(url, data, body_style))
    if data.get("extracted_contacts"):
        story.append(Paragraph("Extracted Contacts:", body_style))
        for contact in data["extracted_contacts"]:
            story.append(Paragraph(f"• {contact}", body_style))
    story.append(PageBreak())
    
    # 3.5 Engagement & Partnerships
    story.append(Paragraph("3.5 Engagement & Partnerships", subsection_style))
    for url, data in research_data['engagement_signals']['partnerships_and_initiatives'].items():
        story.extend(format_source_content(url, data, body_style))
    story.append(PageBreak())
    
    # Source Analysis
    story.append(Paragraph("4. Source Analysis", section_style))
    story.append(Paragraph("Source Credibility Analysis:", subsection_style))
    
    # Detailed source analysis table
    source_analysis_data = [['Source', 'Type', 'Score', 'Domain']]
    for url, info in research_data['source_credibility']['reference_info'].items():
        source_analysis_data.append([
            info['title'],
            info.get('source_type', 'N/A'),
            f"{info['score']:.2f}",
            info['domain']
        ])
    
    t = Table(source_analysis_data, colWidths=[3*inch, 1*inch, 0.7*inch, 1.3*inch])
    t.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#F4F6F7')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.HexColor('#2C3E50')),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('PADDING', (0, 0), (-1, -1), 4),
    ]))
    story.append(t)
    story.append(PageBreak())
    
    # Final Research Summary (Original Report)
    story.append(Paragraph("5. Final Research Summary", section_style))
    story.append(Paragraph("Generated Research Report:", subsection_style))
    
    # Analysis Statistics
    stats = [
        ['Research Statistics', ''],
        ['Total Sources Analyzed', str(meta['successful_extractions'].get('total_analyzed', 0))],
        ['Relevant Sources', str(meta['successful_extractions'].get('relevant_sources', 0))],
        ['Contacts Identified', str(meta['successful_extractions'].get('contacts_found', 0))],
        ['Research Date', meta['timestamp'].split('T')[0]],
    ]
    
    t = Table(stats, colWidths=[2*inch, 4*inch])
    t.setStyle(table_style)
    story.append(t)

    # Build the PDF
    doc.build(story)
    logger.info(f"Enhanced PDF report generated for {company}")