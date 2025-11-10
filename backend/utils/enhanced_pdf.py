"""
Enhanced PDF generation with AI-optimized context and formatting.

This script is structured to be highly useful for future AI agents:
1.  **AI Summaries First:** Presents the AI-generated briefings at the beginning.
2.  **Metadata Context:** Provides the "who, what, why" (profile, queries, scores).
3.  **Raw Data Appendix:** Appends all raw source text as a ground-truth reference.
"""
import io
import json
import logging
import os  # Added for logo path
import re  # Added for markdown parsing
from datetime import datetime
from typing import Dict, Any, List

from reportlab.lib import colors
try:
    from .context_analyzer import ContextAnalyzer
except ImportError:
    logger = logging.getLogger(__name__)
    logger.warning("Could not import ContextAnalyzer. Using dummy class.")
    # Define a dummy class if import fails (e.g., in a test)
    class ContextAnalyzer:
        async def prepare_context(self, data):
            logger.info("Using dummy ContextAnalyzer.")
            return data if isinstance(data, dict) else {}

from reportlab.lib.enums import TA_LEFT, TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle,
    PageBreak, Image, ListFlowable, ListItem
)
import logging
import os.path
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

# Initialize logger first
logger = logging.getLogger(__name__)

# Get the assets directory path
ASSETS_DIR = os.path.join(os.path.dirname(__file__), '..', 'assets')

# Initialize font variables with default Helvetica
FONT_REGULAR = "Helvetica"
FONT_BOLD = "Helvetica-Bold"
FONT_SEMIBOLD = "Helvetica-Bold"

# Try to load OpenSans fonts if available
try:
    # Check if OpenSans variable font exists in the assets directory
    # NOTE: ReportLab has limited support for variable fonts. While it can use them,
    # it won't take advantage of the variable font features. The font will work but
    # only in its default weight/width configuration.
    opensans_regular = os.path.join(ASSETS_DIR, 'OpenSans-VariableFont_wdth,wght.ttf')
    opensans_italic = os.path.join(ASSETS_DIR, 'OpenSans-Italic-VariableFont_wdth,wght.ttf')
    
    if all(os.path.exists(f) for f in [opensans_regular, opensans_italic]):
        # Register OpenSans fonts
        pdfmetrics.registerFont(TTFont('OpenSans', opensans_regular))
        pdfmetrics.registerFont(TTFont('OpenSans-Italic', opensans_italic))
        
        # Update font variables to use OpenSans
        FONT_REGULAR = "OpenSans"
        # NOTE: Using 'OpenSans' for all weights, as ReportLab can struggle with
        # variable font weight names. Bolding will be applied via <b> tags.
        FONT_BOLD = "OpenSans"
        FONT_SEMIBOLD = "OpenSans"
        
        logger.info("Successfully registered OpenSans variable fonts")
    else:
        logger.warning("OpenSans variable font files not found in assets directory. Using Helvetica as fallback.")
except Exception as e:
    logger.warning(f"Could not register Open Sans fonts: {str(e)}. Using Helvetica as fallback.")

# --- Define ReFED Brand Styles ---
REFED_DARK = colors.HexColor('#384954')
REFED_GREEN = colors.HexColor('#48B674')
REFED_LIGHT_GREY = colors.HexColor('#EFEDEB')
REFED_LIGHT_GREEN_TINT = colors.HexColor('#D1ECC1')

# --- Define Logo Path ---
LOGO_PATH = os.path.join(os.path.dirname(__file__), '..', 'assets', 'refed_logo.png')


# --- End ReFED Styles ---


def _convert_markdown_to_elements(
    md_text: str,
    body_style: ParagraphStyle,
    subsection_style: ParagraphStyle
) -> List[Any]:
    """
    Basic converter for the final markdown report into ReportLab elements.
    This is crucial for rendering the AI's output correctly.
    """
    logger.debug(f"Converting markdown text of length: {len(md_text) if md_text else 0}")
    
    # Validate and clean input
    if md_text is None:
        logger.warning("Received None as markdown text")
        return [Paragraph("No content available.", body_style)]
    
    if not isinstance(md_text, str):
        logger.warning(f"Converting non-string markdown text of type {type(md_text)} to string")
        try:
            md_text = str(md_text)
        except Exception as e:
            logger.error(f"Failed to convert markdown text to string: {e}")
            return [Paragraph("Failed to process content format.", body_style)]
    
    if not md_text.strip():
        logger.warning("Received empty markdown text")
        return [Paragraph("No content available.", body_style)]
    
    # Clean up common markdown issues
    md_text = md_text.replace('\r\n', '\n')  # Normalize line endings
    md_text = re.sub(r'\n{3,}', '\n\n', md_text)  # Remove excessive newlines
    
    # Clean up markdown response for parsing
    md_text = re.sub(r"^\s*```markdown", "", md_text, flags=re.MULTILINE)
    md_text = re.sub(r"```\s*$", "", md_text, flags=re.MULTILINE)
    md_text = md_text.strip()
    
    elements = []
    in_list = False
    list_items = []

    def flush_list():
        nonlocal list_items, in_list, elements
        if list_items:
            # Use a style for list items
            list_item_style = ParagraphStyle(name='ListItem', parent=body_style, leftIndent=20)
            list_flow = ListFlowable(
                [ListItem(Paragraph(item, list_item_style)) for item in list_items],
                bulletType='bullet', leftIndent=20, bulletColor=REFED_DARK
            )
            elements.append(list_flow)
            list_items = []
        in_list = False

    if not md_text:
        return [Paragraph("No summary was generated.", body_style)]

    for line in md_text.split('\n'):
        line_strip = line.strip()

        if not line_strip:
            if in_list: flush_list()
            elements.append(Spacer(1, 0.1*inch))
            continue

        if line_strip.startswith('## '):
            if in_list: flush_list()
            elements.append(Paragraph(line_strip[3:], subsection_style))
        elif line_strip.startswith('### '):
            if in_list: flush_list()
            # Treat H3 as bold body text for this context
            elements.append(Paragraph(f"<b>{line_strip[4:]}</b>", body_style))
        elif line_strip.startswith('* '):
            in_list = True
            list_items.append(line_strip[2:])
        else:
            if in_list: flush_list()
            elements.append(Paragraph(line_strip, body_style))
    
    if in_list: flush_list() # Flush any remaining list items
    return elements


def format_source_content(
    url: str, 
    data: Dict[str, Any], 
    body_style: ParagraphStyle, 
    subsection_style: ParagraphStyle
) -> List[Any]:
    """
    Helper function to format source content with metadata,
    styled for the ReFED brand.
    """
    elements = []
    
    # Source header (using the ReFED Subsection Style for hierarchy)
    title = data.get("title") or url
    elements.append(Paragraph(title, subsection_style))
    elements.append(Paragraph(f"<i>Source: {url}</i>", body_style))
    
    # Metadata table (Styled with ReFED colors)
    meta_data = [
        ["Score", f"{data.get('relevance', 0):.2f}"],
        ["Query", data.get("query_context", "N/A")],
        ["Timestamp", data.get("timestamp", "N/A")],
        ["Source Type", data.get("source_type", "N/A")]
    ]
    t = Table(meta_data, colWidths=[1.5*inch, 4*inch])
    t.setStyle(TableStyle([
        ('GRID', (0, 0), (-1, -1), 0.5, REFED_LIGHT_GREEN_TINT),
        ('BACKGROUND', (0, 0), (0, -1), REFED_LIGHT_GREY),
        ('FONTNAME', (0, 0), (-1, -1), FONT_REGULAR),
        ('FONTSIZE', (0, 0), (-1, -1), 8),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('PADDING', (0, 0), (-1, -1), 4),
        ('TEXTCOLOR', (0,0), (-1,-1), REFED_DARK),
    ]))
    elements.append(t)
    elements.append(Spacer(1, 0.1*inch))
    
    # Raw content section
    # **REMOVED TRUNCATION** to ensure all raw data is included for the AI
    if data.get("raw_content"):
        elements.append(Paragraph("<b>Raw Content:</b>", body_style))
        raw_content = str(data["raw_content"])
        elements.append(Paragraph(raw_content, body_style))
        elements.append(Spacer(1, 0.1*inch))
    
    # Processed content section (this is usually the Tavily snippet)
    if data.get("content"):
        elements.append(Paragraph("<b>Processed Content (Snippet):</b>", body_style))
        elements.append(Paragraph(data["content"], body_style))
    
    elements.append(Spacer(1, 0.2*inch))
    return elements

async def create_enhanced_research_pdf(research_data: Dict[str, Any], output: io.BytesIO) -> None:
    """
    Creates a comprehensive PDF optimized for AI consumption with enhanced context
    and styled according to the ReFED brand guide.
    
    Args:
        research_data: The complete *FLAT* research data dictionary (agent state)
        output: BytesIO object to write the PDF to
    """
    # --- START FIX: Data Structuring ---
    # Extract company name first, before any potential errors
    company_name = None
    if isinstance(research_data, dict):
        # Try multiple paths to find company name
        company_name = (
            research_data.get('company_name') or
            research_data.get('name') or
            research_data.get('company_identity', {}).get('name') or
            (research_data.get('final_summary', {}) or {}).get('company_name') or
            (research_data.get('metadata', {}) or {}).get('company_name')
        )
        logger.debug(f"Extracted company name: {company_name}")

    # The ContextAnalyzer *creates* the nested structure from the flat agent state.
    analyzer = ContextAnalyzer()
    try:
        # Pass the flat agent state to the analyzer
        enhanced_data = await analyzer.prepare_context(research_data)
        logger.debug(f"Context analyzer returned keys: {enhanced_data.keys() if isinstance(enhanced_data, dict) else 'Not a dictionary'}")
    except Exception as e:
        logger.error(f"Failed to prepare context: {e}. PDF will be incomplete.", exc_info=True)
        # Create a basic structure with the company name if we have it
        enhanced_data = {
            'company_identity': {'name': company_name} if company_name else {}
        }

    # Set up default data structure with empty values
    default_data = {
        'company_identity': {
            'name': company_name or 'Company',  # Use extracted company name
            'headquarters': 'Not Available',
            'industry': 'Not Available',
            'region': 'Not Available',
            'scale': 'Not Available',
            'website': 'Not Available'
        },
        'research_meta': {
            'queries_used': {},
            'successful_extractions': {
                'total_analyzed': 0,
                'relevant_sources': 0,
                'contacts_found': 0
            },
            'timestamp': datetime.now().isoformat()
        },
        'final_summary': {'markdown_report': ''},
        'source_credibility': {'reference_info': {}},
        'official_content': {
            'company_information': {},
            'sustainability_reporting': {}
        },
        'recent_developments': {'news_coverage': {}},
        'key_personnel': {'identified_contacts': {}},
        'engagement_signals': {'partnerships_and_initiatives': {}}
    }

    # Merge the enhanced data with default data, keeping existing values
    # This ensures the PDF generation doesn't crash if a key is missing
    enhanced_data = {**default_data, **enhanced_data}
    for key in default_data:
        if isinstance(default_data[key], dict) and key in enhanced_data and enhanced_data[key] is not None:
            enhanced_data[key] = {**default_data[key], **enhanced_data[key]}
    
    company_name = enhanced_data['company_identity']['name']
    # --- END FIX: Data Structuring ---
    
    # Document setup
    doc = SimpleDocTemplate(
        output,
        pagesize=A4,
        rightMargin=0.75*inch,
        leftMargin=0.75*inch,
        topMargin=0.75*inch,
        bottomMargin=0.75*inch,
        title=f"{company_name} Research Report",
        author="ReFED Development Team"
    )

    # --- ReFED Styles ---
    styles = getSampleStyleSheet()
    
    title_style = ParagraphStyle(
        'ReFEDTitle',
        parent=styles['Heading1'],
        fontName=FONT_BOLD,
        fontSize=24,
        spaceAfter=30,
        alignment=TA_CENTER,
        textColor=REFED_DARK
    )
    
    section_style = ParagraphStyle(
        'ReFEDSection',
        parent=styles['Heading2'],
        fontName=FONT_SEMIBOLD,
        fontSize=16,
        spaceBefore=20,
        spaceAfter=10,
        textColor=REFED_GREEN
    )
    
    subsection_style = ParagraphStyle(
        'ReFEDSubsection',
        parent=styles['Heading3'],
        fontName=FONT_SEMIBOLD,
        fontSize=14,
        spaceBefore=15,
        spaceAfter=8,
        textColor=REFED_DARK
    )
    
    body_style = ParagraphStyle(
        'ReFEDBody',
        parent=styles['Normal'],
        fontName=FONT_REGULAR,
        fontSize=10,
        leading=14,
        alignment=TA_LEFT,
        textColor=REFED_DARK,
        spaceAfter=6
    )

    table_style = TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), REFED_LIGHT_GREY),
        ('TEXTCOLOR', (0, 0), (-1, 0), REFED_DARK),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, 0), FONT_SEMIBOLD),
        ('FONTSIZE', (0, 0), (-1, 0), 10),
        ('FONTNAME', (0, 1), (-1, -1), FONT_REGULAR),
        ('FONTSIZE', (0, 1), (-1, -1), 10),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('TOPPADDING', (0, 0), (-1, 0), 12),
        ('BOTTOMPADDING', (0, 1), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 1, REFED_LIGHT_GREEN_TINT),
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
    ])
    
    source_analysis_table_style = TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), REFED_LIGHT_GREY),
        ('TEXTCOLOR', (0, 0), (-1, 0), REFED_DARK),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, 0), FONT_SEMIBOLD),
        ('FONTNAME', (0, 1), (-1, -1), FONT_REGULAR),
        ('FONTSIZE', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 0.5, REFED_LIGHT_GREEN_TINT),
        ('PADDING', (0, 0), (-1, -1), 4),
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
    ])
    # --- End Styles ---

    # Build the document
    story = []
    
    # --- Title Page with Logo ---
    if os.path.exists(LOGO_PATH):
        try:
            # --- FIX: Removed preserveAspectRatio=True ---
            story.append(Image(LOGO_PATH, width=2*inch))
            # --- END FIX ---
            story.append(Spacer(1, 0.2*inch))
        except Exception as e:
            # Log the full error
            logger.warning(f"Could not load logo from {LOGO_PATH}: {e}", exc_info=True)
            story.append(Paragraph("ReFED", title_style)) # Fallback text
    else:
        logger.warning(f"Logo not found at {LOGO_PATH}. Skipping logo. Place it at backend/assets/refed_logo.png")
        story.append(Paragraph("ReFED", title_style)) # Fallback text
        
    story.append(Paragraph(f"{company_name} Research Report", title_style))
    story.append(Paragraph(f"Generated: {datetime.now().strftime('%Y-%m-%d')}", body_style))
    story.append(Spacer(1, 20))
    
    # --- AI-Optimized Table of Contents ---
    story.append(Paragraph("Table of Contents", section_style))
    toc_items = [
        "1. AI-Generated Briefings",
        "2. Company Profile",
        "3. Research Process & Queries",
        "4. Source Credibility Analysis",
        "5. Research Statistics",
        "6. Raw Data Appendix",
        "   6.1 Company Information",
        "   6.2 News & Recent Developments",
        "   6.3 Sustainability & FLW Data",
        "   6.4 Contact Information",
        "   6.5 Engagement & Partnerships",
    ]
    for item in toc_items:
        style = body_style
        if item.strip().startswith("   "):
            style = ParagraphStyle(name='TOCSub', parent=body_style, leftIndent=20)
        story.append(Paragraph(item, style))
    story.append(PageBreak())
    
    # --- Section 1: AI-Generated Briefings (AI-FIRST) ---
    story.append(Paragraph("1. AI-Generated Briefings", section_style))
    story.append(Paragraph(
        "This section contains the AI-generated summaries for each research category, "
        "which can be used to draft tailored outreach.",
        body_style
    ))
    
    # This assumes the AI briefings are passed in `final_summary`
    if 'final_summary' in enhanced_data and 'markdown_report' in enhanced_data['final_summary']:
        md_report = enhanced_data['final_summary']['markdown_report']
        story.extend(_convert_markdown_to_elements(md_report, body_style, subsection_style))
    else:
        logger.warning("No 'final_summary.markdown_report' key found in enhanced_data.")
        story.append(Paragraph(
            "AI-generated briefings were not found in the research data.",
            body_style
        ))
    story.append(PageBreak())

    # --- Section 2: Company Profile ---
    story.append(Paragraph("2. Company Profile", section_style))
    identity_data = enhanced_data['company_identity']
    company_info = [
        [Paragraph('Company Profile', body_style), Paragraph(identity_data.get('name', 'Not Available'), body_style)],
        [Paragraph('Headquarters', body_style), Paragraph(identity_data.get('headquarters', 'Not Available'), body_style)],
        [Paragraph('Industry', body_style), Paragraph(identity_data.get('industry', 'Not Available'), body_style)],
        [Paragraph('Region', body_style), Paragraph(identity_data.get('region', 'Not Available'), body_style)],
        [Paragraph('Scale', body_style), Paragraph(identity_data.get('scale', 'Not Available'), body_style)],
        [Paragraph('Website', body_style), Paragraph(identity_data.get('website', 'Not Available'), body_style)],
    ]
    t = Table(company_info, colWidths=[1.5*inch, 4.5*inch])
    t.setStyle(table_style)
    story.append(t)
    story.append(Spacer(1, 0.2*inch))
    story.append(PageBreak())

    # --- Section 3: Research Process & Queries ---
    story.append(Paragraph("3. Research Process & Queries", section_style))
    meta = enhanced_data['research_meta']
    story.append(Paragraph("Research Queries by Category:", subsection_style))
    for query_type, queries in meta.get('queries_used', {}).items():
        story.append(Paragraph(f"<b>{query_type.replace('_', ' ').title()}:</b>", body_style))
        query_items = [
            ListItem(Paragraph(query, body_style), leftIndent=20) for query in queries
        ]
        story.append(ListFlowable(query_items, bulletType='bullet', leftIndent=20, bulletColor=REFED_DARK))
    story.append(PageBreak())
    
    # --- Section 4: Source Credibility Analysis ---
    story.append(Paragraph("4. Source Credibility Analysis", section_style))
    story.append(Paragraph("Source Credibility Analysis:", subsection_style))
    
    source_analysis_data = [['Source', 'Type', 'Score', 'Domain']]
    source_info = enhanced_data.get('source_credibility', {}).get('reference_info', {})
    if source_info:
        for url, info in source_info.items():
            if not isinstance(info, dict): continue # Add check
            title_p = Paragraph(info.get('title', url), body_style)
            try:
                score_val = f"{float(info.get('score', 0)):.2f}"
            except (ValueError, TypeError):
                score_val = "N/A"
            
            source_analysis_data.append([
                title_p,
                info.get('source_type', 'N/A'),
                score_val,
                info.get('domain', 'N/A')
            ])
        
        t = Table(source_analysis_data, colWidths=[3*inch, 1*inch, 0.7*inch, 1.3*inch])
        t.setStyle(source_analysis_table_style)
        story.append(t)
    else:
        story.append(Paragraph("No source credibility data available.", body_style))
    story.append(PageBreak())

    # --- Section 5: Research Statistics ---
    story.append(Paragraph("5. Research Statistics", section_style))
    stats_meta = enhanced_data.get('research_meta', {}) # Use variable
    stats = [
        [Paragraph('Research Statistics', body_style), ''],
        [Paragraph('Total Sources Analyzed', body_style), str(stats_meta.get('successful_extractions', {}).get('total_analyzed', 0))],
        [Paragraph('Relevant Sources', body_style), str(stats_meta.get('successful_extractions', {}).get('relevant_sources', 0))],
        [Paragraph('Contacts Identified', body_style), str(stats_meta.get('successful_extractions', {}).get('contacts_found', 0))],
        [Paragraph('Research Date', body_style), 
         stats_meta.get('timestamp', 'N/A').split('T')[0] if stats_meta.get('timestamp') else 'N/A'],
    ]
    t = Table(stats, colWidths=[2*inch, 4*inch])
    t.setStyle(table_style)
    story.append(PageBreak())

    # --- Section 6: Raw Data Appendix (The "Rich Context") ---
    story.append(Paragraph("6. Raw Data Appendix", section_style))
    story.append(Paragraph(
        "This appendix contains the detailed source material, including metadata and full extracted text. "
        "This is the ground-truth data for AI analysis.",
        body_style
    ))
    
    # Helper to iterate and add sections
    def add_raw_data_section(title, section_data):
        story.append(Paragraph(title, subsection_style))
        if not section_data:
            story.append(Paragraph("No data found for this section.", body_style))
            return
        
        for url, data in section_data.items():
            if isinstance(data, dict):
                story.extend(format_source_content(url, data, body_style, subsection_style))
            else:
                logger.warning(f"Skipping invalid data entry for {title}: {data}")
        story.append(PageBreak())

    # 6.1 Company Information
    add_raw_data_section("6.1 Company Information", enhanced_data['official_content']['company_information'])
    
    # 6.2 News & Recent Developments
    add_raw_data_section("6.2 News & Recent Developments", enhanced_data['recent_developments']['news_coverage'])
    
    # 6.3 Sustainability & FLW Data
    add_raw_data_section("6.3 Sustainability & FLW Data", enhanced_data['official_content']['sustainability_reporting'])
    
    # 6.4 Contact Information
    add_raw_data_section("6.4 Contact Information", enhanced_data['key_personnel']['identified_contacts'])
    # Note: The 'extracted_contacts' logic from the original script was flawed and specific
    # to a loop. It's been removed for stability. The raw data will contain contact info.
    
    # 6.5 Engagement & Partnerships
    add_raw_data_section("6.5 Engagement & Partnerships", enhanced_data['engagement_signals']['partnerships_and_initiatives'])
    
    # --- Build the PDF ---
    try:
        doc.build(story)
        logger.info(f"AI-Optimized ReFED-styled PDF report generated for {company_name}")
    except Exception as build_err:
        logger.error(f"CRITICAL: PDF doc.build() failed: {build_err}", exc_info=True)
        # Re-raise the exception to be caught by the service layer
        raise