"""
Executive Summary PDF Generation - Concise 1-2 page AI-generated research brief.

This module generates a dynamic, AI-written executive summary that adapts its structure
based on research findings. The full JSON remains the source of truth.
"""

import io
import logging
import os
from datetime import datetime
from typing import Any, Dict

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_LEFT
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    Image,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

try:
    import google.generativeai as genai
except ImportError:
    genai = None

logger = logging.getLogger(__name__)

# ReFED Brand Colors
REFED_DARK = colors.HexColor("#384954")
REFED_GREEN = colors.HexColor("#48B674")
REFED_LIGHT_GREY = colors.HexColor("#EFEDEB")

# Asset paths
ASSETS_DIR = os.path.join(os.path.dirname(__file__), "assets")
LOGO_PATH = os.path.join(ASSETS_DIR, "refed_logo.png")

# Font setup
FONT_REGULAR = "Helvetica"
FONT_BOLD = "Helvetica-Bold"

try:
    opensans_regular = os.path.join(ASSETS_DIR, "OpenSans-VariableFont_wdth,wght.ttf")
    if os.path.exists(opensans_regular):
        pdfmetrics.registerFont(TTFont("OpenSans", opensans_regular))
        FONT_REGULAR = "OpenSans"
        FONT_BOLD = "OpenSans"
        logger.info("Successfully loaded OpenSans font")
except Exception as e:
    logger.warning(f"Could not load OpenSans font: {e}. Using Helvetica.")


class ExecutiveSummaryGenerator:
    """Generates AI-written executive summaries with dynamic structure."""

    def __init__(self):
        self.gemini_key = os.getenv("GEMINI_API_KEY")
        if not self.gemini_key and genai:
            logger.warning("GEMINI_API_KEY not set - will use template-based summary")
            self.model = None
        elif genai:
            try:
                genai.configure(api_key=self.gemini_key)
                self.model = genai.GenerativeModel("gemini-2.0-flash-exp")
                logger.info(
                    "Initialized Gemini 2.0 Flash for executive summary generation"
                )
            except Exception as e:
                logger.error(f"Failed to initialize Gemini: {e}")
                self.model = None
        else:
            self.model = None

    async def generate_executive_summary(self, research_data: Dict[str, Any]) -> str:
        """Generate a 1-2 page executive summary using AI."""

        if not self.model:
            return self._generate_template_summary(research_data)

        # Extract key information for the prompt
        company_name = research_data.get("company_identity", {}).get("name", "Company")

        # Get briefings if available
        comm_insights = research_data.get("communication_insights", {})
        company_brief = comm_insights.get("company_brief_briefing", "")
        news_brief = comm_insights.get("news_signal_briefing", "")
        flw_brief = comm_insights.get("flw_sustainability_briefing", "")
        contact_brief = comm_insights.get("contact_briefing", "")
        engagement_brief = comm_insights.get("engagement_briefing", "")

        # Collect source information
        sources_summary = self._summarize_sources(research_data)

        prompt = f"""You are writing a concise executive research brief for {company_name}. This will be a 1-2 page PDF report.

**Your task:** Write a professional, well-structured executive summary that adapts its format based on what information is most relevant and available.

**Guidelines:**
1. Maximum length: 400-500 words (this will fit on 1-2 pages)
2. Use a dynamic structure - only include sections where you have meaningful information
3. Write in clear, professional prose (not bullet points)
4. Focus on insights and implications, not just facts
5. Prioritize: Strategic relevance, opportunities, risks, and actionable insights
6. DO NOT use generic headers like "Introduction" or "Conclusion"
7. Start directly with the most important finding

**Available Research Data:**

COMPANY OVERVIEW:
{company_brief[:1000] if company_brief else 'Limited information available'}

NEWS & DEVELOPMENTS:
{news_brief[:1000] if news_brief else 'Limited information available'}

SUSTAINABILITY & FOOD WASTE:
{flw_brief[:1000] if flw_brief else 'Limited information available'}

PARTNERSHIPS & ENGAGEMENT:
{engagement_brief[:1000] if engagement_brief else 'Limited information available'}

KEY CONTACTS:
{contact_brief[:500] if contact_brief else 'Limited information available'}

SOURCES:
{sources_summary}

**Output Format:**
Write the summary as clean markdown with dynamic headers (##) based on your content. 
Adapt the structure to emphasize the most important findings.
Do NOT include any meta-commentary, just the executive summary itself.

Now write the executive summary:"""

        try:
            response = await self.model.generate_content_async(
                prompt, request_options={"timeout": 60}
            )

            if response and response.parts:
                summary = "".join(
                    part.text for part in response.parts if hasattr(part, "text")
                ).strip()
                logger.info(f"Generated AI executive summary ({len(summary)} chars)")
                return summary
            else:
                logger.warning("Empty response from Gemini, using template")
                return self._generate_template_summary(research_data)

        except Exception as e:
            logger.error(f"Error generating AI summary: {e}")
            return self._generate_template_summary(research_data)

    def _summarize_sources(self, research_data: Dict[str, Any]) -> str:
        """Create a brief summary of research sources."""
        source_info = research_data.get("source_credibility", {}).get(
            "reference_info", {}
        )
        if not source_info:
            return "No sources available"

        source_types = {}
        for url, info in source_info.items():
            if isinstance(info, dict):
                source_type = info.get("source_type", "unknown")
                source_types[source_type] = source_types.get(source_type, 0) + 1

        summary_parts = [f"{count} {stype}" for stype, count in source_types.items()]
        return f"Based on {len(source_info)} sources: {', '.join(summary_parts)}"

    def _generate_template_summary(self, research_data: Dict[str, Any]) -> str:
        """Fallback template-based summary when AI is not available."""
        company_name = research_data.get("company_identity", {}).get("name", "Company")

        comm_insights = research_data.get("communication_insights", {})
        company_brief = comm_insights.get("company_brief_briefing", "")

        summary = f"## Research Brief: {company_name}\n\n"

        if company_brief:
            # Extract first few sentences
            sentences = company_brief.split(".")[:3]
            summary += ".".join(sentences) + ".\n\n"

        summary += "This is a template-based summary. Configure GEMINI_API_KEY for AI-generated summaries.\n"

        return summary


def header_and_footer(canvas, doc):
    """Add ReFED-branded header and footer."""
    canvas.saveState()
    page_width = doc.width + doc.leftMargin + doc.rightMargin
    page_height = doc.height + doc.topMargin + doc.bottomMargin

    # Header with logo
    if os.path.exists(LOGO_PATH):
        canvas.drawImage(
            LOGO_PATH,
            doc.leftMargin,
            page_height - 0.8 * inch,
            width=1.2 * inch,
            preserveAspectRatio=True,
            mask="auto",
        )

    # Header line
    canvas.setStrokeColor(REFED_GREEN)
    canvas.setLineWidth(1)
    canvas.line(
        doc.leftMargin,
        page_height - 1 * inch,
        page_width - doc.rightMargin,
        page_height - 1 * inch,
    )

    # Footer
    canvas.setFont(FONT_REGULAR, 8)
    canvas.setFillColor(REFED_DARK)
    footer_text = (
        f"ReFED Research Brief • Generated {datetime.now().strftime('%B %d, %Y')}"
    )
    canvas.drawString(doc.leftMargin, 0.5 * inch, footer_text)

    if doc.page > 1:
        canvas.drawRightString(
            page_width - doc.rightMargin, 0.5 * inch, f"Page {doc.page}"
        )

    canvas.restoreState()


async def create_executive_summary_pdf(
    research_data: Dict[str, Any], output: io.BytesIO
) -> None:
    """
    Create a concise 1-2 page executive summary PDF with AI-generated content.

    Args:
        research_data: Complete research data dictionary
        output: BytesIO buffer to write PDF to
    """

    # Initialize generator
    generator = ExecutiveSummaryGenerator()

    # Generate AI summary
    logger.info("Generating AI executive summary...")
    summary_text = await generator.generate_executive_summary(research_data)

    # Get company info
    company_name = research_data.get("company_identity", {}).get("name", "Company")
    company_url = research_data.get("company_identity", {}).get("website", "")

    # Setup PDF document
    doc = SimpleDocTemplate(
        output,
        pagesize=letter,
        rightMargin=0.75 * inch,
        leftMargin=0.75 * inch,
        topMargin=1.25 * inch,
        bottomMargin=0.75 * inch,
        title=f"{company_name} - Research Brief",
        author="ReFED",
    )

    # Setup styles
    styles = getSampleStyleSheet()

    title_style = ParagraphStyle(
        "Title",
        parent=styles["Heading1"],
        fontName=FONT_BOLD,
        fontSize=18,
        textColor=REFED_DARK,
        spaceAfter=6,
        alignment=TA_LEFT,
    )

    subtitle_style = ParagraphStyle(
        "Subtitle",
        parent=styles["Normal"],
        fontName=FONT_REGULAR,
        fontSize=10,
        textColor=REFED_DARK,
        spaceAfter=20,
        alignment=TA_LEFT,
    )

    heading_style = ParagraphStyle(
        "Heading",
        parent=styles["Heading2"],
        fontName=FONT_BOLD,
        fontSize=13,
        textColor=REFED_GREEN,
        spaceBefore=12,
        spaceAfter=6,
        alignment=TA_LEFT,
    )

    body_style = ParagraphStyle(
        "Body",
        parent=styles["Normal"],
        fontName=FONT_REGULAR,
        fontSize=10,
        leading=14,
        textColor=REFED_DARK,
        alignment=TA_JUSTIFY,
        spaceAfter=10,
    )

    # Build document content
    story = []

    # Title
    story.append(Paragraph(company_name, title_style))

    # Subtitle with URL
    if company_url:
        story.append(
            Paragraph(
                f'<a href="{company_url}" color="{REFED_GREEN}">{company_url}</a>',
                subtitle_style,
            )
        )
    else:
        story.append(Spacer(1, 20))

    # Convert markdown summary to PDF elements
    summary_elements = _convert_markdown_to_flowables(
        summary_text, heading_style, body_style
    )
    story.extend(summary_elements)

    # Build PDF
    try:
        doc.build(story, onFirstPage=header_and_footer, onLaterPages=header_and_footer)
        logger.info(f"Executive summary PDF generated for {company_name}")
    except Exception as e:
        logger.error(f"Failed to build PDF: {e}", exc_info=True)
        raise


def _convert_markdown_to_flowables(
    markdown_text: str, heading_style, body_style
) -> list:
    """Convert markdown text to ReportLab flowable elements."""
    elements = []

    if not markdown_text or not markdown_text.strip():
        elements.append(Paragraph("No summary available.", body_style))
        return elements

    # Clean up markdown
    markdown_text = markdown_text.replace("\r\n", "\n")
    markdown_text = re.sub(r"\n{3,}", "\n\n", markdown_text)

    # Process line by line
    for line in markdown_text.split("\n"):
        line = line.strip()

        if not line:
            elements.append(Spacer(1, 6))
            continue

        # Handle headers
        if line.startswith("## "):
            elements.append(Paragraph(line[3:], heading_style))
        elif line.startswith("### "):
            elements.append(Paragraph(f"<b>{line[4:]}</b>", body_style))
        # Handle bold
        elif "**" in line:
            line = line.replace("**", "<b>", 1).replace("**", "</b>", 1)
            elements.append(Paragraph(line, body_style))
        # Handle italic
        elif "*" in line and not line.startswith("*"):
            line = line.replace("*", "<i>", 1).replace("*", "</i>", 1)
            elements.append(Paragraph(line, body_style))
        # Regular paragraph
        else:
            elements.append(Paragraph(line, body_style))

    return elements
