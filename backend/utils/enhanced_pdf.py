"""
Enhanced PDF generation with AI-optimized context and formatting.

V2 TEST SCRIPT:
- Combines enhanced_pdf, context_analyzer, and context_polisher.
- Updates footer text to "ReFED Research Report".
- Simplifies asset path to look in a local "./assets" folder.
"""

import asyncio  # Added for test harness
import io
import json
import logging
import os
import re
from datetime import datetime
from typing import Any, Dict, List

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm, inch
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    Image,
    ListFlowable,
    ListItem,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

# --- Initialize Logger ---
# Basic logging config for testing
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- START: V2 - Combined Code from context_polisher.py ---
# Note: In a real app, Gemini API key should be securely managed.
# For this test, it will warn if the key is not set but won't fail.
try:
    import google.generativeai as genai
except ImportError:
    logger.warning(
        "google.generativeai not installed. ContextPolisher will be disabled."
    )
    genai = None


class ContextPolisher:
    """Polishes research context JSON using Gemini for better readability and structure."""

    def __init__(self) -> None:
        if genai is None:
            logger.warning("Gemini library not available. ContextPolisher is disabled.")
            self.model = None
            return

        self.gemini_key = os.getenv("GEMINI_API_KEY")
        if not self.gemini_key:
            logger.warning(
                "GEMINI_API_KEY environment variable is not set - will return unpolished context"
            )
            self.model = None
            return

        try:
            # Configure Gemini
            genai.configure(api_key=self.gemini_key)
            self.model = genai.GenerativeModel("gemini-1.5-flash")  # Using 1.5-flash
            logger.info("Context Polisher initialized with Gemini 1.5 Flash model")
        except Exception as e:
            logger.error(f"Failed to initialize Gemini model: {e}")
            self.model = None

    async def polish_context(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Processes the research context to improve readability and structure.
        """
        try:
            if not self.model:
                logger.warning(
                    "Gemini model not available - returning unpolished context"
                )
                return context

            # This is a placeholder for the actual polishing prompt.
            # In a real scenario, you'd send parts of the context to Gemini.
            # For this test, we'll just log and return the original context
            # to avoid unnecessary API calls during visual testing.
            logger.info("ContextPolisher: Bypassing Gemini API call for visual test.")
            return context

        except Exception as e:
            logger.error(f"Error during context polishing: {e}")
            return context  # Return original context if polishing fails


# --- END: V2 - Combined Code from context_polisher.py ---


# --- START: V2 - Combined Code from context_analyzer.py ---
class ContextAnalyzer:
    """Analyzes and structures research context for AI consumption."""

    def __init__(self):
        # Use the ContextPolisher class defined above
        self.polisher = ContextPolisher()

    async def prepare_context(self, research_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepares research context from the flat agent state.
        """
        try:
            logger.debug(
                f"Received research data keys: {research_data.keys() if isinstance(research_data, dict) else 'Not a dictionary'}"
            )

            # Start with a fresh structure
            enhanced_data = {
                "company_identity": {
                    "name": "Company",
                    "headquarters": "Not Available",
                    "industry": "Not Available",
                    "region": "Not Available",
                    "scale": "Not Available",
                    "website": "Not Available",
                },
                "research_meta": {
                    "queries_used": {},
                    "successful_extractions": {
                        "total_analyzed": 0,
                        "relevant_sources": 0,
                        "contacts_found": 0,
                    },
                    "timestamp": "",
                    "data_relationships": {  # <-- Key fix from our previous chat
                        "queries_to_sources": {}
                    },
                },
                "final_summary": {"markdown_report": ""},  # Add default
                "source_credibility": {"reference_info": {}},  # Add default
                "official_content": {
                    "company_information": {},
                    "sustainability_reporting": {},
                },
                "recent_developments": {"news_coverage": {}},
                "key_personnel": {"identified_contacts": {}},
                "engagement_signals": {"partnerships_and_initiatives": {}},
            }

            # Update with actual data if available
            if isinstance(research_data, dict):
                # Extract company identity data
                company_data = research_data.get("company_identity", {})

                # Update company identity
                enhanced_data["company_identity"].update(company_data)

                # Update with actual data if available
                if "research_meta" in research_data:
                    enhanced_data["research_meta"].update(
                        research_data["research_meta"]
                    )
                    logger.debug(
                        f"Updated research_meta with keys: {research_data['research_meta'].keys()}"
                    )

                # Update content sections
                for section in [
                    "official_content",
                    "recent_developments",
                    "key_personnel",
                    "engagement_signals",
                    "source_credibility",
                    "final_summary",
                ]:
                    if section in research_data:
                        enhanced_data[section].update(research_data[section])
                        logger.debug(
                            f"Updated section '{section}' with keys: {research_data[section].keys()}"
                        )
                    else:
                        logger.warning(f"Missing section in research data: {section}")

                # Log final structure
                logger.debug(
                    f"Final enhanced data structure keys: {enhanced_data.keys()}"
                )

            # Track relationships (Fix from previous chat)
            for query_type, queries in (
                research_data.get("research_meta", {}).get("queries_used", {}).items()
            ):
                if (
                    query_type
                    not in enhanced_data["research_meta"]["data_relationships"][
                        "queries_to_sources"
                    ]
                ):
                    enhanced_data["research_meta"]["data_relationships"][
                        "queries_to_sources"
                    ][query_type] = {}

                for query in queries:
                    matching_sources = []
                    for section in [
                        "official_content",
                        "recent_developments",
                        "key_personnel",
                        "engagement_signals",
                    ]:
                        if section in research_data:
                            for subsection in research_data.get(section, {}).values():
                                for url, data in subsection.items():
                                    if (
                                        isinstance(data, dict)
                                        and data.get("query_context") == query
                                    ):
                                        matching_sources.append(
                                            {
                                                "url": url,
                                                "relevance": data.get("relevance", 0),
                                                "source_type": data.get(
                                                    "source_type", "unknown"
                                                ),
                                            }
                                        )

                    enhanced_data["research_meta"]["data_relationships"][
                        "queries_to_sources"
                    ][query_type][query] = matching_sources

            # Light content polishing (will be bypassed in this test script)
            try:
                polished_sections = await self.polisher.polish_context(
                    {
                        "company_information": research_data.get(
                            "official_content", {}
                        ).get("company_information", {}),
                        "sustainability_data": research_data.get(
                            "official_content", {}
                        ).get("sustainability_reporting", {}),
                        "news_coverage": research_data.get(
                            "recent_developments", {}
                        ).get("news_coverage", {}),
                    }
                )

                enhanced_data["raw_content"] = research_data
                enhanced_data["polished_content"] = polished_sections

            except Exception as polish_err:
                logger.error(f"Error during content polishing: {polish_err}")
                enhanced_data["raw_content"] = research_data
                enhanced_data["polishing_error"] = str(polish_err)

            # Add source credibility context
            if (
                "source_credibility" in research_data
                and "reference_info" in research_data["source_credibility"]
            ):
                enhanced_data["source_analysis"] = {
                    "credibility_metrics": research_data["source_credibility"],
                    "source_relationships": {
                        url: {
                            "related_sources": [
                                rel_url
                                for rel_url, rel_data in research_data[
                                    "source_credibility"
                                ]["reference_info"].items()
                                if isinstance(rel_data, dict)
                                and rel_data.get("domain") == data.get("domain")
                                and rel_url != url
                            ],
                            "domain_authority": data.get("score", 0),
                        }
                        for url, data in research_data["source_credibility"][
                            "reference_info"
                        ].items()
                        if isinstance(data, dict)
                    },
                }

            return enhanced_data

        except Exception as e:
            logger.error(f"Error preparing context: {e}", exc_info=True)
            # Return original data if enhancement fails
            return {"error": str(e), "original_data": research_data}


# --- END: V2 - Combined Code from context_analyzer.py ---


# --- START: V2 - Main PDF Generation Code ---

# --- V2 DESIGN CHANGE: Simplified asset path for local testing ---
ASSETS_DIR = os.path.join(os.path.dirname(__file__), "assets")

# Initialize font variables with default Helvetica
FONT_REGULAR = "Helvetica"
FONT_BOLD = "Helvetica-Bold"
FONT_SEMIBOLD = "Helvetica-Bold"

# Try to load OpenSans fonts if available
try:
    opensans_regular = os.path.join(ASSETS_DIR, "OpenSans-VariableFont_wdth,wght.ttf")
    opensans_italic = os.path.join(
        ASSETS_DIR, "OpenSans-Italic-VariableFont_wdth,wght.ttf"
    )

    if all(os.path.exists(f) for f in [opensans_regular, opensans_italic]):
        pdfmetrics.registerFont(TTFont("OpenSans", opensans_regular))
        pdfmetrics.registerFont(TTFont("OpenSans-Italic", opensans_italic))

        FONT_REGULAR = "OpenSans"
        FONT_BOLD = "OpenSans"
        FONT_SEMIBOLD = "OpenSans"

        logger.info("Successfully registered OpenSans variable fonts")
    else:
        logger.warning(
            f"OpenSans font files not found in '{ASSETS_DIR}'. Using Helvetica as fallback."
        )
except Exception as e:
    logger.warning(
        f"Could not register Open Sans fonts: {str(e)}. Using Helvetica as fallback."
    )

# --- Define ReFED Brand Styles ---
REFED_DARK = colors.HexColor("#384954")
REFED_GREEN = colors.HexColor("#48B674")
REFED_LIGHT_GREY = colors.HexColor("#EFEDEB")
REFED_LIGHT_GREEN_TINT = colors.HexColor("#D1ECC1")

# --- Define Logo Path ---
LOGO_PATH = os.path.join(ASSETS_DIR, "refed_logo.png")

# --- End ReFED Styles ---


def header_and_footer(canvas, doc):
    """
    Adds a branded header and footer to each page.
    """
    canvas.saveState()
    page_width = doc.width + doc.leftMargin + doc.rightMargin
    page_height = doc.height + doc.topMargin + doc.bottomMargin

    # --- Header ---
    if os.path.exists(LOGO_PATH):
        # Draw a centered logo in the header band (between page top and separator line)
        img_width = 1.5 * inch
        img_height = 0.45 * inch
        line_y = page_height - 1.25 * inch
        band_top = page_height - 0.15 * inch
        band_height = max(0.2 * inch, band_top - line_y)
        x_pos = (page_width - img_width) / 2.0
        y_pos = line_y + (band_height - img_height) / 2.0
        try:
            logger.info(
                f"Drawing logo: {LOGO_PATH} at ({x_pos:.1f}, {y_pos:.1f}) size {img_width/inch:.2f}in x {img_height/inch:.2f}in"
            )
            canvas.drawImage(
                LOGO_PATH,
                x_pos,
                y_pos,
                width=img_width,
                height=img_height,
                preserveAspectRatio=True,
                mask="auto",
            )
        except Exception as e:
            logger.warning(f"Failed to draw logo: {e}")
    else:
        logger.warning(f"Logo not found at {LOGO_PATH} for header.")

    canvas.setStrokeColor(REFED_GREEN)
    canvas.setLineWidth(1)
    canvas.line(
        doc.leftMargin,
        page_height - 1.25 * inch,
        page_width - doc.rightMargin,
        page_height - 1.25 * inch,
    )

    # --- Footer ---
    canvas.setFont(FONT_REGULAR, 9)
    canvas.setFillColor(REFED_DARK)

    # --- V2 DESIGN CHANGE: Updated footer text ---
    footer_text = "ReFED Research Report"
    canvas.drawString(doc.leftMargin, 0.75 * inch, footer_text)

    page_num_text = f"Page {doc.page}"
    canvas.drawRightString(page_width - doc.rightMargin, 0.75 * inch, page_num_text)

    canvas.restoreState()


def _convert_markdown_to_elements(
    md_text: str, body_style: ParagraphStyle, subsection_style: ParagraphStyle
) -> List[Any]:
    """
    Basic converter for the final markdown report into ReportLab elements.
    """
    if md_text is None:
        return [Paragraph("No content available.", body_style)]
    if not isinstance(md_text, str):
        md_text = str(md_text)
    if not md_text.strip():
        return [Paragraph("No content available.", body_style)]

    md_text = md_text.replace("\r\n", "\n")
    md_text = re.sub(r"\n{3,}", "\n\n", md_text)
    md_text = re.sub(r"^\s*```markdown", "", md_text, flags=re.MULTILINE)
    md_text = re.sub(r"```\s*$", "", md_text, flags=re.MULTILINE)
    md_text = md_text.strip()

    elements = []
    in_list = False
    list_items = []

    def flush_list():
        nonlocal list_items, in_list, elements
        if list_items:
            list_item_style = ParagraphStyle(
                name="ListItem", parent=body_style, leftIndent=20
            )
            list_flow = ListFlowable(
                [ListItem(Paragraph(item, list_item_style)) for item in list_items],
                bulletType="bullet",
                leftIndent=20,
                bulletColor=REFED_DARK,
            )
            elements.append(list_flow)
            list_items = []
        in_list = False

    if not md_text:
        return [Paragraph("No summary was generated.", body_style)]

    for line in md_text.split("\n"):
        line_strip = line.strip()

        if not line_strip:
            if in_list:
                flush_list()
            elements.append(Spacer(1, 0.1 * inch))
            continue

        if line_strip.startswith("## "):
            if in_list:
                flush_list()
            elements.append(Paragraph(line_strip[3:], subsection_style))
        elif line_strip.startswith("### "):
            if in_list:
                flush_list()
            elements.append(Paragraph(f"<b>{line_strip[4:]}</b>", body_style))
        elif line_strip.startswith("* "):
            in_list = True
            list_items.append(line_strip[2:])
        else:
            if in_list:
                flush_list()
            elements.append(Paragraph(line_strip, body_style))

    if in_list:
        flush_list()
    return elements


def format_source_content(
    url: str,
    data: Dict[str, Any],
    body_style: ParagraphStyle,
    subsection_style: ParagraphStyle,
) -> List[Any]:
    """
    Helper function to format source content with metadata.
    """
    elements = []
    title = data.get("title") or url
    elements.append(Paragraph(title, subsection_style))
    elements.append(Paragraph(f"<i>Source: {url}</i>", body_style))

    meta_data = [
        ["Score", f"{data.get('relevance', 0):.2f}"],
        ["Query", data.get("query_context", "N/A")],
        ["Timestamp", data.get("timestamp", "N/A")],
        ["Source Type", data.get("source_type", "N/A")],
    ]
    t = Table(meta_data, colWidths=[1.5 * inch, 4 * inch])
    t.setStyle(
        TableStyle(
            [
                ("GRID", (0, 0), (-1, -1), 0.5, REFED_LIGHT_GREEN_TINT),
                ("BACKGROUND", (0, 0), (0, -1), REFED_LIGHT_GREY),
                ("FONTNAME", (0, 0), (-1, -1), FONT_REGULAR),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
                ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                ("PADDING", (0, 0), (-1, -1), 4),
                ("TEXTCOLOR", (0, 0), (-1, -1), REFED_DARK),
            ]
        )
    )
    elements.append(t)
    elements.append(Spacer(1, 0.1 * inch))

    if data.get("raw_content"):
        elements.append(Paragraph("<b>Raw Content:</b>", body_style))
        raw_content = str(data["raw_content"])
        elements.append(Paragraph(raw_content, body_style))
        elements.append(Spacer(1, 0.1 * inch))

    if data.get("content"):
        elements.append(Paragraph("<b>Processed Content (Snippet):</b>", body_style))
        elements.append(Paragraph(data["content"], body_style))

    elements.append(Spacer(1, 0.2 * inch))
    return elements


async def create_enhanced_research_pdf(
    research_data: Dict[str, Any], output: io.BytesIO
) -> None:
    """
    Creates a comprehensive PDF optimized for AI consumption.
    """
    company_name = None
    if isinstance(research_data, dict):
        company_name = (
            research_data.get("company_name")
            or research_data.get("name")
            or research_data.get("company_identity", {}).get("name")
            or (research_data.get("final_summary", {}) or {}).get("company_name")
            or (research_data.get("research_meta", {}) or {}).get(
                "company_name"
            )  # Corrected key
        )
        logger.debug(f"Extracted company name: {company_name}")

    analyzer = ContextAnalyzer()
    try:
        enhanced_data = await analyzer.prepare_context(research_data)
        logger.debug(
            f"Context analyzer returned keys: {enhanced_data.keys() if isinstance(enhanced_data, dict) else 'Not a dictionary'}"
        )
    except Exception as e:
        logger.error(
            f"Failed to prepare context: {e}. PDF will be incomplete.", exc_info=True
        )
        enhanced_data = {
            "company_identity": {"name": company_name} if company_name else {}
        }

    default_data = {
        "company_identity": {
            "name": company_name or "Company",
            "headquarters": "Not Available",
            "industry": "Not Available",
            "region": "Not Available",
            "scale": "Not Available",
            "website": "Not Available",
        },
        "research_meta": {
            "queries_used": {},
            "successful_extractions": {
                "total_analyzed": 0,
                "relevant_sources": 0,
                "contacts_found": 0,
            },
            "timestamp": datetime.now().isoformat(),
        },
        "final_summary": {"markdown_report": ""},
        "source_credibility": {"reference_info": {}},
        "official_content": {"company_information": {}, "sustainability_reporting": {}},
        "recent_developments": {"news_coverage": {}},
        "key_personnel": {"identified_contacts": {}},
        "engagement_signals": {"partnerships_and_initiatives": {}},
    }

    enhanced_data = {**default_data, **enhanced_data}
    for key in default_data:
        if (
            isinstance(default_data[key], dict)
            and key in enhanced_data
            and enhanced_data[key] is not None
        ):
            enhanced_data[key] = {**default_data[key], **enhanced_data[key]}

    company_name = enhanced_data["company_identity"]["name"]

    doc = SimpleDocTemplate(
        output,
        pagesize=A4,
        rightMargin=0.75 * inch,
        leftMargin=0.75 * inch,
        topMargin=1.5 * inch,  # V2: Increased top margin
        bottomMargin=1.25 * inch,  # V2: Increased bottom margin
        title=f"{company_name} Research Report",
        author="ReFED Development Team",
    )

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "ReFEDTitle",
        parent=styles["Heading1"],
        fontName=FONT_BOLD,
        fontSize=24,
        spaceAfter=30,
        alignment=TA_CENTER,
        textColor=REFED_DARK,
    )
    section_style = ParagraphStyle(
        "ReFEDSection",
        parent=styles["Heading2"],
        fontName=FONT_SEMIBOLD,
        fontSize=16,
        spaceBefore=20,
        spaceAfter=10,
        textColor=REFED_GREEN,
    )
    subsection_style = ParagraphStyle(
        "ReFEDSubsection",
        parent=styles["Heading3"],
        fontName=FONT_SEMIBOLD,
        fontSize=14,
        spaceBefore=15,
        spaceAfter=8,
        textColor=REFED_DARK,
    )
    body_style = ParagraphStyle(
        "ReFEDBody",
        parent=styles["Normal"],
        fontName=FONT_REGULAR,
        fontSize=10,
        leading=14,
        alignment=TA_LEFT,
        textColor=REFED_DARK,
        spaceAfter=6,
    )
    table_style = TableStyle(
        [
            ("BACKGROUND", (0, 0), (-1, 0), REFED_LIGHT_GREY),
            ("TEXTCOLOR", (0, 0), (-1, 0), REFED_DARK),
            ("ALIGN", (0, 0), (-1, -1), "LEFT"),
            ("FONTNAME", (0, 0), (-1, 0), FONT_SEMIBOLD),
            ("FONTSIZE", (0, 0), (-1, 0), 10),
            ("FONTNAME", (0, 1), (-1, -1), FONT_REGULAR),
            ("FONTSIZE", (0, 1), (-1, -1), 10),
            ("BOTTOMPADDING", (0, 0), (-1, 0), 12),
            ("TOPPADDING", (0, 0), (-1, 0), 12),
            ("BOTTOMPADDING", (0, 1), (-1, -1), 8),
            ("GRID", (0, 0), (-1, -1), 1, REFED_LIGHT_GREEN_TINT),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ]
    )
    source_analysis_table_style = TableStyle(
        [
            ("BACKGROUND", (0, 0), (-1, 0), REFED_LIGHT_GREY),
            ("TEXTCOLOR", (0, 0), (-1, 0), REFED_DARK),
            ("ALIGN", (0, 0), (-1, -1), "LEFT"),
            ("FONTNAME", (0, 0), (-1, 0), FONT_SEMIBOLD),
            ("FONTNAME", (0, 1), (-1, -1), FONT_REGULAR),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("GRID", (0, 0), (-1, -1), 0.5, REFED_LIGHT_GREEN_TINT),
            ("PADDING", (0, 0), (-1, -1), 4),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ]
    )

    story = []

    # --- Title Page ---
    story.append(Spacer(1, 1.5 * inch))  # Space down from header
    story.append(Paragraph(f"{company_name} Research Report", title_style))
    story.append(
        Paragraph(f"Generated: {datetime.now().strftime('%Y-%m-%d')}", body_style)
    )
    story.append(Spacer(1, 20))

    # --- Table of Contents ---
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
            style = ParagraphStyle(name="TOCSub", parent=body_style, leftIndent=20)
        story.append(Paragraph(item, style))
    story.append(PageBreak())

    # --- Section 1: AI-Generated Briefings ---
    story.append(Paragraph("1. AI-Generated Briefings", section_style))
    story.append(
        Paragraph("AI-generated summaries for each research category.", body_style)
    )
    md_report = enhanced_data.get("final_summary", {}).get("markdown_report", "")
    story.extend(_convert_markdown_to_elements(md_report, body_style, subsection_style))
    story.append(PageBreak())

    # --- Section 2: Company Profile ---
    story.append(Paragraph("2. Company Profile", section_style))
    identity_data = enhanced_data["company_identity"]

    # Helper function to safely get values and convert None to 'Not Available'
    def safe_get(d, key, default="Not Available"):
        value = d.get(key, default)
        return value if value is not None else default

    company_info = [
        [
            Paragraph("Company Profile", body_style),
            Paragraph(safe_get(identity_data, "name"), body_style),
        ],
        [
            Paragraph("Headquarters", body_style),
            Paragraph(safe_get(identity_data, "headquarters"), body_style),
        ],
        [
            Paragraph("Industry", body_style),
            Paragraph(safe_get(identity_data, "industry"), body_style),
        ],
        [
            Paragraph("Region", body_style),
            Paragraph(safe_get(identity_data, "region"), body_style),
        ],
        [
            Paragraph("Scale", body_style),
            Paragraph(safe_get(identity_data, "scale"), body_style),
        ],
        [
            Paragraph("Website", body_style),
            Paragraph(safe_get(identity_data, "website"), body_style),
        ],
    ]
    t = Table(company_info, colWidths=[1.5 * inch, 4.5 * inch])
    t.setStyle(table_style)
    story.append(t)
    story.append(Spacer(1, 0.2 * inch))
    story.append(PageBreak())

    # --- Section 3: Research Process & Queries ---
    story.append(Paragraph("3. Research Process & Queries", section_style))
    meta = enhanced_data["research_meta"]
    story.append(Paragraph("Research Queries by Category:", subsection_style))
    for query_type, queries in meta.get("queries_used", {}).items():
        story.append(
            Paragraph(f"<b>{query_type.replace('_', ' ').title()}:</b>", body_style)
        )
        query_list = queries if isinstance(queries, list) else [str(queries)]
        query_items = [
            ListItem(Paragraph(query, body_style), leftIndent=20)
            for query in query_list
        ]
        story.append(
            ListFlowable(
                query_items, bulletType="bullet", leftIndent=20, bulletColor=REFED_DARK
            )
        )
    story.append(PageBreak())

    # --- Section 4: Source Credibility Analysis ---
    story.append(Paragraph("4. Source Credibility Analysis", section_style))
    story.append(Paragraph("Source Credibility Analysis:", subsection_style))
    source_analysis_data = [["Source", "Type", "Score", "Domain"]]
    source_info = enhanced_data.get("source_credibility", {}).get("reference_info", {})
    if source_info:
        for url, info in source_info.items():
            if not isinstance(info, dict):
                continue
            title_p = Paragraph(info.get("title", url), body_style)
            try:
                score_val = f"{float(info.get('score', 0)):.2f}"
            except (ValueError, TypeError):
                score_val = "N/A"

            source_analysis_data.append(
                [
                    title_p,
                    info.get("source_type", "N/A"),
                    score_val,
                    info.get("domain", "N/A"),
                ]
            )

        t = Table(
            source_analysis_data, colWidths=[3 * inch, 1 * inch, 0.7 * inch, 1.3 * inch]
        )
        t.setStyle(source_analysis_table_style)
        story.append(t)
    else:
        story.append(Paragraph("No source credibility data available.", body_style))
    story.append(PageBreak())

    # --- Section 5: Research Statistics ---
    story.append(Paragraph("5. Research Statistics", section_style))
    stats_meta = enhanced_data.get("research_meta", {})
    stats_data = stats_meta.get("successful_extractions", {})
    stats = [
        [Paragraph("Research Statistics", body_style), ""],
        [
            Paragraph("Total Sources Analyzed", body_style),
            str(stats_data.get("total_analyzed", 0)),
        ],
        [
            Paragraph("Relevant Sources", body_style),
            str(stats_data.get("relevant_sources", 0)),
        ],
        [
            Paragraph("Contacts Identified", body_style),
            str(stats_data.get("contacts_found", 0)),
        ],
        [
            Paragraph("Research Date", body_style),
            (
                stats_meta.get("timestamp", "N/A").split("T")[0]
                if stats_meta.get("timestamp")
                else "N/A"
            ),
        ],
    ]
    t = Table(stats, colWidths=[2 * inch, 4 * inch])
    t.setStyle(table_style)
    story.append(t)
    story.append(PageBreak())

    # --- Section 6: Raw Data Appendix ---
    story.append(Paragraph("6. Raw Data Appendix", section_style))
    story.append(
        Paragraph(
            "This appendix contains the detailed source material, including metadata and full extracted text.",
            body_style,
        )
    )

    def add_raw_data_section(title, section_data):
        story.append(Paragraph(title, subsection_style))
        if not section_data:
            story.append(Paragraph("No data found for this section.", body_style))
            return

        item_found = False
        for url, data in section_data.items():
            if isinstance(data, dict):
                story.extend(
                    format_source_content(url, data, body_style, subsection_style)
                )
                item_found = True
            else:
                logger.warning(f"Skipping invalid data entry for {title}: {data}")

        if not item_found:
            story.append(Paragraph("No data found for this section.", body_style))

        story.append(PageBreak())

    add_raw_data_section(
        "6.1 Company Information",
        enhanced_data.get("official_content", {}).get("company_information", {}),
    )
    add_raw_data_section(
        "6.2 News & Recent Developments",
        enhanced_data.get("recent_developments", {}).get("news_coverage", {}),
    )
    add_raw_data_section(
        "6.3 Sustainability & FLW Data",
        enhanced_data.get("official_content", {}).get("sustainability_reporting", {}),
    )
    add_raw_data_section(
        "6.4 Contact Information",
        enhanced_data.get("key_personnel", {}).get("identified_contacts", {}),
    )
    add_raw_data_section(
        "6.5 Engagement & Partnerships",
        enhanced_data.get("engagement_signals", {}).get(
            "partnerships_and_initiatives", {}
        ),
    )

    # --- Build the PDF ---
    try:
        doc.build(story, onFirstPage=header_and_footer, onLaterPages=header_and_footer)
        logger.info(
            f"AI-Optimized ReFED-styled PDF report generated for {company_name}"
        )
    except Exception as build_err:
        logger.error(f"CRITICAL: PDF doc.build() failed: {build_err}", exc_info=True)
        raise


# --- END: V2 - Main PDF Generation Code ---


def create_executive_summary_pdf(
    markdown_content: str, company_name: str, output_path: str
) -> None:
    """
    Generate a clean, branded 1-2 page executive summary PDF from markdown.

    Args:
        summary_markdown: The markdown-formatted executive summary text
        company_name: Name of the company for the title
        output_path: File path to save the generated PDF
    """
    logger.info(f"Generating executive summary PDF for {company_name}")

    # Set up document with smaller margins for executive summary
    doc = SimpleDocTemplate(
        output_path,
        pagesize=A4,
        leftMargin=0.75 * inch,
        rightMargin=0.75 * inch,
        topMargin=1.5 * inch,
        bottomMargin=1 * inch,
        title=f"{company_name} - Executive Summary",
    )

    story = []

    # Define styles for executive summary
    title_style = ParagraphStyle(
        "ExecutiveTitle",
        parent=getSampleStyleSheet()["Heading1"],
        fontName=FONT_SEMIBOLD,
        fontSize=20,
        textColor=REFED_DARK,
        spaceAfter=16,
        leading=24,
    )

    section_style = ParagraphStyle(
        "ExecutiveSection",
        parent=getSampleStyleSheet()["Heading2"],
        fontName=FONT_SEMIBOLD,
        fontSize=14,
        textColor=REFED_GREEN,
        spaceBefore=14,
        spaceAfter=8,
        leading=17,
    )

    body_style = ParagraphStyle(
        "ExecutiveBody",
        parent=getSampleStyleSheet()["BodyText"],
        fontName=FONT_REGULAR,
        fontSize=10,
        textColor=REFED_DARK,
        leading=14,
        spaceAfter=10,
        alignment=TA_LEFT,
    )

    bullet_style = ParagraphStyle(
        "ExecutiveBullet",
        parent=body_style,
        leftIndent=20,
        bulletIndent=10,
        spaceAfter=6,
    )

    # Parse markdown and convert to PDF elements
    lines = markdown_content.split("\n")
    i = 0

    while i < len(lines):
        line = lines[i].strip()

        if not line:
            i += 1
            continue

        # Parse markdown headers
        if line.startswith("# "):
            # Main title (H1)
            text = line[2:].strip()
            story.append(Paragraph(text, title_style))
        elif line.startswith("## "):
            # Section header (H2)
            text = line[3:].strip()
            story.append(Paragraph(text, section_style))
        elif line.startswith("### "):
            # Subsection (H3) - treat as bold body
            text = line[4:].strip()
            story.append(Paragraph(f"<b>{text}</b>", body_style))
        elif line.startswith("* ") or line.startswith("- "):
            # Bullet point
            text = line[2:].strip()
            # Clean up any markdown bold/italic
            # Handle bold text properly
            text = re.sub(r"\*\*(.*?)\*\*", r"<b>\1</b>", text)
            # Handle italic text properly (but not * that's part of a list)
            text = re.sub(r"(?<!\*)\*([^\*]+?)\*(?!\*)", r"<i>\1</i>", text)
            # Clean up any remaining raw asterisks
            text = text.replace("**", "").replace("*", "")
            # Escape special XML characters
            text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            # But allow our intentional HTML tags
            text = text.replace("&lt;b&gt;", "<b>").replace("&lt;/b&gt;", "</b>")
            text = text.replace("&lt;i&gt;", "<i>").replace("&lt;/i&gt;", "</i>")
            story.append(Paragraph(f"• {text}", bullet_style))
        elif line.startswith("**") and line.endswith("**"):
            # Bold paragraph
            text = line.strip("*").strip()
            story.append(Paragraph(f"<b>{text}</b>", body_style))
        else:
            # Regular paragraph
            # Handle inline markdown formatting
            text = line
            # Bold text
            text = re.sub(r"\*\*(.*?)\*\*", r"<b>\1</b>", text)
            # Italic text
            text = re.sub(r"\*(.*?)\*", r"<i>\1</i>", text)
            # Links (just show the text, not the URL)
            text = re.sub(r"\[(.*?)\]\(.*?\)", r"\1", text)

            story.append(Paragraph(text, body_style))

        i += 1

    # Build PDF with header/footer
    try:
        doc.build(story, onFirstPage=header_and_footer, onLaterPages=header_and_footer)
        logger.info(f"Executive summary PDF generated for {company_name}")
    except Exception as build_err:
        logger.error(
            f"Failed to build executive summary PDF: {build_err}", exc_info=True
        )
        raise
