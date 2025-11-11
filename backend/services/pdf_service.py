# backend/services/pdf_service.py
import io
import logging
import os
from datetime import datetime
from typing import Dict, Any, Tuple

from backend.utils.utils import generate_pdf_from_md
from backend.utils.enhanced_pdf import create_enhanced_research_pdf
from backend.utils.executive_summary_pdf import create_executive_summary_pdf

logger = logging.getLogger(__name__)

class PDFService:
    def __init__(self, config: Dict[str, Any]):
        self.output_dir = config.get("pdf_output_dir", "pdfs")
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def _generate_filename(self, company_name: str) -> str:
        """Generates a sanitized PDF filename."""
        safe_name = "".join(
            [c for c in company_name if c.isalnum() or c in (" ", "-")]
        ).rstrip()
        safe_name = safe_name.replace(" ", "_").replace("-", "_")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{safe_name}_research_report_{timestamp}.pdf"

    async def generate_pdf_stream(
        self, content: str | Dict[str, Any], company_name: str = "Company", 
        summary_mode: bool = True
    ) -> Tuple[bool, Any]:
        """
        Generates a PDF and returns it as a BytesIO stream.
        
        Args:
            content: Either markdown content (str) or research data dictionary
            company_name: Name of the company for the filename
            summary_mode: If True, generate 1-2 page executive summary (default).
                         If False, generate detailed multi-page report.
        
        Returns:
            Tuple[bool, Any]: (success, result)
            On success: (True, (BytesIO, str)) - The stream and the filename
            On failure: (False, str) - The error message
        """
        try:
            pdf_buffer = io.BytesIO()
            filename = self._generate_filename(company_name)

            # Choose the appropriate PDF generation method
            if isinstance(content, dict):
                # Use executive summary for concise 1-2 page report (default)
                if summary_mode:
                    await create_executive_summary_pdf(content, pdf_buffer)
                else:
                    # Use enhanced PDF for detailed multi-page report
                    await create_enhanced_research_pdf(content, pdf_buffer)
            else:
                # Use simple markdown conversion for markdown content
                generate_pdf_from_md(content, pdf_buffer)

            # Rewind the buffer to the beginning so it can be read
            pdf_buffer.seek(0)

            logger.info(f"Successfully generated PDF stream: {filename}")
            return (True, (pdf_buffer, filename))

        except Exception as e:
            logger.error(f"Failed to generate PDF stream: {e}", exc_info=True)
            return (False, str(e))