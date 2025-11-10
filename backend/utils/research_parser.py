"""
Utilities for parsing different types of research files.
"""
import json
from pathlib import Path
from typing import Dict, Any, Optional
import logging
from PyPDF2 import PdfReader
import markdown
import bs4

logger = logging.getLogger(__name__)

class ResearchFileParser:
    """Parser for different types of research files."""
    
    @staticmethod
    def parse_json(content: str) -> Dict[str, Any]:
        """Parse JSON content."""
        try:
            return json.loads(content)
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON content: {e}")
            return {}
            
    @staticmethod
    def parse_pdf(file_path: str) -> str:
        """Extract text content from PDF file."""
        try:
            reader = PdfReader(file_path)
            text = ""
            for page in reader.pages:
                text += page.extract_text() + "\n"
            return text.strip()
        except Exception as e:
            logger.error(f"Error parsing PDF file {file_path}: {e}")
            return ""
            
    @staticmethod
    def parse_markdown(content: str) -> str:
        """Convert markdown to plain text."""
        try:
            # Convert markdown to HTML
            html = markdown.markdown(content)
            # Convert HTML to plain text
            soup = bs4.BeautifulSoup(html, 'html.parser')
            return soup.get_text(separator='\n\n').strip()
        except Exception as e:
            logger.error(f"Error parsing markdown content: {e}")
            return content
            
    @staticmethod
    def parse_txt(content: str) -> str:
        """Parse plain text content."""
        return content.strip()
        
    @classmethod
    def parse_file(cls, file_path: str, content: str) -> Dict[str, Any]:
        """
        Parse file content based on file extension.
        
        Args:
            file_path: Path to the file
            content: Raw file content
            
        Returns:
            Parsed content as dictionary or string
        """
        ext = Path(file_path).suffix.lower()
        
        try:
            if ext == '.json':
                return {'type': 'json', 'content': cls.parse_json(content)}
            elif ext == '.pdf':
                return {'type': 'pdf', 'content': cls.parse_pdf(file_path)}
            elif ext == '.md':
                return {'type': 'markdown', 'content': cls.parse_markdown(content)}
            elif ext == '.txt':
                return {'type': 'text', 'content': cls.parse_txt(content)}
            else:
                logger.warning(f"Unsupported file type: {ext}")
                return {'type': 'unknown', 'content': content.strip()}
                
        except Exception as e:
            logger.error(f"Error parsing file {file_path}: {e}")
            return {'type': 'error', 'content': str(e)}