"""
Tests for the EmailGeneratorService.
"""
import os
import json
import pytest
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock

from backend.services.email_generator import EmailGeneratorService

# Configure pytest-asyncio marker
pytestmark = pytest.mark.asyncio

@pytest.fixture
def email_generator():
    """Create an EmailGeneratorService instance for testing."""
    return EmailGeneratorService(openai_api_key="test-key-for-unit-tests")

@pytest.fixture
def mock_template_content():
    """Mock template content for testing."""
    return """Subject: Following up on climate commitments

Dear {name},

I noticed {company}'s impressive work on {focus_area}. I'd love to discuss how our solution could support your initiatives.

Best regards,
Erik"""

@pytest.fixture
def mock_research_context():
    """Mock research context for testing."""
    return {
        "company_brief.json": {
            "company_brief_data": {
                "https://example.com/news1": {
                    "title": "Company Announces Climate Goals",
                    "content": "Company X has announced ambitious climate goals..."
                }
            }
        },
        "notes.txt": "Recent press coverage highlights their focus on methane reduction."
    }

@pytest.fixture
def mock_airtable_context():
    """Mock Airtable context for testing."""
    return {
        "name": "Test Company",
        "title": "Sustainability Director",
        "summary": "Leading energy company focused on emissions reduction",
        "angle_for_outreach": "Recent methane reduction commitments",
        "note": "Strong interest in technology solutions"
    }

async def test_generate_email(email_generator, mock_template_content, mock_research_context, mock_airtable_context):
    """Test email generation with mock data."""
    # Create async mock for OpenAI client
    mock_client = AsyncMock()
    mock_completion = AsyncMock()
    mock_completion.choices = [MagicMock(message=MagicMock(content="Generated email content"))]
    mock_client.chat.completions.create = AsyncMock(return_value=mock_completion)
    
    # Replace the OpenAI client with our mock
    email_generator.client = mock_client
    
    email = await email_generator.generate_email(
        template_content=mock_template_content,
        research_context=mock_research_context,
        airtable_context=mock_airtable_context,
        contact_name="John Doe"
    )
    
    assert email == "Generated email content"
    
    # Verify the mock was called with correct parameters
    mock_client.chat.completions.create.assert_called_once()

async def test_fetch_template(email_generator):
    """Test template fetching."""
    # Mock template manager
    mock_template = MagicMock(name="Test Template", template_type="TEST_TEMPLATE")
    mock_template_content = "Template content"
    
    mock_manager = AsyncMock()
    mock_manager.get_template_id.return_value = "test_id"
    mock_manager.templates = {"TEST_TEMPLATE": mock_template}
    mock_manager.refresh_templates = AsyncMock()
    
    with patch('backend.utils.email_templates.get_template_manager', return_value=mock_manager):
        # Mock Drive service
        mock_drive = MagicMock()
        mock_drive.files().get_media().execute.return_value = mock_template_content.encode()
        email_generator.drive_service = mock_drive
        
        result = await email_generator.fetch_template("TEST_TEMPLATE")
        assert result == ("Test Template", mock_template_content)
        
        # Verify mocks were called correctly
        mock_manager.refresh_templates.assert_called_once()
        mock_manager.get_template_id.assert_called_once_with("TEST_TEMPLATE")

@pytest.mark.asyncio
async def test_fetch_research_context(email_generator):
    """Test research context fetching."""
    mock_files = {
        'files': [
            {'id': 'file1', 'name': 'test.json', 'mimeType': 'application/json'},
            {'id': 'file2', 'name': 'notes.txt', 'mimeType': 'text/plain'}
        ]
    }
    
    mock_json_content = json.dumps({"key": "value"}).encode()
    mock_text_content = "Test notes".encode()
    
    # Mock Drive service
    email_generator.drive_service = MagicMock()
    email_generator.drive_service.files().list().execute.return_value = mock_files
    
    def mock_get_media(*args, **kwargs):
        file_id = kwargs.get('fileId') or args[0].get('fileId')
        if file_id == 'file1':
            return MagicMock(execute=lambda: mock_json_content)
        return MagicMock(execute=lambda: mock_text_content)
    
    email_generator.drive_service.files().get_media = mock_get_media
    
    result = await email_generator.fetch_research_context("https://drive.google.com/folders/test_folder")
    
    assert 'test.json' in result
    assert 'notes.txt' in result
    assert result['test.json'] == {"key": "value"}
    assert result['notes.txt'] == "Test notes"

@pytest.mark.asyncio
async def test_error_handling(email_generator):
    """Test error handling in the service."""
    # Test template fetch error
    result = await email_generator.fetch_template("NONEXISTENT_TEMPLATE")
    assert result is None
    
    # Test research context error
    email_generator.drive_service = None  # Simulate missing Drive service
    result = await email_generator.fetch_research_context("https://invalid/url")
    assert result == {}