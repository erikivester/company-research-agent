
"""
Pydantic models for the outreach email generation feature.
"""
from pydantic import BaseModel, Field, HttpUrl
from typing import Optional, Dict, Any

class AirtableContext(BaseModel):
    """Represents the context fields from an Airtable record."""
    name: str = Field(..., description="Company/prospect name")
    title: str = Field(..., description="Contact's title")
    summary: str = Field(..., description="Company summary")
    angle_for_outreach: str = Field(..., description="Strategic notes from the team")
    note: Optional[str] = Field(None, description="Additional relevant context")

class EmailGenerationRequest(BaseModel):
    """Request model for the email generation endpoint."""
    airtable_context: AirtableContext
    contact_name: str = Field(..., description="Full name of the person to email")
    google_drive_folder_url: HttpUrl = Field(..., description="URL to the research folder")
    template_type: str = Field(
        ...,
        description="Type of email template to use (see /templates endpoint for available types)"
    )

class EmailGenerationResponse(BaseModel):
    """Response model for the email generation endpoint."""
    email_text: str = Field(..., description="The generated email text")
    subject: str = Field(..., description="The email subject line")
    template_used: str = Field(..., description="Name of the template file that was used")
    context_used: Dict[str, bool] = Field(
        ...,
        description="Indicates which context sources were successfully used"
    )