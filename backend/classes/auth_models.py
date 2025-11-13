"""
Pydantic models for authentication.
"""

from pydantic import BaseModel, EmailStr


class TokenRequest(BaseModel):
    """Request model for obtaining JWT token."""

    email: EmailStr
    api_key: str


class Token(BaseModel):
    """Response model for JWT token."""

    access_token: str
    token_type: str = "bearer"
