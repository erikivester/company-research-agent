"""
Security middleware and helpers for the outreach email generator API.
"""
from typing import Optional, List
from datetime import datetime, timedelta
from fastapi import Request, HTTPException, Depends, Header
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import jwt
from jwt import InvalidTokenError
from pydantic import BaseModel
import logging
from slowapi import Limiter
from slowapi.util import get_remote_address

logger = logging.getLogger(__name__)

# Initialize rate limiter
limiter = Limiter(key_func=get_remote_address)

class JWTBearer(HTTPBearer):
    """Custom JWT bearer token authentication."""
    
    def __init__(self, secret_key: str, auto_error: bool = True):
        super().__init__(auto_error=auto_error)
        self.secret_key = secret_key
        
    async def __call__(self, request: Request) -> Optional[str]:
        credentials: HTTPAuthorizationCredentials = await super().__call__(request)
        
        if not credentials:
            if self.auto_error:
                raise HTTPException(
                    status_code=401,
                    detail="Not authenticated"
                )
            return None
            
        if not credentials.scheme == "Bearer":
            if self.auto_error:
                raise HTTPException(
                    status_code=401,
                    detail="Invalid authentication scheme"
                )
            return None
            
        try:
            payload = jwt.decode(
                credentials.credentials,
                self.secret_key,
                algorithms=["HS256"]
            )
            return payload
        except InvalidTokenError:
            raise HTTPException(
                status_code=401,
                detail="Invalid token"
            )

class SecurityConfig(BaseModel):
    """Configuration for security settings."""
    secret_key: str
    allowed_origins: List[str]
    rate_limit_requests: int = 100
    rate_limit_period: int = 3600  # 1 hour in seconds

def create_access_token(
    data: dict,
    secret_key: str,
    expires_delta: Optional[timedelta] = None
) -> str:
    """Create a JWT access token."""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(hours=1)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, secret_key, algorithm="HS256")

def sanitize_input(text: str) -> str:
    """
    Sanitize input text to prevent injection attacks.
    Removes or escapes potentially dangerous characters.
    """
    if not text:
        return text
        
    # Remove null bytes
    text = text.replace('\x00', '')
    
    # HTML escape special characters
    text = (
        text.replace('&', '&amp;')
            .replace('<', '&lt;')
            .replace('>', '&gt;')
            .replace('"', '&quot;')
            .replace("'", '&#x27;')
    )
    
    return text.strip()

def validate_folder_url(url: str) -> bool:
    """Validate Google Drive folder URL format."""
    return (
        url.startswith('https://drive.google.com/') and
        '/folders/' in url and
        len(url.split('/folders/')[-1]) >= 25  # Minimum ID length
    )

def get_current_user(secret_key: str, token: str = None) -> dict:
    """Get the current authenticated user from JWT token."""
    jwt_bearer = JWTBearer(secret_key=secret_key)
    if token is None:
        token = Depends(jwt_bearer)
    return token