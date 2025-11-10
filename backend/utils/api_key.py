"""API key management utilities."""
import os
import hmac
import hashlib

def verify_api_key(api_key: str) -> bool:
    """Verify an API key against the stored key."""
    stored_key = os.getenv("API_KEY")
    if not stored_key:
        return False
        
    # Use constant time comparison to prevent timing attacks
    return hmac.compare_digest(api_key, stored_key)