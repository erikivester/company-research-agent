"""
Cache management utilities for optimizing performance.
"""
from typing import Dict, Any, Optional
import time
import logging
from threading import Lock
from functools import wraps
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class CacheEntry:
    """Represents a cached item with expiration."""
    def __init__(self, value: Any, ttl: int):
        self.value = value
        self.expiry = datetime.now() + timedelta(seconds=ttl)
        
    @property
    def is_expired(self) -> bool:
        """Check if the cache entry has expired."""
        return datetime.now() > self.expiry

class CacheManager:
    """Thread-safe cache manager with TTL support."""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(CacheManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
        
    def __init__(self):
        if self._initialized:
            return
            
        self._cache: Dict[str, CacheEntry] = {}
        self._lock = Lock()
        self._last_cleanup = time.time()
        self._cleanup_interval = 300  # 5 minutes
        self._initialized = True
        
    def get(self, key: str) -> Optional[Any]:
        """Get a value from the cache if it exists and hasn't expired."""
        with self._lock:
            self._maybe_cleanup()
            
            if key not in self._cache:
                return None
                
            entry = self._cache[key]
            if entry.is_expired:
                del self._cache[key]
                return None
                
            return entry.value
            
    def set(self, key: str, value: Any, ttl: int = 3600):
        """Set a value in the cache with a TTL (default 1 hour)."""
        with self._lock:
            self._cache[key] = CacheEntry(value, ttl)
            
    def delete(self, key: str):
        """Remove a value from the cache."""
        with self._lock:
            self._cache.pop(key, None)
            
    def clear(self):
        """Clear all entries from the cache."""
        with self._lock:
            self._cache.clear()
            
    def _maybe_cleanup(self):
        """Periodically clean up expired entries."""
        now = time.time()
        if now - self._last_cleanup > self._cleanup_interval:
            expired_keys = [
                k for k, v in self._cache.items() 
                if v.is_expired
            ]
            for k in expired_keys:
                del self._cache[k]
            self._last_cleanup = now

def cached(ttl: int = 3600):
    """
    Decorator to cache function results.
    
    Args:
        ttl: Time-to-live in seconds (default 1 hour)
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Create cache key from function name and arguments
            key = f"{func.__name__}:{str(args)}:{str(kwargs)}"
            
            # Get cache manager instance
            cache = CacheManager()
            
            # Check cache
            if cached_value := cache.get(key):
                logger.debug(f"Cache hit for {key}")
                return cached_value
                
            # Call function and cache result
            result = await func(*args, **kwargs)
            cache.set(key, result, ttl)
            logger.debug(f"Cached result for {key}")
            
            return result
        return wrapper
    return decorator

def bulk_cache_update(cache_keys: Dict[str, Any], ttl: int = 3600):
    """
    Update multiple cache entries atomically.
    
    Args:
        cache_keys: Dictionary of keys and values to cache
        ttl: Time-to-live in seconds (default 1 hour)
    """
    cache = CacheManager()
    with cache._lock:
        for key, value in cache_keys.items():
            cache.set(key, value, ttl)