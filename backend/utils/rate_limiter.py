"""
Global rate limiters for API services to prevent exceeding rate limits
when running multiple concurrent jobs.
"""
import asyncio
import time
from collections import deque
from typing import Optional


class TokenBucketRateLimiter:
    """
    Thread-safe token bucket rate limiter for async operations.

    Allows bursts up to max_tokens, then enforces rate_per_minute.
    """
    def __init__(self, rate_per_minute: int, max_burst: Optional[int] = None):
        """
        Args:
            rate_per_minute: Maximum requests per minute
            max_burst: Maximum burst size (defaults to rate_per_minute)
        """
        self.rate_per_minute = rate_per_minute
        self.max_tokens = max_burst or rate_per_minute
        self.tokens = self.max_tokens
        self.updated_at = time.monotonic()
        self.lock = asyncio.Lock()

        # Track recent requests for monitoring
        self.request_times = deque(maxlen=rate_per_minute)

    async def acquire(self, tokens: int = 1):
        """
        Acquire tokens from the bucket, waiting if necessary.

        Args:
            tokens: Number of tokens to acquire (usually 1 per request)
        """
        async with self.lock:
            while True:
                now = time.monotonic()

                # Refill tokens based on time elapsed
                time_passed = now - self.updated_at
                self.tokens = min(
                    self.max_tokens,
                    self.tokens + (time_passed * self.rate_per_minute / 60.0)
                )
                self.updated_at = now

                # If we have enough tokens, take them and return
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    self.request_times.append(now)
                    return

                # Calculate how long to wait for enough tokens
                tokens_needed = tokens - self.tokens
                wait_time = (tokens_needed * 60.0) / self.rate_per_minute

                # Wait and retry
                await asyncio.sleep(min(wait_time, 1.0))  # Max 1s wait per iteration

    def get_current_rpm(self) -> int:
        """Get current requests per minute based on recent activity."""
        now = time.monotonic()
        cutoff = now - 60

        # Count requests in the last minute
        return sum(1 for req_time in self.request_times if req_time > cutoff)

    def get_stats(self) -> dict:
        """Get rate limiter statistics."""
        return {
            "rate_limit_rpm": self.rate_per_minute,
            "current_rpm": self.get_current_rpm(),
            "available_tokens": int(self.tokens),
            "max_tokens": self.max_tokens,
            "utilization_percent": round((1 - self.tokens / self.max_tokens) * 100, 1)
        }


# Global rate limiters for each API service
# These are shared across ALL concurrent jobs

# Tavily API - Conservative limits to avoid 429 errors
# Basic plan: 100 RPM, we use 80 to leave safety margin
tavily_limiter = TokenBucketRateLimiter(
    rate_per_minute=80,  # Conservative: 80% of 100 RPM limit
    max_burst=20  # Allow small bursts
)

# OpenAI API - Tier 1 limits
# Default Tier 1: 500 RPM, we use 400 for safety
openai_limiter = TokenBucketRateLimiter(
    rate_per_minute=400,
    max_burst=50
)

# Gemini API - Depends on your plan
# Free tier: 60 RPM, Paid: 2000 RPM
# Assuming paid tier for production
gemini_limiter = TokenBucketRateLimiter(
    rate_per_minute=1500,  # Conservative for paid tier
    max_burst=100
)

# Airtable API - 5 req/sec = 300 RPM
airtable_limiter = TokenBucketRateLimiter(
    rate_per_minute=250,  # Conservative
    max_burst=30
)


def get_all_limiter_stats() -> dict:
    """Get statistics for all rate limiters."""
    return {
        "tavily": tavily_limiter.get_stats(),
        "openai": openai_limiter.get_stats(),
        "gemini": gemini_limiter.get_stats(),
        "airtable": airtable_limiter.get_stats()
    }
