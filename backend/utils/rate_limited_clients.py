"""
Rate-limited wrappers for API clients.

This module provides drop-in replacements for API clients that automatically
apply global rate limiting to prevent exceeding API quotas when running
multiple concurrent jobs.
"""
import logging
from typing import Any

from .rate_limiter import openai_limiter, gemini_limiter

logger = logging.getLogger(__name__)


class RateLimitedOpenAIWrapper:
    """
    Wrapper around OpenAI client that applies global rate limiting.

    Usage:
        client = RateLimitedOpenAIWrapper(openai.AsyncOpenAI(...))
        response = await client.chat.completions.create(...)
    """
    def __init__(self, openai_client):
        self._client = openai_client

    @property
    def chat(self):
        return self._ChatWrapper(self._client.chat)

    class _ChatWrapper:
        def __init__(self, chat_client):
            self._chat = chat_client

        @property
        def completions(self):
            return self._CompletionsWrapper(self._chat.completions)

        class _CompletionsWrapper:
            def __init__(self, completions_client):
                self._completions = completions_client

            async def create(self, *args, **kwargs):
                """Rate-limited create method."""
                await openai_limiter.acquire()
                logger.debug(f"OpenAI API call (RPM: {openai_limiter.get_current_rpm()})")
                return await self._completions.create(*args, **kwargs)


class RateLimitedGeminiWrapper:
    """
    Wrapper around Gemini GenerativeModel that applies global rate limiting.

    Usage:
        model = RateLimitedGeminiWrapper(genai.GenerativeModel(...))
        response = await model.generate_content_async(...)
    """
    def __init__(self, gemini_model):
        self._model = gemini_model

    async def generate_content_async(self, *args, **kwargs):
        """Rate-limited generate_content_async method."""
        await gemini_limiter.acquire()
        logger.debug(f"Gemini API call (RPM: {gemini_limiter.get_current_rpm()})")
        return await self._model.generate_content_async(*args, **kwargs)

    def generate_content(self, *args, **kwargs):
        """Pass-through for sync method (not rate-limited - shouldn't be used in async code)."""
        logger.warning("Using synchronous Gemini generate_content - this bypasses rate limiting!")
        return self._model.generate_content(*args, **kwargs)

    def __getattr__(self, name):
        """Pass through other attributes to the wrapped model."""
        return getattr(self._model, name)
