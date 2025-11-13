import logging
import os
from typing import Any, Dict

import google.generativeai as genai

logger = logging.getLogger(__name__)


class ContextPolisher:
    """Polishes research context JSON using Gemini for better readability and structure."""

    def __init__(self) -> None:
        self.gemini_key = os.getenv("GEMINI_API_KEY")
        if not self.gemini_key:
            logger.warning(
                "GEMINI_API_KEY environment variable is not set - will return unpolished context"
            )
            return

        try:
            # Configure Gemini
            genai.configure(api_key=self.gemini_key)
            self.model = genai.GenerativeModel("gemini-2.5-flash")
            logger.info("Context Polisher initialized with Gemini 2.5 Flash model")
        except Exception as e:
            logger.error(f"Failed to initialize Gemini model: {e}")
            self.model = None

    async def polish_context(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Processes the research context to improve readability and structure:
        1. Cleans and standardizes content formatting
        2. Adds descriptive metadata where helpful
        3. Ensures consistent structure
        4. Summarizes very long content while preserving originals
        """
        try:
            # If Gemini model not available, return original context
            if not hasattr(self, "model") or self.model is None:
                logger.warning(
                    "Gemini model not available - returning unpolished context"
                )
                return context

            system_prompt = """You are an expert data curator helping to polish a research context JSON document. 
            Your task is to improve the structure and readability of the content while preserving all important information.
            
            Guidelines:
            1. Clean and standardize all text content
            2. Add brief summaries for long content sections
            3. Ensure consistent formatting
            4. Add descriptive metadata where helpful
            5. Preserve all original content
            6. Maintain the existing structure but enhance it
            7. Focus on making the content more useful for writing persuasive emails
            
            CRITICAL: You must return a valid JSON object and NOTHING else.
            - Your response must start with a '{' and end with a '}'
            - Use proper JSON syntax with double quotes for keys and string values
            - Ensure all nested objects and arrays are properly formatted
            - Do not include any explanations or text outside the JSON object"""

            # Convert context to string with nice formatting
            context_str = str(context)

            response = await self.model.generate_content_async(
                f"{system_prompt}\n\nHere is the research context to polish:\n{context_str}",
                request_options={"timeout": 300},
            )

            content = ""
            if response and response.parts:
                content = "".join(
                    part.text for part in response.parts if hasattr(part, "text")
                ).strip()

            if not content:
                logger.warning(
                    "No response content from Gemini model, returning original context"
                )
                return context

            # Parse the response back to a dictionary
            try:
                import json

                # Try to detect if the content is already JSON
                content = content.strip()
                if not (content.startswith("{") and content.endswith("}")):
                    logger.warning(
                        "Response is not in JSON format, returning original context"
                    )
                    return context

                polished = json.loads(content)
                if not isinstance(polished, dict):
                    logger.warning(
                        "Parsed response is not a dictionary, returning original context"
                    )
                    return context

                logger.info("Successfully polished research context")
                return polished
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse Gemini response as JSON: {e}")
                return context

        except Exception as e:
            logger.error(f"Error during context polishing: {e}")
            return context  # Return original context if polishing fails
