"""
Custom exceptions for the email generator service.
"""


class EmailGeneratorError(Exception):
    """Base exception for email generator errors."""

    pass


class TemplateNotFoundError(EmailGeneratorError):
    """Raised when a requested template is not found."""

    pass


class DriveServiceError(EmailGeneratorError):
    """Raised when there are issues with Google Drive service."""

    pass


class ResearchContextError(EmailGeneratorError):
    """Raised when there are issues fetching or parsing research context."""

    pass


class EmailGenerationError(EmailGeneratorError):
    """Raised when there are issues generating the email content."""

    pass
