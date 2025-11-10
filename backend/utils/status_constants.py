"""Constants for research status tracking throughout the application."""

class ResearchStatus:
    """Standardized research status constants."""
    
    # Main status states
    QUEUED = "Queued"
    IN_PROGRESS = "In Progress"
    COLLECTING_DATA = "Collecting Data"
    CURATING_DOCUMENTS = "Curating Documents"
    ENRICHING_CONTENT = "Enriching Content"
    GENERATING_BRIEFINGS = "Generating Briefings"
    COMPILING_REPORT = "Compiling Report"
    CLASSIFYING = "Classifying"
    COMPLETED = "Completed"
    FAILED = "Failed"

    # Error status templates
    FAILED_CURATION = "Failed: Curation Error - {}"
    FAILED_ENRICHMENT = "Failed: Enrichment Error - {}"
    FAILED_CLASSIFICATION = "Failed: Classification Error - {}"
    FAILED_MISSING_COMPANY = "Failed: Missing Company Name"
    
    @classmethod
    def format_error(cls, status_template: str, error_msg: str, max_length: int = 50) -> str:
        """Format an error status message with truncation."""
        return status_template.format(error_msg[:max_length])