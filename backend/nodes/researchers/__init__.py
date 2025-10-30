# backend/nodes/researchers/__init__.py
# --- v2: Import our 5 new/refocused nodes ---
from .company import CompanyBriefNode
from .news import NewsSignalNode
from .flw import FLWAnalyzer
from .contact_finder import ContactFinderNode
from .engagement_finder import EngagementFinderNode

# --- v2: Update __all__ to export the 5 new nodes ---
__all__ = [
    "CompanyBriefNode",
    "NewsSignalNode",
    "FLWAnalyzer",
    "ContactFinderNode",
    "EngagementFinderNode"
]