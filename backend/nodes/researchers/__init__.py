# backend/nodes/researchers/__init__.py
from .financial import FinancialAnalyst
from .news import NewsScanner
from .industry import IndustryAnalyzer
from .company import CompanyAnalyzer
from .flw import FLWAnalyzer # <-- ADDED Import for the new analyzer

__all__ = [
    "FinancialAnalyst",
    "NewsScanner",
    "IndustryAnalyzer",
    "CompanyAnalyzer",
    "FLWAnalyzer" # <-- ADDED FLWAnalyzer to the list
]