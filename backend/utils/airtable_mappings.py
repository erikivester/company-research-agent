"""Maps for standardizing Airtable field values."""

REVENUE_BAND_MAPPINGS = {
    # Map common variations to exact Airtable options
    "<$1M": "<$1M",
    "< $1M": "<$1M",
    "Less than $1M": "<$1M",
    
    "$1M-$10M": "$1M-$10M",
    "$1M - $10M": "$1M-$10M",
    "1M to 10M": "$1M-$10M",
    
    "$10M-$50M": "$10M-$50M",
    "$10M - $50M": "$10M-$50M",
    "10M to 50M": "$10M-$50M",
    
    "$50M-$100M": "$50M-$100M",
    "$50M - $100M": "$50M-$100M",
    "50M to 100M": "$50M-$100M",
    
    "$100M-$500M": "$100M-$500M",
    "$100M - $500M": "$100M-$500M",
    "100M to 500M": "$100M-$500M",
    
    "$500M-$1B": "$500M-$1B",
    "$500M - $1B": "$500M-$1B",
    "500M to 1B": "$500M-$1B",
    
    "$1B-$10B": "$1B-$10B",
    "$1B - $10B": "$1B-$10B",
    "1B to 10B": "$1B-$10B",

    # These higher bands may not exist in some Airtable bases.
    # Map them to a safe fallback to avoid 422 INVALID_MULTIPLE_CHOICE_OPTIONS.
    "$10B-$50B": "Unknown",
    "$10B - $50B": "Unknown",
    "10B to 50B": "Unknown",
    "$50B+": "Unknown",
    
    # Handle unknown/unclear cases
    "Unknown": "Unknown",
    "Not Available": "Unknown",
    "N/A": "Unknown",
    "Undisclosed": "Unknown"
}