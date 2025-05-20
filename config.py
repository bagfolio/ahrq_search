"""
Configuration settings for the AHRQ Compendium Citation Tracker.
"""

import os
import pathlib
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

# User configuration
EMAIL = os.getenv("EMAIL", "your_email@domain.com")  # Required for PubMed and Unpaywall
MAX_RESULTS_PER_QUERY = 1000  # Maximum results to fetch per keyword/query

# Project paths
PROJECT_ROOT = pathlib.Path(__file__).parent.absolute()
KEYWORDS_PATH = PROJECT_ROOT / "keywords.yaml"
OUTPUT_DIR = PROJECT_ROOT / "output"
CACHE_DIR = PROJECT_ROOT / "cache"

# Ensure output and cache directories exist
OUTPUT_DIR.mkdir(exist_ok=True)
CACHE_DIR.mkdir(exist_ok=True)

# API rate limiting settings
PUBMED_REQUESTS_PER_SECOND = 3  # NCBI E-utilities guideline
SCHOLAR_SLEEP_SECONDS = 30  # Time to wait between Google Scholar requests
SCHOLAR_MAX_RESULTS = 100  # Cap Scholar results per query to avoid blocking

# Advanced settings
FULL_TEXT_FETCH = True  # Whether to attempt retrieving full-text for articles
REGEX_CLASSIFY = True  # Use regex to classify usage
SPACY_CLASSIFY = False  # Use spaCy model for classification (if trained)
SPACY_MODEL_PATH = PROJECT_ROOT / "models" / "usage_classifier"

# Output file names
ALL_HITS_CSV = "all_hits.csv"
USED_COMPENDIUM_CSV = "used_compendium.csv"
REPORT_HTML = "report.html"

# Compendium seed PMID (for citation graph traversal)
SEED_PMID = "30674227"  # 2019 JAMA Methods paper for Compendium

# File hashing (to avoid re-processing)
HASH_ALGORITHM = "sha256"
