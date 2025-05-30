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

# Google Scholar configuration
SCHOLAR_USE_SELENIUM = True  # Whether to use Selenium for Google Scholar
SCHOLAR_SLEEP_BETWEEN_QUERIES = (35, 60)  # seconds, min/max
SCHOLAR_MAX_PAGES = 3  # stop after this many "next" clicks

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

# --- stage-2 relevance scoring  ----------------------------
# Signal class weights for the enhanced scoring system
RELEVANCE_WEIGHTS = {
    # Positive signals
    "keyword_hit": 0.5,          # Baseline: we fetched it
    "canonical_term": 2.0,       # A: Almost a sure thing
    "dataset_author_seed": 1.5,  # B: Cites seed PMID or matches known author
    "integration_term": 1.0,     # C: Integration/market structure term
    "scope_term": 1.0,          # D: US health-system scope cue
    "journal_whitelist": 0.5,    # E: Journal in whitelist
    
    # Negative signals
    "neg_geography": -1.0,       # F: Negative geography term
    "neg_domain": -1.0,         # G: Negative domain term
    
    # Minor heuristics
    "short_title": 0.0,        # Titles < 5 words (often editorials)
    "old_paper": -0.5,          # Year < 2008 (Compendium launched 2016)
}

# Keep anything with composite score >= 0
RELEVANCE_THRESHOLD = 0.0
