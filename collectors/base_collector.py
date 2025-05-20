"""
Base collector interface for AHRQ Compendium Citation Tracker.
All source-specific collectors inherit from this base class.
"""

from abc import ABC, abstractmethod
import pandas as pd
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

class BaseCollector(ABC):
    """Abstract base class for all citation collectors."""
    
    def __init__(self, email: str, max_results: int = 1000):
        """
        Initialize the collector.
        
        Args:
            email: Email address for API access
            max_results: Maximum results to fetch per query
        """
        self.email = email
        self.max_results = max_results
        self.source_name = self.__class__.__name__
        
    @abstractmethod
    def search(self, terms: List[str]) -> pd.DataFrame:
        """
        Search for citations using the provided terms.
        
        Args:
            terms: List of search terms/keywords
            
        Returns:
            DataFrame with normalized citation data
        """
        pass
        
    @abstractmethod
    def normalize_data(self, raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Normalize raw data from the source into a standard DataFrame.
        
        Standard columns:
        - title: Article title
        - authors: List of author names
        - journal: Journal name
        - year: Publication year
        - doi: DOI (lowercase)
        - pmid: PubMed ID
        - abstract: Abstract text
        - url: URL to article
        - source: Source name (PubMed, OpenAlex, etc.)
        - match_term: Term that matched this article
        
        Args:
            raw_data: Raw data from the source
            
        Returns:
            DataFrame with normalized citation data
        """
        pass
    
    def log_stats(self, df: pd.DataFrame, term: str) -> None:
        """Log statistics about the search results."""
        if df is not None and not df.empty:
            logger.info(f"{self.source_name}: Found {len(df)} results for term '{term}'")
        else:
            logger.warning(f"{self.source_name}: No results for term '{term}'")
    
    def add_match_term(self, df: pd.DataFrame, term: str) -> pd.DataFrame:
        """Add the match term to the DataFrame."""
        if df is not None and not df.empty:
            df['match_term'] = term
        return df
