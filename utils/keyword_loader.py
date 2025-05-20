"""
Keyword loader utility for AHRQ Compendium Citation Tracker.
Loads and processes keywords from YAML configuration.
"""

import yaml
from typing import Dict, List, Any
import logging
import pathlib
from itertools import chain

logger = logging.getLogger(__name__)

class KeywordLoader:
    """Loads keywords from YAML file and provides access methods."""
    
    def __init__(self, yaml_path: pathlib.Path):
        """
        Initialize the keyword loader with path to YAML file.
        
        Args:
            yaml_path: Path to keywords.yaml file
        """
        self.yaml_path = yaml_path
        self.keywords = self._load_yaml()
        
    def _load_yaml(self) -> Dict[str, List[str]]:
        """Load keywords from YAML file."""
        try:
            with open(self.yaml_path, 'r', encoding='utf-8') as file:
                return yaml.safe_load(file)
        except Exception as e:
            logger.error(f"Failed to load keywords from {self.yaml_path}: {e}")
            raise
    
    def get_all_urls(self) -> List[str]:
        """Get all URL-based keywords (landing pages + PDF files)."""
        urls = []
        urls.extend(self.keywords.get('exact_urls', []))
        urls.extend(self.keywords.get('pdf_urls', []))
        return urls
    
    def get_all_phrases(self) -> List[str]:
        """Get all phrase-based keywords."""
        phrases = []
        phrases.extend(self.keywords.get('phrase_variants', []))
        return phrases
    
    def get_all_combos(self) -> List[str]:
        """Get all combination-based keywords."""
        combos = []
        combos.extend(self.keywords.get('year_combos', []))
        combos.extend(self.keywords.get('ahrq_combos', []))
        combos.extend(self.keywords.get('funding_acknowledgment', []))
        return combos
    
    def get_regex_patterns(self) -> List[str]:
        """Get regex patterns for usage detection."""
        return self.keywords.get('regex_usage_patterns', [])
    
    def get_all_search_terms(self) -> List[str]:
        """Get all search terms for API queries (urls + phrases + combos)."""
        return list(chain(
            self.get_all_urls(),
            self.get_all_phrases(),
            self.get_all_combos()
        ))

    def get_category_for_term(self, term: str) -> str:
        """
        Determine which category a term belongs to.
        
        Args:
            term: The search term
            
        Returns:
            Category string (url, phrase, year_combo, etc.)
        """
        for category, terms in self.keywords.items():
            if term in terms:
                return category
        return "unknown"
