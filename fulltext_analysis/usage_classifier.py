"""
Usage classifier for AHRQ Compendium Citation Tracker.
Determines if papers actually use the Compendium data versus just mentioning it.
"""

import re
import logging
from typing import Dict, List, Tuple, Union, Optional
import pandas as pd
import pathlib

from utils.keyword_loader import KeywordLoader
import config

logger = logging.getLogger(__name__)

class UsageClassifier:
    """Classifies citations as 'data used' vs 'mere mention'."""
    
    def __init__(self, keyword_loader: KeywordLoader, use_spacy: bool = False, spacy_model_path: Optional[pathlib.Path] = None):
        """
        Initialize the usage classifier.
        
        Args:
            keyword_loader: KeywordLoader instance for regex patterns
            use_spacy: Whether to use spaCy model for classification
            spacy_model_path: Path to trained spaCy model
        """
        self.keyword_loader = keyword_loader
        self.use_spacy = use_spacy
        self.spacy_model_path = spacy_model_path
        
        # Compile regex patterns from keywords.yaml
        self.regex_patterns = self._compile_regex_patterns()
        
        # Load spaCy model if enabled
        self.nlp = None
        if use_spacy and spacy_model_path and spacy_model_path.exists():
            try:
                import spacy
                self.nlp = spacy.load(spacy_model_path)
                logger.info(f"Loaded spaCy model from {spacy_model_path}")
            except Exception as e:
                logger.error(f"Failed to load spaCy model: {e}")
                self.use_spacy = False
    
    def _compile_regex_patterns(self) -> List[re.Pattern]:
        """
        Compile regex patterns from keywords.yaml.
        
        Returns:
            List of compiled regex patterns
        """
        patterns = []
        
        # Get regex patterns from keywords.yaml
        raw_patterns = self.keyword_loader.get_regex_patterns()
        
        # Compile each pattern
        for pattern in raw_patterns:
            try:
                compiled = re.compile(pattern, re.IGNORECASE)
                patterns.append(compiled)
            except re.error as e:
                logger.error(f"Invalid regex pattern '{pattern}': {e}")
        
        # Add URL patterns from keywords.yaml
        for url in self.keyword_loader.get_all_urls():
            # Escape the URL to make it a literal pattern
            escaped_url = re.escape(url)
            try:
                compiled = re.compile(escaped_url, re.IGNORECASE)
                patterns.append(compiled)
            except re.error as e:
                logger.error(f"Failed to compile URL pattern '{url}': {e}")
        
        logger.info(f"Compiled {len(patterns)} regex patterns for usage detection")
        return patterns
    
    def classify_usage(self, text: str) -> Dict[str, Union[int, str, List[str]]]:
        """
        Classify whether the text uses the Compendium data.
        
        Args:
            text: Full text of article
            
        Returns:
            Dict with classification results:
            {
                'uses_compendium': 0 or 1,
                'method': 'regex' or 'spaCy',
                'matched_patterns': list of matched patterns,
                'evidence': text snippets around matches
            }
        """
        if not text:
            return {
                'uses_compendium': 0,
                'method': 'none',
                'matched_patterns': [],
                'evidence': ''
            }
        
        # First try regex classification (faster)
        regex_result = self._classify_with_regex(text)
        
        # If regex found a match or spaCy is disabled, return regex result
        if regex_result['uses_compendium'] == 1 or not self.use_spacy or self.nlp is None:
            return regex_result
        
        # Otherwise try spaCy classification (more accurate)
        return self._classify_with_spacy(text)
    
    def _classify_with_regex(self, text: str) -> Dict[str, Union[int, str, List[str]]]:
        """
        Classify usage with regex patterns.
        
        Args:
            text: Full text of article
            
        Returns:
            Classification result dict
        """
        matched_patterns = []
        evidence = []
        
        for pattern in self.regex_patterns:
            matches = list(pattern.finditer(text))
            if matches:
                matched_patterns.append(pattern.pattern)
                
                # Get context around each match (up to 3 matches per pattern)
                for match in matches[:3]:
                    start = max(0, match.start() - 100)
                    end = min(len(text), match.end() + 100)
                    context = text[start:end]
                    
                    # Highlight the match
                    highlight = text[match.start():match.end()]
                    highlighted_context = context.replace(highlight, f"**{highlight}**")
                    
                    evidence.append(highlighted_context.strip())
        
        # Determine classification (1 = uses data, 0 = mention only)
        uses_compendium = 1 if matched_patterns else 0
        
        return {
            'uses_compendium': uses_compendium,
            'method': 'regex',
            'matched_patterns': matched_patterns,
            'evidence': '\n---\n'.join(evidence)
        }
    
    def _classify_with_spacy(self, text: str) -> Dict[str, Union[int, str, List[str]]]:
        """
        Classify usage with trained spaCy model.
        
        Args:
            text: Full text of article
            
        Returns:
            Classification result dict
        """
        # Process text with spaCy (limit to 100k chars to avoid memory issues)
        doc = self.nlp(text[:100000])
        
        # Get classification scores
        scores = doc.cats
        
        # Determine classification (1 = uses data, 0 = mention only)
        if 'USES_DATA' in scores and scores['USES_DATA'] > 0.6:  # Threshold can be adjusted
            uses_compendium = 1
        else:
            uses_compendium = 0
        
        return {
            'uses_compendium': uses_compendium,
            'method': 'spaCy',
            'matched_patterns': [f"spaCy:USES_DATA:{scores.get('USES_DATA', 0):.2f}"],
            'evidence': f"SpaCy classification scores: {scores}"
        }
    
    def extract_snippet(self, text: str, max_length: int = 160) -> str:
        """
        Extract a representative snippet from text containing evidence of Compendium usage.
        
        Args:
            text: Full text to extract snippet from
            max_length: Maximum snippet length
            
        Returns:
            Extracted snippet
        """
        if not text:
            return ""
        
        # Look for matches of key Compendium terms
        key_terms = [
            "compendium", "AHRQ", "health system", "U.S. Health Systems",
            "Agency for Healthcare Research and Quality"
        ]
        
        for term in key_terms:
            pattern = re.compile(r'\b' + re.escape(term) + r'\b', re.IGNORECASE)
            match = pattern.search(text)
            if match:
                start = max(0, match.start() - max_length // 2)
                end = min(len(text), match.start() + max_length // 2)
                
                # Expand to word boundaries
                while start > 0 and text[start] != ' ' and text[start] != '\n':
                    start -= 1
                while end < len(text) - 1 and text[end] != ' ' and text[end] != '\n':
                    end += 1
                
                return text[start:end].strip()
        
        # Fall back to first N characters if no match found
        return text[:max_length].strip() + "..."
