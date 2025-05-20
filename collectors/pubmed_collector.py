"""
PubMed collector for AHRQ Compendium Citation Tracker.
Uses PyMed to search for citations in PubMed.
"""

import time
import re
import pandas as pd
from typing import List, Dict, Any
import logging
from tqdm import tqdm
from pymed import PubMed

from collectors.base_collector import BaseCollector
import config

logger = logging.getLogger(__name__)

class PubMedCollector(BaseCollector):
    """Collector for PubMed citations using PyMed."""
    
    def __init__(self, email: str, max_results: int = 1000, requests_per_second: int = 3):
        """
        Initialize the PubMed collector.
        
        Args:
            email: Email address for NCBI E-utilities
            max_results: Maximum results to fetch per query
            requests_per_second: Maximum requests per second (NCBI limit)
        """
        super().__init__(email, max_results)
        self.pubmed = PubMed(tool="AHRQCompendiumTracker", email=email)
        self.requests_per_second = requests_per_second
        
    def search(self, terms: List[str]) -> pd.DataFrame:
        """
        Search PubMed for citations using the provided terms.
        
        Args:
            terms: List of search terms/keywords
            
        Returns:
            DataFrame with normalized citation data
        """
        all_results = []
        
        for term in terms:
            logger.info(f"PubMed query: {term}")
            try:
                # Execute search with rate limiting
                results = list(tqdm(
                    self.pubmed.query(term, max_results=self.max_results),
                    desc=f"PubMed: {term[:30]}..." if len(term) > 30 else f"PubMed: {term}"
                ))
                
                # Convert to dict and add match term
                results_dict = [article.toDict() for article in results]
                
                # Log stats
                logger.info(f"PubMed: Found {len(results_dict)} results for term '{term}'")
                
                # Add to all results
                if results_dict:
                    df = self.normalize_data(results_dict)
                    df = self.add_match_term(df, term)
                    all_results.append(df)
                
                # Respect rate limits
                time.sleep(1.0 / self.requests_per_second)
                
            except Exception as e:
                logger.error(f"Error searching PubMed for term '{term}': {e}")
        
        # Combine all results
        if all_results:
            return pd.concat(all_results, ignore_index=True)
        else:
            return pd.DataFrame()
        
    def normalize_data(self, raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Normalize raw PyMed data into a standard DataFrame.
        
        Args:
            raw_data: Raw data from PyMed
            
        Returns:
            DataFrame with normalized citation data
        """
        normalized = []
        
        for article in raw_data:
            try:
                # ---- authors -------------------------------------------------
                authors = []
                for a in article.get("authors", []) or []:
                    if not a:
                        continue
                    lastname  = a.get("lastname") or ""
                    firstname = a.get("firstname") or ""
                    full_name = " ".join(filter(None, [lastname, firstname])).strip()
                    if full_name:
                        authors.append(full_name)

                # ---- year ----------------------------------------------------
                year = None
                pub_date = article.get("publication_date")
                if pub_date:
                    if hasattr(pub_date, "year"):
                        year = pub_date.year
                    elif isinstance(pub_date, str):
                        m = re.match(r"\d{4}", pub_date)
                        if m:
                            year = int(m.group())
                
                # Normalize DOI
                doi = article.get('doi', '').lower() if article.get('doi') else None
                
                # Create normalized entry
                entry = {
                    'title': article.get('title', ''),
                    'authors': authors,
                    'author_string': '; '.join(authors) if authors else '',
                    'journal': article.get('journal', ''),
                    'year': year,
                    'doi': doi,
                    'pmid': article.get('pubmed_id', ''),
                    'abstract': article.get('abstract', ''),
                    'url': f"https://pubmed.ncbi.nlm.nih.gov/{article.get('pubmed_id', '')}/",
                    'source': 'PubMed',
                    'publication_date': article.get('publication_date', None)
                }
                
                normalized.append(entry)
                
            except Exception as e:
                logger.error(f"Error normalizing PubMed article: {e}")
                continue
        
        return pd.DataFrame(normalized)
