"""
Google Scholar collector for AHRQ Compendium Citation Tracker.
Uses scholarly to search for citations in Google Scholar.
"""

import time
import pandas as pd
from typing import List, Dict, Any
import logging
from tqdm import tqdm
import scholarly

from collectors.base_collector import BaseCollector
import config

logger = logging.getLogger(__name__)

class ScholarCollector(BaseCollector):
    """Collector for Google Scholar citations using scholarly."""
    
    def __init__(self, email: str, max_results: int = 100, sleep_seconds: int = 30):
        """
        Initialize the Scholar collector.
        
        Args:
            email: Email address (not used for Scholar but kept for consistency)
            max_results: Maximum results to fetch per query
            sleep_seconds: Time to wait between Scholar requests to avoid blocking
        """
        super().__init__(email, max_results)
        self.sleep_seconds = sleep_seconds
        
    def search(self, terms: List[str]) -> pd.DataFrame:
        """
        Search Google Scholar for citations using the provided terms.
        
        Args:
            terms: List of search terms/keywords
            
        Returns:
            DataFrame with normalized citation data
        """
        all_results = []
        
        for term in terms:
            logger.info(f"Google Scholar query: {term}")
            try:
                # Execute search with rate limiting
                search_query = scholarly.search_pubs(term)
                results = []
                
                with tqdm(total=self.max_results, 
                          desc=f"Scholar: {term[:30]}..." if len(term) > 30 else f"Scholar: {term}") as pbar:
                    for i, result in enumerate(search_query):
                        if i >= self.max_results:
                            break
                        results.append(result)
                        pbar.update(1)
                
                # Log stats
                logger.info(f"Scholar: Found {len(results)} results for term '{term}'")
                
                # Add to all results
                if results:
                    df = self.normalize_data(results)
                    df = self.add_match_term(df, term)
                    all_results.append(df)
                
                # Sleep to avoid rate limiting
                if len(terms) > 1:  # Don't sleep after the last query
                    logger.info(f"Sleeping for {self.sleep_seconds} seconds to avoid Scholar rate limiting...")
                    time.sleep(self.sleep_seconds)
                
            except Exception as e:
                logger.error(f"Error searching Google Scholar for term '{term}': {e}")
        
        # Combine all results
        if all_results:
            return pd.concat(all_results, ignore_index=True)
        else:
            return pd.DataFrame()
        
    def normalize_data(self, raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Normalize raw scholarly data into a standard DataFrame.
        
        Args:
            raw_data: Raw data from scholarly
            
        Returns:
            DataFrame with normalized citation data
        """
        normalized = []
        
        for pub in raw_data:
            try:
                # Extract basic metadata
                title = pub.get('bib', {}).get('title', '')
                
                # Extract authors
                authors = []
                if 'bib' in pub and 'author' in pub['bib']:
                    authors = pub['bib']['author'].split(' and ') if isinstance(pub['bib']['author'], str) else []
                
                # Extract year
                year = None
                if 'bib' in pub and 'pub_year' in pub['bib']:
                    try:
                        year = int(pub['bib']['pub_year'])
                    except (ValueError, TypeError):
                        year = None
                
                # Create normalized entry
                entry = {
                    'title': title,
                    'authors': authors,
                    'author_string': '; '.join(authors) if authors else '',
                    'journal': pub.get('bib', {}).get('venue', ''),
                    'year': year,
                    'doi': None,  # Scholar doesn't reliably provide DOIs
                    'pmid': None,  # Scholar doesn't provide PMIDs
                    'abstract': pub.get('bib', {}).get('abstract', ''),
                    'url': pub.get('pub_url', ''),
                    'source': 'GoogleScholar',
                    'cited_by_count': pub.get('num_citations', 0)
                }
                
                normalized.append(entry)
                
            except Exception as e:
                logger.error(f"Error normalizing Scholar publication: {e}")
                continue
        
        return pd.DataFrame(normalized)
