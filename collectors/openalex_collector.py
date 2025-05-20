"""
OpenAlex collector for AHRQ Compendium Citation Tracker.
Uses PyAlex to search for citations in OpenAlex.
"""

import time
import pandas as pd
from typing import List, Dict, Any
import logging
from tqdm import tqdm
from pyalex import Works

from collectors.base_collector import BaseCollector
import config

logger = logging.getLogger(__name__)

class OpenAlexCollector(BaseCollector):
    """Collector for OpenAlex citations using PyAlex."""
    
    def __init__(self, email: str, max_results: int = 1000, results_per_page: int = 200):
        """
        Initialize the OpenAlex collector.
        
        Args:
            email: Email address for polite API usage
            max_results: Maximum results to fetch per query
            results_per_page: Results per page (OpenAlex caps at 200)
        """
        super().__init__(email, max_results)
        self.works = Works(email=email)
        self.results_per_page = results_per_page
        
    def search(self, terms: List[str]) -> pd.DataFrame:
        """
        Search OpenAlex for citations using the provided terms.
        
        Args:
            terms: List of search terms/keywords
            
        Returns:
            DataFrame with normalized citation data
        """
        all_results = []
        
        for term in terms:
            logger.info(f"OpenAlex query: {term}")
            try:
                results = []
                
                # Calculate how many pages to request
                pages_to_request = min(
                    self.max_results // self.results_per_page + 1,
                    5  # Cap at 5 pages (1000 results) per term to avoid excessive API usage
                )
                
                # Execute search with pagination
                page = self.works.search(term).per_page(self.results_per_page)
                
                with tqdm(total=min(page.count, self.max_results), 
                          desc=f"OpenAlex: {term[:30]}..." if len(term) > 30 else f"OpenAlex: {term}") as pbar:
                    for i, work in enumerate(page):
                        if i >= self.max_results:
                            break
                        results.append(work)
                        pbar.update(1)
                
                # Log stats
                logger.info(f"OpenAlex: Found {len(results)} results for term '{term}'")
                
                # Add to all results
                if results:
                    df = self.normalize_data(results)
                    df = self.add_match_term(df, term)
                    all_results.append(df)
                
                # Be polite to the API
                time.sleep(1.0)
                
            except Exception as e:
                logger.error(f"Error searching OpenAlex for term '{term}': {e}")
        
        # Combine all results
        if all_results:
            return pd.concat(all_results, ignore_index=True)
        else:
            return pd.DataFrame()
        
    def normalize_data(self, raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Normalize raw OpenAlex data into a standard DataFrame.
        
        Args:
            raw_data: Raw data from OpenAlex
            
        Returns:
            DataFrame with normalized citation data
        """
        normalized = []
        
        for work in raw_data:
            try:
                # Extract basic metadata
                title = work.get('title', '')
                
                # Extract authors
                authors = []
                if 'authorships' in work and work['authorships']:
                    authors = [authorship.get('author', {}).get('display_name', '')
                               for authorship in work['authorships'] if authorship.get('author')]
                
                # Extract year
                year = None
                if 'publication_year' in work:
                    year = work['publication_year']
                
                # Extract OA URL
                oa_url = None
                if 'open_access' in work and work['open_access'] and work['open_access'].get('oa_url'):
                    oa_url = work['open_access'].get('oa_url')
                elif 'primary_location' in work and work['primary_location'] and \
                     'source' in work['primary_location'] and work['primary_location']['source']:
                    oa_url = work['primary_location']['source'].get('fulltext_url')
                
                # Normalize DOI
                doi = work.get('doi', '').lower() if work.get('doi') else None
                
                # Create normalized entry
                entry = {
                    'title': title,
                    'authors': authors,
                    'author_string': '; '.join(authors) if authors else '',
                    'journal': work.get('host_venue', {}).get('display_name', ''),
                    'year': year,
                    'doi': doi,
                    'pmid': None,  # OpenAlex doesn't provide PMIDs directly
                    'abstract': work.get('abstract', ''),
                    'url': work.get('doi', ''),
                    'source': 'OpenAlex',
                    'open_access_url': oa_url,
                    'cited_by_count': work.get('cited_by_count', 0)
                }
                
                normalized.append(entry)
                
            except Exception as e:
                logger.error(f"Error normalizing OpenAlex work: {e}")
                continue
        
        return pd.DataFrame(normalized)
