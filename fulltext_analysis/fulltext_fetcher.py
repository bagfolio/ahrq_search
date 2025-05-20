"""
Full-text fetcher for AHRQ Compendium Citation Tracker.
Retrieves full-text content from open access sources.
"""

import time
import hashlib
import pathlib
import logging
import pandas as pd
from typing import Dict, Optional, Tuple
import requests
import trafilatura
from requests.exceptions import RequestException
import unpywall

from utils.keyword_loader import KeywordLoader
import config

logger = logging.getLogger(__name__)

class FulltextFetcher:
    """Fetches and caches full-text content from various sources."""
    
    def __init__(self, email: str, cache_dir: pathlib.Path = config.CACHE_DIR):
        """
        Initialize the full-text fetcher.
        
        Args:
            email: Email address for Unpaywall API
            cache_dir: Directory to cache downloaded content
        """
        self.email = email
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(exist_ok=True)
        self.unpaywall = unpywall.Unpywall(email=email)
        
        # User agent for polite scraping
        self.headers = {
            'User-Agent': 'AHRQCompendiumTracker/1.0 (Health Research; ' + 
                          f'{email}) trafilatura/1.6',
            'Accept': 'text/html,application/xhtml+xml,application/xml',
        }
    
    def get_fulltext(self, row: pd.Series) -> Tuple[str, str, str]:
        """
        Get full-text content for a citation.
        
        Args:
            row: DataFrame row with citation metadata
            
        Returns:
            Tuple of (full_text, url_used, content_hash)
        """
        # Check for cached content first
        if pd.notna(row.get('doi')):
            cache_path = self._get_cache_path(row['doi'])
            if cache_path.exists():
                logger.info(f"Using cached content for DOI: {row['doi']}")
                content = cache_path.read_text(encoding='utf-8')
                return content, "cache", self._hash_content(content)
        
        # Try different URL sources in order of preference
        url = None
        
        # 1. Try OpenAlex OA URL if available
        if pd.notna(row.get('open_access_url')):
            url = row['open_access_url']
            source = "open_access_url"
        
        # 2. Try Unpaywall if DOI is available
        elif pd.notna(row.get('doi')):
            try:
                record = self.unpaywall.record(row['doi'])
                if record and record.best_oa_location:
                    url = record.best_oa_location.url_for_pdf or record.best_oa_location.url
                    source = "unpaywall"
            except Exception as e:
                logger.warning(f"Error retrieving Unpaywall data for DOI {row['doi']}: {e}")
        
        # 3. Use article URL as fallback
        if url is None and pd.notna(row.get('url')):
            url = row['url']
            source = "article_url"
        
        # Fetch and process content if URL is available
        if url:
            try:
                logger.info(f"Fetching content from {url}")
                
                # Use trafilatura to extract main content
                downloaded = trafilatura.fetch_url(
                    url, 
                    headers=self.headers,
                    decode=True,
                    include_comments=False
                )
                
                if downloaded:
                    # Extract main content and remove boilerplate
                    content = trafilatura.extract(
                        downloaded,
                        include_comments=False,
                        include_tables=True,
                        include_links=True,
                        include_images=False,
                        output_format='text'
                    )
                    
                    if content:
                        # Cache content if DOI is available
                        if pd.notna(row.get('doi')):
                            cache_path = self._get_cache_path(row['doi'])
                            cache_path.write_text(content, encoding='utf-8')
                        
                        # Generate hash
                        content_hash = self._hash_content(content)
                        
                        return content, source, content_hash
            
            except Exception as e:
                logger.error(f"Error fetching content from {url}: {e}")
        
        # Return empty values if full-text retrieval failed
        return "", "", ""
    
    def _get_cache_path(self, doi: str) -> pathlib.Path:
        """
        Get cache path for a DOI.
        
        Args:
            doi: DOI to cache
            
        Returns:
            Path to cache file
        """
        # Normalize DOI and create a safe filename
        doi_norm = doi.strip().lower()
        filename = hashlib.md5(doi_norm.encode()).hexdigest() + ".txt"
        return self.cache_dir / filename
    
    def _hash_content(self, content: str) -> str:
        """
        Hash content for deduplication.
        
        Args:
            content: Text content to hash
            
        Returns:
            SHA-256 hash of content
        """
        return hashlib.sha256(content.encode('utf-8')).hexdigest()
