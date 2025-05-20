"""
Full-text fetcher for AHRQ Compendium Citation Tracker.
Retrieves full-text content from open access sources.
"""

import time
import hashlib
import pathlib
import logging
import os
import pandas as pd
from typing import Dict, Optional, Tuple, Union
import requests
import trafilatura
from requests.exceptions import RequestException
import unpywall
from hashlib import sha1

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
        
        # Set the Unpaywall email via environment variable (proper authentication method)
        os.environ["UNPAYWALL_EMAIL"] = self.email
        
        # Initialize unpywall with no arguments
        self.unpywall = unpywall.Unpywall()
        
        # Initialize cache with no arguments (uses default location)
        self.unpywall.init_cache()
        
        # User agent for polite scraping (browser-like UA to avoid 403 errors)
        self.user_agent = f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AHRQCompendiumBot/1.0 (Health Research; {email})"
        
        # Headers with browser-like useragent to reduce blocks
        self.headers = {
            'User-Agent': self.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml',
        }
        
    def _download_url(self, url: str) -> Optional[bytes]:
        """
        Download content from a URL with retry logic for 403 errors.
        
        Args:
            url: URL to download from
            
        Returns:
            Raw content bytes or None if download failed
        """
        for attempt in (1, 2):  # one retry with back-off
            try:
                r = requests.get(
                    url,
                    headers={"User-Agent": self.user_agent},
                    timeout=25,
                    allow_redirects=True,
                    cookies={} if attempt == 1 else None  # Allow cookies on second attempt
                )
                
                # If we get a 403 on first attempt, retry with cookies
                if r.status_code == 403 and attempt == 1:
                    logger.debug(f"Received 403 from {url}, retrying with cookies...")
                    time.sleep(5)
                    continue
                    
                if r.ok:
                    return r.content
                else:
                    logger.debug(f"Failed to download {url}: HTTP {r.status_code}")
                    return None  # Give up on 403/404 etc.
            except Exception as e:
                logger.warning(f"Error downloading {url}: {e}")
                return None
    
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
                doi = row['doi']
                
                # Try to get PDF and document links using the instance methods
                result = self.unpywall.query(doi)
                
                # First check if we got a result at all
                if result is None:
                    logger.info(f"No Unpaywall record for {doi}")
                # Then check if the result is valid and has content
                elif not result.empty:
                    # Try to extract PDF URL and OA URL from the result
                    pdf_url = None
                    oa_url = None
                    
                    # Look for PDF URL in common column names
                    for col in ['pdf_url', 'best_oa_location.pdf_url']:
                        try:
                            if col in result.columns and pd.notna(result.iloc[0][col]):
                                pdf_url = result.iloc[0][col]
                                break
                        except (KeyError, AttributeError):
                            pass
                    
                    # Look for OA URL in common column names
                    for col in ['url', 'best_oa_url', 'best_oa_location.url']:
                        try:
                            if col in result.columns and pd.notna(result.iloc[0][col]):
                                oa_url = result.iloc[0][col]
                                break
                        except (KeyError, AttributeError):
                            pass
                    
                    # Use PDF URL if available, otherwise use OA URL
                    if pdf_url:
                        url = pdf_url
                        source = "unpaywall_pdf"
                    elif oa_url:
                        url = oa_url
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
                # Skip PubMed landing pages - they don't have useful full text
                if "pubmed.ncbi.nlm.nih.gov" in url:
                    logger.debug(f"Skipping PubMed landing page – no full text: {url}")
                    return "", "pubmed_stub", ""
                    
                logger.info(f"Fetching content from {url}")
                
                # Use our improved download method with retry logic
                blob = self._download_url(url)
                
                if not blob:
                    logger.warning(f"Failed to download content from {url}")
                    return "", "unreachable", None
                
                # Check if this is a PDF file (look for PDF header magic bytes)
                if b"%PDF" in blob[:1024]:
                    logger.info(f"Detected PDF content from {url}")
                    
                    # Generate a hash for the content
                    content_hash = sha1(blob).hexdigest()
                    
                    # For now just return an empty string - in a real implementation you would
                    # use something like pdfminer to extract text
                    # TODO: Implement _extract_pdf_text helper method
                    return "", "pdf", content_hash
                
                # Process as HTML
                try:
                    # Safely decode HTML content
                    html = blob.decode("utf-8", "ignore")
                    
                    # Extract main content and remove boilerplate
                    content = trafilatura.extract(
                        html,
                        include_comments=False,
                        include_tables=True,
                        include_links=True,
                        include_images=False
                    )
                    
                    if content:
                        # Cache the content if DOI is available
                        if pd.notna(row.get('doi')):
                            cache_path = self._get_cache_path(row['doi'])
                            cache_path.write_text(content, encoding='utf-8')
                        
                        content_hash = sha1(content.encode()).hexdigest()
                        return content, source, content_hash
                    else:
                        logger.warning(f"Failed to extract content from {url}")
                except Exception as e:
                    logger.warning(f"Error processing HTML from {url}: {e}")
                        
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
