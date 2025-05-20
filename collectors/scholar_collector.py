"""
Google Scholar collector for AHRQ Compendium Citation Tracker.
Uses scholarly to search for citations in Google Scholar.
"""

import time
import pandas as pd
from typing import List, Dict, Any
import logging
import functools
from random import uniform
from urllib.parse import quote_plus
from tqdm import tqdm
from scholarly import scholarly

from collectors.base_collector import BaseCollector
import config

# Conditionally import Selenium if enabled in config
if config.SCHOLAR_USE_SELENIUM:
    from selenium import webdriver
    from selenium.webdriver.firefox.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from bs4 import BeautifulSoup

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
        self.driver = None  # Will be initialized if using Selenium
        
    def _selenium_scrape(self, term: str) -> List[Dict[str, Any]]:
        """
        Scrape Google Scholar using Selenium to avoid captchas.
        
        Args:
            term: Search term to query
            
        Returns:
            List of publication data dictionaries
        """
        results = []
        
        # Initialize browser if not already done
        if self.driver is None:
            logger.info("Initializing headless Firefox for Scholar scraping")
            options = Options()
            options.headless = True
            self.driver = webdriver.Firefox(options=options)
        
        # Navigate to Google Scholar search results
        url = f"https://scholar.google.com/scholar?q={quote_plus(term)}"
        logger.info(f"Navigating to Google Scholar: {url}")
        
        try:
            self.driver.get(url)
            time.sleep(uniform(1, 3))  # Random delay
            
            # Process up to SCHOLAR_MAX_PAGES pages
            for page_num in range(config.SCHOLAR_MAX_PAGES):
                # Parse current page
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                result_blocks = soup.select('.gs_r.gs_or.gs_scl')
                
                logger.info(f"Found {len(result_blocks)} results on page {page_num+1}")
                
                # Process each result
                for block in result_blocks:
                    try:
                        # Extract title and URL
                        title_elem = block.select_one('.gs_rt a')
                        title = title_elem.text if title_elem else "Unknown Title"
                        url = title_elem['href'] if title_elem and 'href' in title_elem.attrs else ""
                        
                        # Extract authors, journal, year
                        meta_elem = block.select_one('.gs_a')
                        meta_text = meta_elem.text if meta_elem else ""
                        
                        # Authors typically come before the first dash
                        authors = meta_text.split('-')[0].strip() if '-' in meta_text else ""
                        
                        # Journal typically is between the first and second dash
                        journal = meta_text.split('-')[1].strip() if len(meta_text.split('-')) > 1 else ""
                        
                        # Year is often in the last part, extract digits
                        year = ""
                        for part in meta_text.split('-'):
                            # Look for 4-digit year
                            for word in part.split():
                                if word.isdigit() and len(word) == 4:
                                    year = word
                                    break
                        
                        # Extract abstract if available
                        abstract_elem = block.select_one('.gs_rs')
                        abstract = abstract_elem.text if abstract_elem else ""
                        
                        # Extract any available PDF link
                        pdf_elem = block.select_one('a:has(span:contains("[PDF]"))')
                        pdf_url = pdf_elem['href'] if pdf_elem and 'href' in pdf_elem.attrs else ""
                        
                        # Create a result entry
                        pub = {
                            'title': title,
                            'authors': authors,
                            'journal': journal,
                            'year': year,
                            'url': url,
                            'abstract': abstract,
                            'pdf_url': pdf_url,
                        }
                        
                        results.append(pub)
                        
                        # Limit results if needed
                        if len(results) >= self.max_results:
                            return results
                            
                    except Exception as e:
                        logger.warning(f"Error extracting Scholar result: {e}")
                
                # Check if there's a next page and navigate if possible
                if page_num < config.SCHOLAR_MAX_PAGES - 1:
                    next_link = self.driver.find_elements(By.LINK_TEXT, "Next")
                    if next_link and len(next_link) > 0:
                        logger.info("Navigating to next Scholar results page")
                        next_link[0].click()
                        time.sleep(uniform(3, 6))  # Random delay between page loads
                    else:
                        logger.info("No more Scholar result pages available")
                        break
                        
            return results
            
        except Exception as e:
            logger.error(f"Error during Scholar Selenium scraping: {e}")
            return results
    
    def search(self, terms: List[str]) -> pd.DataFrame:
        """
        Search Google Scholar for citations using the provided terms.
        
        Args:
            terms: List of search terms/keywords
            
        Returns:
            DataFrame with normalized citation data
        """
        # Reduce log noise from selenium
        logging.getLogger("selenium").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        
        if not config.SCHOLAR_USE_SELENIUM:
            # Configure scholarly to minimize captcha issues
            scholarly.use_proxy(None)
            scholarly.set_citation_source("pubs")
            scholarly.set_timeout(10)
        
        all_results = []
        
        for term in terms:
            logger.info(f"Google Scholar query: {term}")
            results = []
            
            try:
                # Choose scraping method based on config
                if config.SCHOLAR_USE_SELENIUM:
                    # Use Selenium for scraping
                    results = self._selenium_scrape(term)
                    logger.info(f"Scholar (selenium): Found {len(results)} results for term '{term}'")
                else:
                    # Use scholarly API
                    search_query = scholarly.search_pubs(term)
                    # Set a safe maximum items to fetch to avoid hanging
                    max_to_fetch = min(self.max_results, 20)  # Reduced to avoid triggering captchas
                    
                    with tqdm(total=max_to_fetch, 
                            desc=f"Scholar: {term[:30]}..." if len(term) > 30 else f"Scholar: {term}") as pbar:
                        for i, result in enumerate(search_query):
                            if i >= max_to_fetch:
                                break
                            results.append(result)
                            pbar.update(1)
                    
                    logger.info(f"Scholar (scholarly): Found {len(results)} results for term '{term}'")
                
                # Add to all results
                if results:
                    df = self.normalize_data(results)
                    df = self.add_match_term(df, term)
                    all_results.append(df)
                
                # Sleep with random duration to avoid rate limiting
                if len(terms) > 1:  # Don't sleep after the last query
                    sleep_sec = uniform(*config.SCHOLAR_SLEEP_BETWEEN_QUERIES)
                    logger.info(f"Sleeping {sleep_sec:.1f}s to respect Scholar rate-limits")
                    time.sleep(sleep_sec)
            
            except Exception as e:
                error_str = str(e).lower()
                if "captcha" in error_str or "robot" in error_str or "try again later" in error_str:
                    logger.warning(f"Google Scholar captcha detected for '{term}'. Skipping to avoid hanging.")
                else:
                    logger.error(f"Error searching Google Scholar for term '{term}': {e}")
        
        # Cleanup Selenium driver if it was used
        if getattr(self, 'driver', None):
            logger.info("Closing Selenium browser")
            try:
                self.driver.quit()
            except Exception as e:
                logger.error(f"Error closing Selenium browser: {e}")
            self.driver = None
        
        # Combine all results
        if all_results:
            return pd.concat(all_results, ignore_index=True)
        else:
            return pd.DataFrame()
    
    def __del__(self):
        """Clean up resources when the collector is destroyed."""
        if getattr(self, 'driver', None):
            try:
                self.driver.quit()
            except:
                pass
        
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
