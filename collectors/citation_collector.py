"""
NIH Open Citation Collection (OCC) collector for AHRQ Compendium Citation Tracker.
Uses pmidcite to retrieve citation networks around seed PMIDs.
"""

import pandas as pd
from typing import List, Dict, Any, Union
import logging
from pmidcite import icite

from collectors.base_collector import BaseCollector
import config

logger = logging.getLogger(__name__)

class CitationCollector(BaseCollector):
    """Collector for citation networks using pmidcite."""
    
    def __init__(self, email: str, seed_pmid: str = config.SEED_PMID):
        """
        Initialize the Citation collector.
        
        Args:
            email: Email address for API access
            seed_pmid: Seed PMID for citation network traversal
        """
        super().__init__(email)
        self.seed_pmid = seed_pmid
        
    def search(self, terms: List[str] = None) -> pd.DataFrame:
        """
        Search for citations using the NIH Open Citation Collection.
        Note: terms parameter is ignored as this collector uses the seed PMID.
        
        Args:
            terms: Ignored for this collector
            
        Returns:
            DataFrame with normalized citation data
        """
        logger.info(f"Citation traversal for PMID {self.seed_pmid}")
        
        try:
            # Get citations to the seed PMID
            citing_pmids = icite(self.seed_pmid)
            
            # Convert to DataFrame
            if citing_pmids:
                df = icite.to_df(citing_pmids)
                logger.info(f"Found {len(df)} citations to PMID {self.seed_pmid}")
                
                # Add source and seed information
                df["source"] = "NIH_OCC"
                df["seed_pmid"] = self.seed_pmid
                
                # Normalize the data
                return self.normalize_data(df)
            else:
                logger.warning(f"No citations found for PMID {self.seed_pmid}")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error retrieving citations for PMID {self.seed_pmid}: {e}")
            return pd.DataFrame()
        
    def normalize_data(self, raw_data: Union[pd.DataFrame, List[Dict[str, Any]]]) -> pd.DataFrame:
        """
        Normalize raw pmidcite data into a standard DataFrame.
        
        Args:
            raw_data: Raw DataFrame from pmidcite
            
        Returns:
            DataFrame with normalized citation data
        """
        # If input is already a DataFrame, use it directly
        if isinstance(raw_data, pd.DataFrame):
            df = raw_data
        else:
            # Convert list of dicts to DataFrame
            df = pd.DataFrame(raw_data)
        
        # Map columns to our standard schema
        normalized = pd.DataFrame()
        
        try:
            # Standard columns pmidcite provides
            normalized['pmid'] = df['pmid'].astype(str)
            normalized['title'] = df['title']
            normalized['journal'] = df['journal']
            normalized['year'] = df['year']
            
            # Add default or empty values for columns not provided by pmidcite
            normalized['doi'] = df.get('doi', pd.NA)
            normalized['abstract'] = ''  # pmidcite doesn't provide abstracts
            normalized['authors'] = [[] for _ in range(len(df))]  # Empty list of authors
            normalized['author_string'] = ''
            normalized['url'] = normalized['pmid'].apply(lambda x: f"https://pubmed.ncbi.nlm.nih.gov/{x}/")
            normalized['source'] = 'NIH_OCC'
            normalized['match_term'] = f"Citation to PMID:{self.seed_pmid}"
            
            # Include citation metrics if available
            if 'cited_by' in df.columns:
                normalized['cited_by_count'] = df['cited_by']
            else:
                normalized['cited_by_count'] = 0
            
            return normalized
        
        except Exception as e:
            logger.error(f"Error normalizing pmidcite data: {e}")
            return pd.DataFrame()
