import pandas as pd
from typing import List, Dict, Any, Union
import logging
import requests
import time
import json
from xml.etree import ElementTree as ET

from collectors.base_collector import BaseCollector
import config

logger = logging.getLogger(__name__)

# Constants for PubMed E-utilities
ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
ELINK_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi"
ESUMMARY_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"

# Cutoff year for citation filtering (Compendium first released ~2017)
CUTOFF_YEAR = 2017

class CitationCollector(BaseCollector):
    """Collector for citation network information using PubMed E-utilities."""
    
    def __init__(self, email: str, max_results: int = 100, seed_pmid: str = config.SEED_PMID):
        """
        Initialize the Citation collector.
        
        Args:
            email: Email address for API access
            max_results: Maximum results to fetch per query
            seed_pmid: The seed PMID to start the citation network from
        """
        super().__init__(email, max_results)
        self.seed_pmid = seed_pmid
        # Headers with user agent for API requests
        self.headers = {"User-Agent": f"AHRQCompendiumBot/1.0 (mailto:{email})"}
    
    def _get_citing_pmids(self, pmid: str) -> List[str]:
        """
        Return PMIDs of papers that cite the seed PMID using PubMed's elink API.
        
        Args:
            pmid: The PMID to find citations for
            
        Returns:
            List of PMIDs citing the given PMID
        """
        # Set up parameters for elink request
        params = {
            "dbfrom": "pubmed",
            "linkname": "pubmed_pubmed_citedin",
            "id": pmid,
            "retmode": "xml",
            "tool": "AHRQCompendiumTracker",
            "email": self.email
        }
        
        try:
            # Make request to elink API
            r = requests.get(ELINK_URL, params=params, headers=self.headers, timeout=20)
            r.raise_for_status()
            
            # Parse XML response to extract PMIDs
            root = ET.fromstring(r.text)
            citing_pmids = [idtag.text for idtag in root.findall(".//LinkSetDb/Link/Id")]
            
            logger.info(f"Retrieved {len(citing_pmids)} citing PMIDs from PubMed")
            return citing_pmids
            
        except Exception as e:
            logger.error(f"Error retrieving citing PMIDs: {e}")
            return []
    
    def _get_summaries(self, pmids: List[str]) -> pd.DataFrame:
        """
        Get detailed information about PMIDs using PubMed's esummary API.
        
        Args:
            pmids: List of PMIDs to fetch details for
            
        Returns:
            DataFrame with publication details
        """
        if not pmids:
            return pd.DataFrame()
            
        try:
            # Set up parameters for esummary request
            params = {
                "db": "pubmed",
                "id": ",".join(pmids),
                "retmode": "json",
                "tool": "AHRQCompendiumTracker",
                "email": self.email
            }
            
            # Make request to esummary API
            r = requests.get(ESUMMARY_URL, params=params, headers=self.headers, timeout=30)
            r.raise_for_status()
            
            # Parse JSON response
            records = json.loads(r.text)["result"]
            rows = []
            
            # Extract relevant fields for each PMID
            for p in pmids:
                if p not in records or p == "uids":
                    continue
                    
                rec = records.get(p, {})
                # Extract DOI from articleids list if available
                doi = ""
                for id_obj in rec.get("articleids", []):
                    if id_obj.get("idtype") == "doi":
                        doi = id_obj.get("value", "")
                        break
                        
                # Extract and clean year to make it an integer right from the start
                year_str = rec.get("pubdate", "")[:4] if rec.get("pubdate") else ""
                try:
                    year = int(year_str) if year_str.isdigit() else None
                except (ValueError, TypeError):
                    year = None
                    
                rows.append({
                    "pmid": p,
                    "title": rec.get("title", ""),
                    "journal": rec.get("fulljournalname", ""),
                    "year": year,  # Now an integer or None
                    "doi": doi,
                    "cited_by_count": rec.get("pmcrefcount", 0)
                })
            
            return pd.DataFrame(rows)
            
        except Exception as e:
            logger.error(f"Error retrieving publication summaries: {e}")
            return pd.DataFrame()
    
    def search(self, terms=None) -> pd.DataFrame:
        """
        Retrieves citations to the seed PMID using PubMed E-utilities.
        
        Args:
            terms: Not used in this collector, present for API compatibility
            
        Returns:
            DataFrame with citation information
        """
        logger.info(f"Retrieving citations to PMID {self.seed_pmid}")
        
        try:
            # Get PMIDs that cite the seed PMID
            citing_pmids = self._get_citing_pmids(self.seed_pmid)
            
            # Get detailed information for citing PMIDs
            df = self._get_summaries(citing_pmids)
            
            if df.empty:
                logger.warning("No citing articles found")
                return df
                
            # Filter by cutoff year to exclude obviously impossible hits
            # (Compendium was first released around 2017)
            df['year_numeric'] = pd.to_numeric(df["year"], errors="coerce")
            filtered_df = df[df['year_numeric'] >= CUTOFF_YEAR].copy()
            filtered_df.drop('year_numeric', axis=1, inplace=True)
            
            logger.info(f"After year-filter: {len(filtered_df)} (removed {len(df) - len(filtered_df)} pre-{CUTOFF_YEAR} records)")
            
            # Add additional columns
            filtered_df["url"] = filtered_df["pmid"].apply(lambda x: f"https://pubmed.ncbi.nlm.nih.gov/{x}/")
            filtered_df["source"] = "PubMed_CitedIn"
            filtered_df["match_term"] = f"Cited {self.seed_pmid}"
            
            return filtered_df
            
        except Exception as e:
            logger.error(f"Citation collector failed: {e}", exc_info=True)
            return pd.DataFrame()
    
    # ------------------------------------------------------------------
    # required by BaseCollector (abstract method)
    # ------------------------------------------------------------------
    def normalize_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """The dataframe coming from `search()` is already normalized."""
        return df