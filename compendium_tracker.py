#!/usr/bin/env python3
"""
AHRQ Compendium Citation Tracker

This script orchestrates the collection, analysis, and reporting of citations to the
AHRQ Compendium of U.S. Health Systems. It searches multiple academic sources,
classifies each citation based on whether it uses the data or merely mentions it,
and generates reports summarizing the findings.

Usage:
    python compendium_tracker.py [--no-fulltext] [--debug]

Options:
    --no-fulltext    Skip full-text retrieval and classification
    --debug          Enable debug logging
"""

import os
import sys
import argparse
import logging
import time
import pathlib
import concurrent.futures
from datetime import datetime
import pandas as pd
from typing import Dict, List, Any, Tuple, Optional

# Project modules
import config
from utils.keyword_loader import KeywordLoader
from collectors.pubmed_collector import PubMedCollector
from collectors.openalex_collector import OpenAlexCollector
from collectors.citation_collector import CitationCollector
from collectors.scholar_collector import ScholarCollector
from fulltext_analysis.fulltext_fetcher import FulltextFetcher
from fulltext_analysis.usage_classifier import UsageClassifier
from reporting.csv_reporter import CSVReporter
from reporting.html_reporter import HTMLReporter

# Set up logging with a simpler approach
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Console handler with default settings
        logging.FileHandler(os.path.join(config.OUTPUT_DIR, 'compendium_tracker.log'))
    ]
)

# Add module-specific handlers to split logs into separate files
for mod in ("collectors", "fulltext_analysis"):
    h = logging.FileHandler(config.OUTPUT_DIR / f"{mod}.log")
    h.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logging.getLogger(mod).addHandler(h)

# Down-grade noisy loggers to reduce console output
logging.getLogger("fulltext_analysis.fulltext_fetcher").setLevel(logging.WARNING)  # Show only warnings and errors
logging.getLogger("trafilatura").setLevel(logging.WARNING)  # Suppress HTML parsing noise
logging.getLogger("urllib3").setLevel(logging.WARNING)  # Reduce connection noise

logger = logging.getLogger(__name__)

class CompendiumTracker:
    """Main orchestrator for the AHRQ Compendium Citation Tracker."""
    
    def __init__(self, email: str, skip_fulltext: bool = False):
        """
        Initialize the Compendium Tracker.
        
        Args:
            email: Email address for API access
            skip_fulltext: Whether to skip full-text retrieval and classification
        """
        self.email = email
        self.skip_fulltext = skip_fulltext
        
        # Create output directory
        config.OUTPUT_DIR.mkdir(exist_ok=True)
        
        # Load keywords
        self.keyword_loader = KeywordLoader(config.KEYWORDS_PATH)
        
        # Initialize collectors
        self.collectors = {
            'pubmed': PubMedCollector(email, config.MAX_RESULTS_PER_QUERY),
            'openalex': OpenAlexCollector(email, config.MAX_RESULTS_PER_QUERY),
            'citations': CitationCollector(email, config.MAX_RESULTS_PER_QUERY, config.SEED_PMID),
            # 'scholar': ScholarCollector(email, config.SCHOLAR_MAX_RESULTS, config.SCHOLAR_SLEEP_SECONDS)  # Temporarily disabled for faster testing
        }
        
        # Initialize fulltext components if enabled
        if not skip_fulltext:
            self.fulltext_fetcher = FulltextFetcher(email)
            self.usage_classifier = UsageClassifier(
                self.keyword_loader,
                use_spacy=config.SPACY_CLASSIFY,
                spacy_model_path=config.SPACY_MODEL_PATH
            )
        
        # Initialize reporters
        self.csv_reporter = CSVReporter()
        self.html_reporter = HTMLReporter()
    
    def run(self):
        """Run the complete citation tracking pipeline."""
        start_time = time.time()
        logger.info("Starting AHRQ Compendium Citation Tracker")
        
        # Step 1: Collect citations from all sources
        all_citations = self._collect_citations()
        
        # Step 2: Merge and deduplicate results
        merged_df = self._merge_deduplicate(all_citations)
        
        # Step 3: Retrieve and analyze full-text content (if enabled)
        if not self.skip_fulltext:
            merged_df = self._analyze_fulltext(merged_df)
        
        # Step 4: Generate reports
        self._generate_reports(merged_df)
        
        # Log execution time
        elapsed_time = time.time() - start_time
        logger.info(f"AHRQ Compendium Citation Tracker completed in {elapsed_time:.2f} seconds")
    
    # Define routing matrix to control which collectors receive which types of search terms
    SEARCH_ROUTING = {
        "exact_urls": {"pubmed": False, "openalex": False, "scholar": True},
        "pdf_urls": {"pubmed": False, "openalex": False, "scholar": True},
        "phrase_variants": {"pubmed": True, "openalex": True, "scholar": True},
        "year_combos": {"pubmed": True, "openalex": True, "scholar": True},
        "ahrq_combos": {"pubmed": True, "openalex": True, "scholar": True},
        "funding_acknowledgment": {"pubmed": True, "openalex": True, "scholar": True},
    }
    
    # -----------------------------------------------------------
    #  Light-weight "rank & prune" scorers (title / abstract only)
    # -----------------------------------------------------------
    def _score_row(self, row: pd.Series) -> int:
        w = config.RELEVANCE_WEIGHTS
        score = 0
        details = []

        # 1) keyword hit already implied by collection → +1
        score += w["keyword_hit"]
        details.append(f"keyword_hit: +{w['keyword_hit']}")

        text = f"{row.get('title','')} {row.get('abstract','')}".lower()
        
        # 2) context term
        matching_terms = [term for term in self.keyword_loader.get_context_terms() 
                         if term.lower() in text]
        if matching_terms:
            score += w["context_term"]
            details.append(f"context_term: +{w['context_term']} (matches: {', '.join(matching_terms[:3])})")

        # 3) journal whitelist
        journal = str(row.get("journal", "")).strip()
        if journal in self.keyword_loader.get_journal_whitelist():
            score += w["journal_whitelist"]
            details.append(f"journal_whitelist: +{w['journal_whitelist']} ({journal})")

        # 4) negative filters
        matching_filters = [bad for bad in self.keyword_loader.get_negative_filters() 
                           if bad.lower() in text]
        if matching_filters:
            score += w["negative_filter"]
            details.append(f"negative_filter: {w['negative_filter']} (matches: {', '.join(matching_filters[:3])})")

        # Log detailed scoring for debugging
        title_snippet = row.get('title', '')[:50] + '...' if len(row.get('title', '')) > 50 else row.get('title', '')
        logging.debug(f"Score for '{title_snippet}': {score} [{', '.join(details)}]")
        
        return score
    
    def _collect_citations(self) -> Dict[str, pd.DataFrame]:
        """
        Collect citations from all sources.
        
        Returns:
            Dictionary mapping source names to DataFrames with citation data
        """
        logger.info("Collecting citations from all sources")
        all_citations = {}
        
        # Get search terms
        search_terms = self.keyword_loader.get_all_search_terms()
        logger.info(f"Loaded {len(search_terms)} search terms from keywords.yaml")
        
        # Run each collector
        for name, collector in self.collectors.items():
            logger.info(f"Running {name} collector")
            try:
                if name == 'citations':
                    # Citation collector doesn't use search terms
                    df = collector.search()
                else:
                    # Filter search terms based on collector and term category
                    filtered_terms = []
                    for term in search_terms:
                        category = self.keyword_loader.get_category_for_term(term)
                        if self.SEARCH_ROUTING.get(category, {}).get(name, True):
                            filtered_terms.append(term)
                    
                    logger.info(f"Using {len(filtered_terms)} filtered search terms for {name} collector")
                    df = collector.search(filtered_terms)
                
                if df is not None and not df.empty:
                    # Apply relevance scoring and filtering
                    df["stage2_score"] = df.apply(self._score_row, axis=1)
                    original_len = len(df)
                    
                    # Log score distribution for debugging
                    score_counts = df["stage2_score"].value_counts().sort_index()
                    logger.info(f"Score distribution for {name}: {dict(score_counts)}")
                    
                    # Filter based on threshold
                    df = df[df["stage2_score"] >= config.RELEVANCE_THRESHOLD]  # prune here
                    pruned_count = original_len - len(df)
                    
                    # Include titles of low-scoring articles for inspection at debug level
                    if pruned_count > 0 and logger.isEnabledFor(logging.DEBUG):
                        pruned_df = df[df["stage2_score"] < config.RELEVANCE_THRESHOLD]
                        for idx, row in pruned_df.head(min(5, len(pruned_df))).iterrows():
                            title_snippet = row.get('title', '')[:50] + '...' if len(row.get('title', '')) > 50 else row.get('title', '')
                            logger.debug(f"Pruned article: '{title_snippet}' with score {row['stage2_score']}")
                    
                    if not df.empty:
                        all_citations[name] = df
                        logger.info(f"{name} collector found {len(df)} citations after pruning {pruned_count} low-relevance results (threshold: {config.RELEVANCE_THRESHOLD})")
                    else:
                        logger.warning(f"{name} collector: all {original_len} results pruned due to low relevance scores (threshold: {config.RELEVANCE_THRESHOLD})")
                else:
                    logger.warning(f"{name} collector returned no results")
            
            except Exception as e:
                logger.error(f"Error running {name} collector: {e}", exc_info=True)
        
        return all_citations
    
    def _merge_deduplicate(self, all_citations: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Merge and deduplicate citations from all sources.
        
        Args:
            all_citations: Dictionary mapping source names to DataFrames
            
        Returns:
            Merged and deduplicated DataFrame
        """
        logger.info("Merging and deduplicating citations")
        
        # Combine all DataFrames
        dfs = list(all_citations.values())
        if not dfs:
            logger.warning("No citations found from any source")
            return pd.DataFrame()
        
        merged_df = pd.concat(dfs, ignore_index=True)
        logger.info(f"Combined {len(merged_df)} citations from all sources")
        
        # Convert DOIs to lowercase for matching
        if 'doi' in merged_df.columns:
            merged_df['doi'] = merged_df['doi'].str.lower()
        
        # Create normalized title for matching
        if 'title' in merged_df.columns:
            merged_df['title_norm'] = merged_df['title'].str.replace(r'\W+', '', regex=True).str.lower()
        
        # Guarantee a uniform, sortable dtype for year
        if 'year' in merged_df.columns:
            merged_df['year_numeric'] = pd.to_numeric(
                merged_df['year'], errors='coerce', downcast='integer'
            )
            merged_df = merged_df.sort_values('year_numeric', ascending=False)
            merged_df.drop(columns=['year_numeric'], inplace=True)
        
        # Deduplicate: first by DOI, then by normalized title
        before_dedup = len(merged_df)
        
        # Deduplicate by DOI if present
        if 'doi' in merged_df.columns:
            merged_df = merged_df.drop_duplicates(subset=['doi'], keep='first')
        
        # Then deduplicate by normalized title
        if 'title_norm' in merged_df.columns:
            merged_df = merged_df.drop_duplicates(subset=['title_norm'], keep='first')
        
        logger.info(f"Removed {before_dedup - len(merged_df)} duplicate citations")
        logger.info(f"Final dataset contains {len(merged_df)} unique citations")
        
        # Remove the stage2_score column if it exists
        if "stage2_score" in merged_df.columns:
            merged_df.drop(columns=["stage2_score"], inplace=True)
            
        # Save pre-fulltext citations for review
        merged_df.to_csv(
            config.OUTPUT_DIR / "citations_pre_fulltext.csv",
            index=False,
            encoding="utf-8"
        )
        logger.info("Wrote pre-full-text citation list to citations_pre_fulltext.csv")
        
        return merged_df
    
    def _analyze_fulltext(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Retrieve and analyze full-text content.
        
        Args:
            df: DataFrame with citation data
            
        Returns:
            DataFrame with added fulltext analysis columns
        """
        logger.info("Starting full-text retrieval and analysis")
        
        # Initialize columns if they don't exist
        if 'fulltext' not in df.columns:
            df['fulltext'] = ''
        if 'fulltext_source' not in df.columns:
            df['fulltext_source'] = ''
        if 'content_hash' not in df.columns:
            df['content_hash'] = ''
        if 'uses_compendium' not in df.columns:
            df['uses_compendium'] = 0
        if 'classification_method' not in df.columns:
            df['classification_method'] = ''
        if 'evidence' not in df.columns:
            df['evidence'] = ''
        if 'snippet' not in df.columns:
            df['snippet'] = ''
        
        # Retrieve full-text for each citation
        for idx, row in df.iterrows():
            try:
                # Skip if we already have full-text (by content hash)
                if pd.notna(row['content_hash']) and row['content_hash']:
                    logger.info(f"Skipping full-text retrieval for {row.get('doi', 'unknown DOI')} (already processed)")
                    continue
                
                # Get full-text content
                fulltext, source, content_hash = self.fulltext_fetcher.get_fulltext(row)
                
                # Update DataFrame
                df.at[idx, 'fulltext'] = fulltext
                df.at[idx, 'fulltext_source'] = source
                df.at[idx, 'content_hash'] = content_hash
                
                # Skip classification if no full-text found
                if not fulltext:
                    df.at[idx, 'uses_compendium'] = 0
                    df.at[idx, 'classification_method'] = 'none'
                    continue
                
                # Classify usage
                classification = self.usage_classifier.classify_usage(fulltext)
                
                # Update DataFrame with classification results
                df.at[idx, 'uses_compendium'] = classification['uses_compendium']
                df.at[idx, 'classification_method'] = classification['method']
                df.at[idx, 'evidence'] = classification['evidence']
                
                # Extract snippet
                snippet = self.usage_classifier.extract_snippet(fulltext)
                df.at[idx, 'snippet'] = snippet
                
                logger.info(f"Classified {row.get('doi', 'unknown DOI')}: uses_compendium={classification['uses_compendium']}")
                
            except Exception as e:
                logger.error(f"Error analyzing full-text for {row.get('doi', 'unknown DOI')}: {e}")
        
        # Log statistics
        used_count = df['uses_compendium'].sum()
        logger.info(f"Full-text analysis complete: {used_count} citations classified as 'uses_compendium'")
        
        return df
    
    def _generate_reports(self, df: pd.DataFrame):
        """
        Generate reports from citation data.
        
        Args:
            df: DataFrame with citation data
        """
        logger.info("Generating reports")
        
        # Generate CSV reports
        all_hits_path = self.csv_reporter.generate_all_hits_csv(df)
        used_compendium_path = self.csv_reporter.generate_used_compendium_csv(df)
        
        # Generate HTML report
        html_path = self.html_reporter.generate_report(df)
        
        logger.info(f"Reports generated:")
        if all_hits_path:
            logger.info(f"- All citations: {all_hits_path}")
        if used_compendium_path:
            logger.info(f"- Used Compendium: {used_compendium_path}")
        if html_path:
            logger.info(f"- HTML report: {html_path}")

def main():
    """Main entry point for the script."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='AHRQ Compendium Citation Tracker')
    parser.add_argument('--no-fulltext', action='store_true', help='Skip full-text retrieval and classification')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    args = parser.parse_args()
    
    # Set logging level
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Get email from config (loaded from .env)
    email = config.EMAIL
    
    # Validate email
    if email == "your_email@domain.com":
        logger.error("Email address not configured! Please follow these steps:")
        logger.error("1. Create a .env file in the project root (copy from .env.example)")
        logger.error("2. Set your EMAIL=your_email@example.com in the .env file")
        logger.error("3. Run the script again")
        sys.exit(1)  # Exit with error code
    
    # Create and run tracker
    tracker = CompendiumTracker(email, args.no_fulltext)
    tracker.run()

if __name__ == "__main__":
    main()
