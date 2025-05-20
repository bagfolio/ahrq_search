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
from datetime import datetime
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
    def _score_row(self, row: pd.Series) -> float:
        """Score a citation row using the enhanced multi-signal scoring system.
        
        Args:
            row: DataFrame row containing citation data
            
        Returns:
            float: The composite score, higher values indicate more relevance
        """
        w = config.RELEVANCE_WEIGHTS
        score = 0.0
        details = []
        flags = {}
        
        # Get text fields for scoring
        title = str(row.get('title', '')).strip()
        abstract = str(row.get('abstract', '')).strip()
        authors = str(row.get('author_string', '')).strip()
        journal = str(row.get('journal', '')).strip()
        year = row.get('year')
        pmid = str(row.get('pmid', '')).strip()
        
        # Combined text for term matching
        text = f"{title} {abstract}".lower()

        # ---------- POSITIVE SIGNALS ----------
        
        # Baseline: we fetched it
        score += w["keyword_hit"]
        details.append(f"keyword_hit: +{w['keyword_hit']}")
        flags["keyword_hit"] = True
        
        # A: Canonical Compendium mentions
        matching_canonical = [term for term in self.keyword_loader.get_canonical_terms() 
                             if term.lower() in text]
        if matching_canonical:
            score += w["canonical_term"]
            details.append(f"canonical_term: +{w['canonical_term']} (matches: {', '.join(matching_canonical[:3])})")
            flags["canonical_term"] = True
            
        # B: Dataset author seeds
        seed_pmid = "30674227"  # Primary seed PMID
        seed_terms = self.keyword_loader.get_dataset_author_seeds()
        
        # Split into PMIDs and author names
        seed_pmids = [str(item) for item in seed_terms if isinstance(item, int) or (isinstance(item, str) and item.isdigit())]
        seed_authors = [item for item in seed_terms if isinstance(item, str) and not item.isdigit()]
        
        # Check for seed PMID in citation references
        has_seed_cite = any(p in pmid for p in seed_pmids)
        
        # Check for known authors
        author_matches = [author for author in seed_authors 
                         if isinstance(author, str) and author.lower() in authors.lower()]
        
        if has_seed_cite or author_matches:
            score += w["dataset_author_seed"]
            if has_seed_cite:
                details.append(f"dataset_author_seed: +{w['dataset_author_seed']} (cites seed PMID)")
            else:
                details.append(f"dataset_author_seed: +{w['dataset_author_seed']} (author: {', '.join(author_matches[:3])})")
            flags["dataset_author_seed"] = True
            
        # C: Integration terms
        matching_integration = [term for term in self.keyword_loader.get_integration_terms() 
                              if term.lower() in text]
        if matching_integration:
            score += w["integration_term"]
            details.append(f"integration_term: +{w['integration_term']} (matches: {', '.join(matching_integration[:3])})")
            flags["integration_term"] = True
            
        # D: Scope terms
        matching_scope = [term for term in self.keyword_loader.get_scope_terms() 
                        if term.lower() in text]
        if matching_scope:
            score += w["scope_term"]
            details.append(f"scope_term: +{w['scope_term']} (matches: {', '.join(matching_scope[:3])})")
            flags["scope_term"] = True
            
        # E: Journal whitelist
        if journal in self.keyword_loader.get_journal_whitelist():
            score += w["journal_whitelist"]
            details.append(f"journal_whitelist: +{w['journal_whitelist']} ({journal})")
            flags["journal_whitelist"] = True
            
        # ---------- NEGATIVE SIGNALS ----------
            
        # F: Negative geography
        matching_geography = [term for term in self.keyword_loader.get_neg_geography() 
                            if term.lower() in text]
        if matching_geography:
            score += w["neg_geography"]
            details.append(f"neg_geography: {w['neg_geography']} (matches: {', '.join(matching_geography[:3])})")
            flags["neg_geography"] = True
            
        # G: Negative domain
        matching_domain = [term for term in self.keyword_loader.get_neg_domain() 
                          if term.lower() in text]
        if matching_domain:
            score += w["neg_domain"]
            details.append(f"neg_domain: {w['neg_domain']} (matches: {', '.join(matching_domain[:3])})")
            flags["neg_domain"] = True
            
        # ---------- MINOR HEURISTICS ----------
            
        # Short title penalty (often editorials)
        if len(title.split()) < 5:
            score += w["short_title"]
            details.append(f"short_title: {w['short_title']} (title has fewer than 5 words)")
            flags["short_title"] = True
            
        # Old paper penalty (Compendium launched 2016)
        try:
            if year and int(year) < 2008:
                score += w["old_paper"]
                details.append(f"old_paper: {w['old_paper']} (year: {year})")
                flags["old_paper"] = True
        except (ValueError, TypeError):
            # Skip if year can't be converted to int
            pass
            
        # Add flags to row for auditing
        row["score_flags"] = ",".join([flag for flag, value in flags.items() if value])

        # Format title for logging
        title_snippet = title[:50] + '...' if len(title) > 50 else title
        logging.debug(f"Score for '{title_snippet}': {score:.1f} [{', '.join(details)}]")
        
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
                    # Apply enhanced relevance scoring and filtering
                    df["stage2_score"] = df.apply(self._score_row, axis=1)
                    original_len = len(df)
                    
                    # Log score distribution for debugging
                    score_counts = df["stage2_score"].value_counts().sort_index()
                    logger.info(f"Score distribution for {name}: {dict(score_counts)}")
                    
                    # Log signal flag distribution
                    if logger.isEnabledFor(logging.INFO):
                        # Count occurrences of each signal flag
                        flag_counts = {}
                        for _, row in df.iterrows():
                            if pd.notna(row.get('score_flags')):
                                flags = str(row.get('score_flags')).split(',')
                                for flag in flags:
                                    flag_counts[flag] = flag_counts.get(flag, 0) + 1
                        
                        # Group by signal class for more readable output
                        positive_flags = {k: v for k, v in flag_counts.items() 
                                       if k in ['canonical_term', 'dataset_author_seed', 'integration_term', 
                                                'scope_term', 'journal_whitelist']}
                        negative_flags = {k: v for k, v in flag_counts.items() 
                                        if k in ['neg_geography', 'neg_domain', 'short_title', 'old_paper']}
                        
                        logger.info(f"Positive signal distribution: {positive_flags}")
                        logger.info(f"Negative signal distribution: {negative_flags}")
                    
                    # Filter based on threshold (keep anything with score >= 0)
                    df = df[df["stage2_score"] >= config.RELEVANCE_THRESHOLD]  # prune here
                    pruned_count = original_len - len(df)
                    pruned_pct = (pruned_count / original_len * 100) if original_len > 0 else 0
                    
                    # Include sample of pruned articles in log for inspection
                    if pruned_count > 0 and logger.isEnabledFor(logging.DEBUG):
                        # Get the low-scoring dataframe
                        low_scores_df = df[~df.index.isin(df[df["stage2_score"] >= config.RELEVANCE_THRESHOLD].index)]
                        
                        # Sort by score (ascending) and take first few examples
                        for _, row in low_scores_df.sort_values("stage2_score").head(min(5, len(low_scores_df))).iterrows():
                            title = row.get('title', '')
                            title_snippet = title[:80] + '...' if len(title) > 80 else title
                            score = row.get('stage2_score')
                            flags = row.get('score_flags', '')
                            logger.debug(f"Pruned: '{title_snippet}' score={score:.1f} flags={flags}")
                    
                    if not df.empty:
                        all_citations[name] = df
                        logger.info(f"{name} collector found {len(df)} citations ({original_len-pruned_count}/{original_len} = {100-pruned_pct:.1f}%) after pruning (threshold: {config.RELEVANCE_THRESHOLD})")
                    else:
                        logger.warning(f"{name} collector: all {original_len} results pruned due to low relevance scores (threshold: {config.RELEVANCE_THRESHOLD})")
                        # If everything was pruned, log top signals that caused rejection
                        if original_len > 0 and logger.isEnabledFor(logging.INFO):
                            neg_signals = sorted([(k, v) for k, v in negative_flags.items()], key=lambda x: x[1], reverse=True)
                            if neg_signals:
                                logger.info(f"Top rejection signals: {neg_signals[:3]}")
                    
                    # Store score_flags for CSV output
                    if not df.empty:
                        df["score_flags"] = df["score_flags"].fillna('')
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
        
        # Keep the stage2_score and score_flags columns for CSV output and debugging
        # Note: We now keep these columns to provide transparency about the scoring process
            
        # Save pre-fulltext citations for review
        try:
            output_file = os.path.join(config.OUTPUT_DIR, "citations_pre_fulltext.csv")
            merged_df.to_csv(output_file, index=False)
            logger.info(f"Saved pre-fulltext citations to {output_file}")
        except Exception as e:
            logger.error(f"Error saving pre-fulltext citations: {e}")
            # Try with minimal columns if too many columns cause issues
            try:
                essential_cols = ['title', 'authors', 'journal', 'year', 'doi', 'pmid', 'url', 'source', 'stage2_score', 'score_flags']
                cols_to_use = [c for c in essential_cols if c in merged_df.columns]
                merged_df[cols_to_use].to_csv(os.path.join(config.OUTPUT_DIR, "citations_pre_fulltext_minimal.csv"), index=False)
                logger.info(f"Saved minimal version of pre-fulltext citations")
            except Exception as e2:
                logger.error(f"Error saving minimal citations: {e2}")
                pass
        
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
    parser.add_argument("--debug", action="store_true", help="Enable debug output")
    parser.add_argument("--no-fulltext", action="store_true", help="Skip fulltext retrieval")
    parser.add_argument("--headless", action="store_true", help="Run in headless mode")
    parser.add_argument("--headful", action="store_true", help="Run in headful mode")
    parser.add_argument("--quiet", action="store_true", help="Reduce console output")
    parser.add_argument("--logfile", action="store_true", help="Write output to log file")
    args = parser.parse_args()
    
    # Configure simpler logging
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)
        
    # Handle quiet mode by disabling console output
    if args.quiet:
        for handler in logging.getLogger().handlers:
            if isinstance(handler, logging.StreamHandler):
                handler.setLevel(logging.WARNING)
    
    # Create log file if requested
    if args.logfile:
        try:
            os.makedirs('logs', exist_ok=True)
            log_filename = f"logs/compendium_tracker_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            file_handler = logging.FileHandler(log_filename)
            file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
            file_handler.setLevel(logging.DEBUG if args.debug else logging.INFO)
            logging.getLogger().addHandler(file_handler)
            print(f"Logging to file: {log_filename}")
        except Exception as e:
            print(f"Error setting up log file: {e}")
            pass
    
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
