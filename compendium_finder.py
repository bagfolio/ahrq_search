#!/usr/bin/env python3
"""
AHRQ Compendium Citation Finder

A simplified version of the citation tracker that uses direct web searches
to find citations of the AHRQ Compendium of U.S. Health Systems.

This script:
1. Loads keywords and URLs from keywords.yaml
2. Searches for citations using web requests
3. Analyzes results to determine if they use the Compendium data
4. Generates reports of findings

Usage:
    python compendium_finder.py [--output OUTPUT_DIR] [--limit LIMIT]
"""

import os
import sys
import argparse
import yaml
import time
import logging
import pathlib
import json
import re
from datetime import datetime
from typing import Dict, List, Any, Tuple
import concurrent.futures

import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
import matplotlib.pyplot as plt

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

class CitationFinder:
    """Finds citations of the AHRQ Compendium using web searches."""
    
    def __init__(self, keywords_file: str, output_dir: str, result_limit: int = 100):
        """
        Initialize the citation finder.
        
        Args:
            keywords_file: Path to keywords YAML file
            output_dir: Directory to save output files
            result_limit: Maximum results to process per keyword
        """
        self.keywords_file = keywords_file
        self.output_dir = pathlib.Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.result_limit = result_limit
        
        # Load keywords
        self.keywords = self._load_keywords()
        
        # User agent for requests
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        }
        
        # Create output CSV file
        self.results_file = self.output_dir / "citation_results.csv"
        
    def _load_keywords(self) -> Dict[str, List[str]]:
        """Load keywords from YAML file."""
        try:
            with open(self.keywords_file, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load keywords from {self.keywords_file}: {e}")
            sys.exit(1)
    
    def search_citations(self):
        """Search for citations of the AHRQ Compendium."""
        logger.info("Starting citation search...")
        
        # Get search terms
        all_terms = self._get_all_search_terms()
        logger.info(f"Loaded {len(all_terms)} search terms")
        
        # Search for each term
        all_results = []
        
        for term_type, terms in all_terms.items():
            logger.info(f"Searching for {term_type} terms ({len(terms)} terms)")
            
            for term in tqdm(terms, desc=f"Searching {term_type}"):
                # Search using term
                results = self._search_term(term, term_type)
                
                # Add results to list
                all_results.extend(results)
                
                # Sleep to avoid rate limiting
                time.sleep(1)
        
        # Convert results to DataFrame
        if all_results:
            df = pd.DataFrame(all_results)
            logger.info(f"Found {len(df)} total citations")
            
            # Remove duplicates
            df = self._deduplicate_results(df)
            logger.info(f"After deduplication: {len(df)} unique citations")
            
            # Save results
            self._save_results(df)
            
            # Generate report
            self._generate_report(df)
        else:
            logger.warning("No citations found")
    
    def _get_all_search_terms(self) -> Dict[str, List[str]]:
        """Get all search terms organized by type."""
        search_terms = {}
        
        # Add URL terms
        if 'exact_urls' in self.keywords:
            search_terms['exact_urls'] = self.keywords['exact_urls']
        
        # Add PDF URL terms
        if 'pdf_urls' in self.keywords:
            search_terms['pdf_urls'] = self.keywords['pdf_urls']
        
        # Add phrase terms
        if 'phrase_variants' in self.keywords:
            search_terms['phrases'] = self.keywords['phrase_variants']
        
        # Add year combo terms
        if 'year_combos' in self.keywords:
            search_terms['year_combos'] = self.keywords['year_combos']
        
        return search_terms
    
    def _search_term(self, term: str, term_type: str) -> List[Dict[str, Any]]:
        """
        Search for citations using a specific term.
        
        Args:
            term: Search term
            term_type: Type of term (url, phrase, etc.)
            
        Returns:
            List of citation dictionaries
        """
        results = []
        
        try:
            # For this simplified version, we'll search using a web service
            # that returns citation data in JSON format
            search_url = f"https://api.crossref.org/works?query={requests.utils.quote(term)}&rows=20"
            
            response = requests.get(search_url, headers=self.headers)
            
            if response.status_code == 200:
                data = response.json()
                
                # Process results
                if 'message' in data and 'items' in data['message']:
                    items = data['message']['items']
                    
                    for item in items[:self.result_limit]:
                        # Extract citation information
                        citation = self._extract_citation_info(item, term, term_type)
                        
                        if citation:
                            results.append(citation)
            else:
                logger.warning(f"Search failed for term '{term}': {response.status_code}")
        
        except Exception as e:
            logger.error(f"Error searching for term '{term}': {e}")
        
        return results
    
    def _extract_citation_info(self, item: Dict[str, Any], term: str, term_type: str) -> Dict[str, Any]:
        """
        Extract citation information from search result.
        
        Args:
            item: Search result item
            term: Search term used
            term_type: Type of term used
            
        Returns:
            Citation dictionary or None if invalid
        """
        try:
            # Extract basic information
            title = item.get('title', [''])[0] if 'title' in item and item['title'] else ''
            
            # Skip if no title
            if not title:
                return None
            
            # Extract DOI
            doi = item.get('DOI', '').lower()
            
            # Extract publication date
            year = None
            if 'published' in item and 'date-parts' in item['published']:
                date_parts = item['published']['date-parts']
                if date_parts and date_parts[0]:
                    year = date_parts[0][0]
            
            # Extract authors
            authors = []
            if 'author' in item:
                for author in item['author']:
                    if 'family' in author and 'given' in author:
                        authors.append(f"{author['family']}, {author['given']}")
            
            # Extract journal/container
            journal = ''
            if 'container-title' in item and item['container-title']:
                journal = item['container-title'][0]
            
            # Create citation entry
            citation = {
                'title': title,
                'authors': '; '.join(authors),
                'journal': journal,
                'year': year,
                'doi': doi,
                'url': f"https://doi.org/{doi}" if doi else '',
                'source': 'Crossref',
                'match_term': term,
                'term_type': term_type,
                'uses_compendium': self._check_uses_compendium(title),
            }
            
            return citation
            
        except Exception as e:
            logger.error(f"Error extracting citation info: {e}")
            return None
    
    def _check_uses_compendium(self, text: str) -> int:
        """
        Check if text indicates the paper uses the Compendium data.
        
        Args:
            text: Text to check
            
        Returns:
            1 if uses Compendium, 0 otherwise
        """
        # Simple check for keywords that indicate usage
        usage_indicators = [
            'used', 'utilized', 'employing', 'analyzed', 'derived from',
            'based on', 'data from', 'using the compendium'
        ]
        
        # Check for combination of 'compendium' and usage indicator
        has_compendium = 'compendium' in text.lower()
        
        if has_compendium:
            for indicator in usage_indicators:
                if indicator in text.lower():
                    return 1
        
        return 0
    
    def _deduplicate_results(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove duplicate citations.
        
        Args:
            df: DataFrame with citation results
            
        Returns:
            Deduplicated DataFrame
        """
        # Drop duplicates based on DOI if available
        if 'doi' in df.columns and not df['doi'].isna().all():
            df = df.drop_duplicates(subset=['doi'])
        
        # Then drop duplicates based on title
        if 'title' in df.columns:
            # Normalize titles
            df['title_norm'] = df['title'].str.lower().str.replace(r'\W+', '', regex=True)
            df = df.drop_duplicates(subset=['title_norm'])
            df = df.drop('title_norm', axis=1)
        
        return df
    
    def _save_results(self, df: pd.DataFrame):
        """
        Save results to CSV file.
        
        Args:
            df: DataFrame with citation results
        """
        try:
            df.to_csv(self.results_file, index=False)
            logger.info(f"Saved {len(df)} citations to {self.results_file}")
        except Exception as e:
            logger.error(f"Error saving results: {e}")
    
    def _generate_report(self, df: pd.DataFrame):
        """
        Generate HTML report of citation results.
        
        Args:
            df: DataFrame with citation results
        """
        report_file = self.output_dir / "citation_report.html"
        
        try:
            # Create visualizations
            self._create_visualizations(df)
            
            # Generate HTML report
            html_content = self._generate_html_report(df)
            
            # Save report
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(html_content)
                
            logger.info(f"Generated report at {report_file}")
            
        except Exception as e:
            logger.error(f"Error generating report: {e}")
    
    def _create_visualizations(self, df: pd.DataFrame):
        """
        Create visualizations of citation results.
        
        Args:
            df: DataFrame with citation results
        """
        # Create output directory for figures
        figures_dir = self.output_dir / "figures"
        figures_dir.mkdir(exist_ok=True)
        
        # Create year distribution chart
        if 'year' in df.columns and not df['year'].isna().all():
            try:
                plt.figure(figsize=(10, 6))
                year_counts = df['year'].value_counts().sort_index()
                year_counts.plot(kind='bar', color='steelblue')
                plt.title('Citations by Year')
                plt.xlabel('Year')
                plt.ylabel('Number of Citations')
                plt.grid(axis='y', linestyle='--', alpha=0.7)
                plt.tight_layout()
                plt.savefig(figures_dir / "citations_by_year.png")
                plt.close()
            except Exception as e:
                logger.error(f"Error creating year chart: {e}")
        
        # Create source type chart
        if 'term_type' in df.columns:
            try:
                plt.figure(figsize=(8, 8))
                source_counts = df['term_type'].value_counts()
                plt.pie(source_counts, labels=source_counts.index, autopct='%1.1f%%', 
                        startangle=90, colors=plt.cm.tab10.colors)
                plt.axis('equal')
                plt.title('Citations by Search Term Type')
                plt.tight_layout()
                plt.savefig(figures_dir / "citations_by_term_type.png")
                plt.close()
            except Exception as e:
                logger.error(f"Error creating term type chart: {e}")
    
    def _generate_html_report(self, df: pd.DataFrame) -> str:
        """
        Generate HTML report content.
        
        Args:
            df: DataFrame with citation results
            
        Returns:
            HTML report content
        """
        # Get summary statistics
        total_citations = len(df)
        using_compendium = df['uses_compendium'].sum()
        percent_using = (using_compendium / total_citations * 100) if total_citations > 0 else 0
        
        # Get year range
        year_range = "N/A"
        if 'year' in df.columns and not df['year'].isna().all():
            min_year = df['year'].min()
            max_year = df['year'].max()
            year_range = f"{min_year} - {max_year}"
        
        # Generate HTML
        html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>AHRQ Compendium Citation Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; line-height: 1.6; }}
        h1, h2, h3 {{ color: #2c5985; }}
        .summary {{ display: flex; justify-content: space-between; margin-bottom: 20px; }}
        .summary-card {{ background: #f5f5f5; padding: 15px; border-radius: 5px; width: 30%; }}
        .summary-card h3 {{ margin-top: 0; }}
        .summary-value {{ font-size: 24px; font-weight: bold; color: #2c5985; }}
        .charts {{ display: flex; flex-wrap: wrap; justify-content: space-between; margin-bottom: 20px; }}
        .chart {{ width: 48%; background: #f5f5f5; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
        table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #2c5985; color: white; }}
        tr:nth-child(even) {{ background-color: #f2f2f2; }}
    </style>
</head>
<body>
    <h1>AHRQ Compendium Citation Report</h1>
    <p>Report generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    
    <div class="summary">
        <div class="summary-card">
            <h3>Total Citations</h3>
            <div class="summary-value">{total_citations}</div>
        </div>
        <div class="summary-card">
            <h3>Using Compendium Data</h3>
            <div class="summary-value">{using_compendium}</div>
            <div>({percent_using:.1f}% of total)</div>
        </div>
        <div class="summary-card">
            <h3>Year Range</h3>
            <div class="summary-value">{year_range}</div>
        </div>
    </div>
    
    <div class="charts">
        <div class="chart">
            <h2>Citations by Year</h2>
            <img src="figures/citations_by_year.png" alt="Citations by Year" style="width: 100%;">
        </div>
        <div class="chart">
            <h2>Citations by Search Term Type</h2>
            <img src="figures/citations_by_term_type.png" alt="Citations by Search Term Type" style="width: 100%;">
        </div>
    </div>
    
    <h2>Recent Citations</h2>
    <table>
        <tr>
            <th>Title</th>
            <th>Authors</th>
            <th>Journal</th>
            <th>Year</th>
            <th>Uses Compendium</th>
        </tr>
"""
        
        # Add rows for most recent citations (up to 20)
        if 'year' in df.columns:
            recent_df = df.sort_values('year', ascending=False).head(20)
        else:
            recent_df = df.head(20)
        
        for _, row in recent_df.iterrows():
            uses_compendium = "Yes" if row.get('uses_compendium', 0) == 1 else "No"
            html += f"""        <tr>
            <td>{row.get('title', '')}</td>
            <td>{row.get('authors', '')}</td>
            <td>{row.get('journal', '')}</td>
            <td>{row.get('year', '')}</td>
            <td>{uses_compendium}</td>
        </tr>
"""
        
        # Close HTML
        html += """    </table>
    
    <h2>Download Data</h2>
    <p>The complete citation data is available in CSV format: <a href="citation_results.csv">citation_results.csv</a></p>
    
    <footer>
        <p>AHRQ Compendium Citation Finder | Generated by the AHRQ Impact Analysis Team</p>
    </footer>
</body>
</html>"""
        
        return html

def main():
    """Main entry point for the script."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='AHRQ Compendium Citation Finder')
    parser.add_argument('--keywords', default='keywords.yaml', help='Path to keywords YAML file')
    parser.add_argument('--output', default='output', help='Directory to save output files')
    parser.add_argument('--limit', type=int, default=100, help='Maximum results per keyword')
    args = parser.parse_args()
    
    # Create and run finder
    finder = CitationFinder(args.keywords, args.output, args.limit)
    finder.search_citations()

if __name__ == "__main__":
    main()
