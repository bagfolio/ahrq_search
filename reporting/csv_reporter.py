"""
CSV Reporter for AHRQ Compendium Citation Tracker.
Generates CSV files with citation data.
"""

import pandas as pd
import pathlib
import logging
from typing import List, Dict, Any

import config

logger = logging.getLogger(__name__)

class CSVReporter:
    """Generates CSV reports with citation data."""
    
    def __init__(self, output_dir: pathlib.Path = config.OUTPUT_DIR):
        """
        Initialize the CSV reporter.
        
        Args:
            output_dir: Directory to save CSV files
        """
        self.output_dir = output_dir
        self.output_dir.mkdir(exist_ok=True)
    
    def generate_all_hits_csv(self, df: pd.DataFrame) -> pathlib.Path:
        """
        Generate CSV file with all citation hits.
        
        Args:
            df: DataFrame with all citation data
            
        Returns:
            Path to generated CSV file
        """
        if df is None or df.empty:
            logger.warning("No data to generate all_hits.csv")
            return None
        
        # Create standardized columns for export
        output_df = self._standardize_columns(df)
        
        # Save to CSV
        output_path = self.output_dir / config.ALL_HITS_CSV
        output_df.to_csv(output_path, index=False, encoding='utf-8')
        
        logger.info(f"Generated all_hits.csv with {len(output_df)} rows at {output_path}")
        return output_path
    
    def generate_used_compendium_csv(self, df: pd.DataFrame) -> pathlib.Path:
        """
        Generate CSV file with citations that used the Compendium data.
        
        Args:
            df: DataFrame with all citation data
            
        Returns:
            Path to generated CSV file
        """
        if df is None or df.empty:
            logger.warning("No data to generate used_compendium.csv")
            return None
        
        # Check if uses_compendium column exists (may not when running with --no-fulltext)
        if 'uses_compendium' not in df.columns:
            logger.warning("'uses_compendium' column not found - skipping used_compendium.csv generation")
            return None
            
        # Filter to only include citations with uses_compendium = 1
        used_df = df[df['uses_compendium'] == 1].copy()
        
        if used_df.empty:
            logger.warning("No citations classified as 'uses_compendium' found")
            return None
        
        # Create standardized columns for export
        output_df = self._standardize_columns(used_df)
        
        # Save to CSV
        output_path = self.output_dir / config.USED_COMPENDIUM_CSV
        output_df.to_csv(output_path, index=False, encoding='utf-8')
        
        logger.info(f"Generated used_compendium.csv with {len(output_df)} rows at {output_path}")
        return output_path
    
    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize columns for CSV export.
        
        Args:
            df: DataFrame to standardize
            
        Returns:
            Standardized DataFrame
        """
        # List of standard columns in desired order
        standard_columns = [
            'title', 'authors', 'author_string', 'journal', 'year', 
            'doi', 'pmid', 'abstract', 'url', 'source', 'match_term',
            'uses_compendium', 'classification_method', 'evidence', 'snippet'
        ]
        
        # Create a new DataFrame with standard columns
        output_df = pd.DataFrame()
        
        # Copy existing columns and add empty ones if missing
        for column in standard_columns:
            if column in df.columns:
                output_df[column] = df[column]
            else:
                output_df[column] = None
        
        # Convert list columns to strings for CSV export
        if 'authors' in output_df.columns and len(output_df) > 0 and isinstance(output_df['authors'].iloc[0], list):
            output_df['authors'] = output_df['authors'].apply(lambda x: '; '.join(x) if isinstance(x, list) else x)
        
        # Ensure numeric columns are numeric
        if 'year' in output_df.columns:
            output_df['year'] = pd.to_numeric(output_df['year'], errors='coerce')
        
        # Handle uses_compendium column (may be missing in --no-fulltext mode)
        if 'uses_compendium' not in output_df.columns:
            output_df['uses_compendium'] = 0  # Default to 0 if missing
        elif len(output_df) > 0:  # Only process if dataframe has data
            output_df['uses_compendium'] = pd.to_numeric(output_df['uses_compendium'], errors='coerce').fillna(0).astype(int)
        
        return output_df
