"""
HTML Reporter for AHRQ Compendium Citation Tracker.
Generates HTML report with visualizations.
"""

import pandas as pd
import pathlib
import logging
from typing import List, Dict, Any, Optional, Tuple
import matplotlib.pyplot as plt
from matplotlib.figure import Figure
import io
import base64
import datetime

import config

logger = logging.getLogger(__name__)

class HTMLReporter:
    """Generates HTML report with visualizations of citation data."""
    
    def __init__(self, output_dir: pathlib.Path = config.OUTPUT_DIR):
        """
        Initialize the HTML reporter.
        
        Args:
            output_dir: Directory to save HTML report
        """
        self.output_dir = output_dir
        self.output_dir.mkdir(exist_ok=True)
    
    def generate_report(self, df: pd.DataFrame) -> pathlib.Path:
        """
        Generate HTML report with visualizations.
        
        Args:
            df: DataFrame with all citation data
            
        Returns:
            Path to generated HTML report
        """
        if df is None or df.empty:
            logger.warning("No data to generate HTML report")
            return None
        
        # Create report components
        summary_stats = self._create_summary_stats(df)
        yearly_chart = self._create_yearly_chart(df)
        journal_chart = self._create_journal_chart(df)
        source_chart = self._create_source_chart(df)
        recent_articles = self._create_recent_articles_table(df)
        
        # Generate HTML content
        html_content = self._generate_html(
            summary_stats=summary_stats,
            yearly_chart=yearly_chart,
            journal_chart=journal_chart,
            source_chart=source_chart,
            recent_articles=recent_articles
        )
        
        # Save HTML report
        output_path = self.output_dir / config.REPORT_HTML
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"Generated HTML report at {output_path}")
        return output_path
    
    def _create_summary_stats(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Create summary statistics for the report.
        
        Args:
            df: DataFrame with citation data
            
        Returns:
            Dictionary with summary statistics
        """
        total_articles = len(df)
        used_data_count = df['uses_compendium'].sum() if 'uses_compendium' in df.columns else 0
        used_data_percent = (used_data_count / total_articles * 100) if total_articles > 0 else 0
        
        # Year range
        if 'year' in df.columns and not df['year'].empty:
            min_year = df['year'].min()
            max_year = df['year'].max()
            year_range = f"{min_year}-{max_year}"
        else:
            year_range = "N/A"
        
        # Source counts
        if 'source' in df.columns:
            source_counts = df['source'].value_counts().to_dict()
        else:
            source_counts = {}
        
        return {
            'total_articles': total_articles,
            'used_data_count': used_data_count,
            'used_data_percent': used_data_percent,
            'year_range': year_range,
            'source_counts': source_counts,
            'generated_date': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    
    def _create_yearly_chart(self, df: pd.DataFrame) -> Optional[str]:
        """
        Create chart of articles per year.
        
        Args:
            df: DataFrame with citation data
            
        Returns:
            Base64-encoded PNG image of chart
        """
        if 'year' not in df.columns or df['year'].empty:
            return None
        
        try:
            # Create figure
            fig, ax = plt.subplots(figsize=(10, 5))
            
            # Filter out missing years
            df_year = df.dropna(subset=['year'])
            if df_year.empty:
                return None
            
            # Convert to numeric and filter invalid years
            df_year['year'] = pd.to_numeric(df_year['year'], errors='coerce')
            df_year = df_year[df_year['year'] >= 2000]
            if df_year.empty:
                return None
            
            # Count by year
            yearly_counts = df_year.groupby('year').size()
            
            # Separate used vs. mentioned
            if 'uses_compendium' in df_year.columns:
                used_yearly = df_year[df_year['uses_compendium'] == 1].groupby('year').size()
                mentioned_yearly = df_year[df_year['uses_compendium'] == 0].groupby('year').size()
                
                # Plot stacked bar chart
                ax.bar(mentioned_yearly.index, mentioned_yearly.values, color='lightblue', label='Mentioned')
                ax.bar(used_yearly.index, used_yearly.values, bottom=mentioned_yearly.values, color='darkblue', label='Used Data')
                ax.legend()
            else:
                # Plot simple bar chart
                ax.bar(yearly_counts.index, yearly_counts.values, color='steelblue')
            
            # Customize chart
            ax.set_title('Articles per Year')
            ax.set_xlabel('Year')
            ax.set_ylabel('Number of Articles')
            ax.grid(axis='y', linestyle='--', alpha=0.7)
            
            # Convert to base64
            return self._fig_to_base64(fig)
            
        except Exception as e:
            logger.error(f"Error creating yearly chart: {e}")
            return None
    
    def _create_journal_chart(self, df: pd.DataFrame) -> Optional[str]:
        """
        Create chart of top journals.
        
        Args:
            df: DataFrame with citation data
            
        Returns:
            Base64-encoded PNG image of chart
        """
        if 'journal' not in df.columns or df['journal'].empty:
            return None
        
        try:
            # Create figure
            fig, ax = plt.subplots(figsize=(10, 6))
            
            # Count by journal and get top 10
            journal_counts = df['journal'].value_counts().nlargest(10)
            
            # Plot horizontal bar chart
            bars = ax.barh(journal_counts.index, journal_counts.values, color='steelblue')
            
            # Add count labels to bars
            for bar in bars:
                width = bar.get_width()
                ax.text(width + 0.3, bar.get_y() + bar.get_height()/2, f'{width:.0f}', 
                        va='center', fontsize=9)
            
            # Customize chart
            ax.set_title('Top 10 Journals')
            ax.set_xlabel('Number of Articles')
            ax.invert_yaxis()  # Highest count at top
            ax.grid(axis='x', linestyle='--', alpha=0.7)
            
            # Convert to base64
            return self._fig_to_base64(fig)
            
        except Exception as e:
            logger.error(f"Error creating journal chart: {e}")
            return None
    
    def _create_source_chart(self, df: pd.DataFrame) -> Optional[str]:
        """
        Create chart of data sources.
        
        Args:
            df: DataFrame with citation data
            
        Returns:
            Base64-encoded PNG image of chart
        """
        if 'source' not in df.columns or df['source'].empty:
            return None
        
        try:
            # Create figure
            fig, ax = plt.subplots(figsize=(8, 5))
            
            # Count by source
            source_counts = df['source'].value_counts()
            
            # Plot pie chart
            ax.pie(source_counts.values, labels=source_counts.index, autopct='%1.1f%%',
                   startangle=90, shadow=False, colors=plt.cm.Paired.colors)
            
            # Equal aspect ratio ensures that pie is drawn as a circle
            ax.axis('equal')
            ax.set_title('Citation Sources')
            
            # Convert to base64
            return self._fig_to_base64(fig)
            
        except Exception as e:
            logger.error(f"Error creating source chart: {e}")
            return None
    
    def _create_recent_articles_table(self, df: pd.DataFrame) -> str:
        """
        Create HTML table of recent articles.
        
        Args:
            df: DataFrame with citation data
            
        Returns:
            HTML table string
        """
        if df.empty:
            return "<p>No articles found.</p>"
        
        try:
            # Sort by year (descending) and get most recent 20
            if 'year' in df.columns:
                df_sorted = df.sort_values('year', ascending=False).head(20)
            else:
                df_sorted = df.head(20)
            
            # Select columns for display
            display_cols = ['title', 'journal', 'year', 'doi', 'uses_compendium']
            display_cols = [col for col in display_cols if col in df_sorted.columns]
            
            if not display_cols:
                return "<p>No article data available for display.</p>"
            
            # Create HTML table
            table_html = df_sorted[display_cols].to_html(
                index=False,
                escape=True,
                na_rep='',
                classes='data-table'
            )
            
            return table_html
            
        except Exception as e:
            logger.error(f"Error creating recent articles table: {e}")
            return "<p>Error generating recent articles table.</p>"
    
    def _fig_to_base64(self, fig: Figure) -> str:
        """
        Convert Matplotlib figure to base64-encoded PNG.
        
        Args:
            fig: Matplotlib figure
            
        Returns:
            Base64-encoded PNG image
        """
        buf = io.BytesIO()
        fig.savefig(buf, format='png', bbox_inches='tight', dpi=100)
        plt.close(fig)
        buf.seek(0)
        img_str = base64.b64encode(buf.read()).decode('utf-8')
        return f"data:image/png;base64,{img_str}"
    
    def _generate_html(self, summary_stats: Dict[str, Any], yearly_chart: Optional[str],
                      journal_chart: Optional[str], source_chart: Optional[str],
                      recent_articles: str) -> str:
        """
        Generate complete HTML report.
        
        Args:
            summary_stats: Dictionary with summary statistics
            yearly_chart: Base64-encoded yearly chart
            journal_chart: Base64-encoded journal chart
            source_chart: Base64-encoded source chart
            recent_articles: HTML table of recent articles
            
        Returns:
            Complete HTML report
        """
        # HTML template
        html_template = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>AHRQ Compendium Citation Tracker - Report</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }}
        h1, h2, h3 {{
            color: #2a5885;
        }}
        .container {{
            display: flex;
            flex-wrap: wrap;
            gap: 20px;
            margin-bottom: 30px;
        }}
        .card {{
            background: #f9f9f9;
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            flex: 1;
            min-width: 250px;
        }}
        .stat {{
            font-size: 24px;
            font-weight: bold;
            color: #2a5885;
        }}
        .chart {{
            margin: 20px 0;
            text-align: center;
        }}
        .data-table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }}
        .data-table th {{
            background-color: #2a5885;
            color: white;
            padding: 10px;
            text-align: left;
        }}
        .data-table td {{
            padding: 8px;
            border-bottom: 1px solid #ddd;
        }}
        .data-table tr:nth-child(even) {{
            background-color: #f2f2f2;
        }}
        .footer {{
            margin-top: 40px;
            text-align: center;
            font-size: 12px;
            color: #666;
        }}
    </style>
</head>
<body>
    <h1>AHRQ Compendium Citation Tracker</h1>
    <p>Report generated on {summary_stats['generated_date']}</p>
    
    <div class="container">
        <div class="card">
            <h3>Total Articles</h3>
            <div class="stat">{summary_stats['total_articles']}</div>
        </div>
        <div class="card">
            <h3>Articles Using Data</h3>
            <div class="stat">{summary_stats['used_data_count']}</div>
            <div>({summary_stats['used_data_percent']:.1f}% of total)</div>
        </div>
        <div class="card">
            <h3>Year Range</h3>
            <div class="stat">{summary_stats['year_range']}</div>
        </div>
    </div>
    
    <h2>Articles Over Time</h2>
    <div class="chart">
        {f'<img src="{yearly_chart}" alt="Articles per year">' if yearly_chart else '<p>No yearly data available</p>'}
    </div>
    
    <div class="container">
        <div class="card">
            <h2>Top Journals</h2>
            <div class="chart">
                {f'<img src="{journal_chart}" alt="Top journals">' if journal_chart else '<p>No journal data available</p>'}
            </div>
        </div>
        <div class="card">
            <h2>Citation Sources</h2>
            <div class="chart">
                {f'<img src="{source_chart}" alt="Citation sources">' if source_chart else '<p>No source data available</p>'}
            </div>
        </div>
    </div>
    
    <h2>Most Recent Articles</h2>
    {recent_articles}
    
    <div class="footer">
        <p>AHRQ Compendium Citation Tracker | Generated by the AHRQ Impact Analysis Team</p>
    </div>
</body>
</html>
"""
        return html_template
