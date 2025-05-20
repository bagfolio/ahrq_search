# AHRQ Compendium Citation Tracker

A comprehensive pipeline that finds peer-reviewed papers using data from the AHRQ Compendium of U.S. Health Systems.

## Overview

This tool tracks the academic impact of the AHRQ Compendium by:

1. Searching multiple academic databases for citations (PubMed, OpenAlex, NIH OCC, Google Scholar)
2. Merging and deduplicating results
3. Retrieving and analyzing full-text content where available
4. Classifying citations as "data used" vs. "mention only"
5. Generating CSV reports and visualizations

## Installation

```bash
# Create virtual environment
python -m venv compendium-env
source compendium-env/bin/activate  # On Windows: compendium-env\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Download spaCy model (if using the classifier)
python -m spacy download en_core_web_sm
```

## Usage

Basic usage:

```bash
python compendium_tracker.py
```

This will:
- Search for papers citing the AHRQ Compendium
- Output CSV files with results
- Generate an HTML report with visualization

## Configuration

Edit `config.py` to customize:
- Email address for API access
- Output directory
- Search parameters

## Output Files

- `all_hits.csv`: All discovered citations
- `used_compendium.csv`: Citations classified as "data used"
- `report.html`: HTML visualization of results

## License

[MIT License](LICENSE)
