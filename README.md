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

**Important: This project requires Python 3.11.x**

```bash
# Ensure you have Python 3.11.x installed
# If using pyenv: pyenv install 3.11.9 && pyenv local 3.11.9

# Create virtual environment
python -m venv compendium-env

# On Windows:
.\compendium-env\Scripts\activate
# On macOS/Linux:
# source compendium-env/bin/activate

# Upgrade pip and install dependencies
python -m pip install --upgrade pip
pip install -r requirements.txt

# Verify installation
pip check

# Run smoke test to verify collector imports
python smoke_test_collectors.py

# Download spaCy model (if using the classifier)
python -m spacy download en_core_web_sm
```

### Troubleshooting

If you encounter dependency issues:

1. Ensure you're using Python 3.11.x (not newer versions like 3.12+)
2. The requirements.txt file uses `--only-binary :all:` for several packages to avoid compilation issues
3. For Google Scholar access issues, you may need to use a proxy or add delays between requests

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

### Environment Variables (Required)

The application uses environment variables for sensitive configuration. To set this up:

1. Copy the `.env.example` file to create a new file named `.env`:

```bash
# On Windows
copy .env.example .env
# On macOS/Linux
# cp .env.example .env
```

2. Edit the `.env` file and add your email address:

```
EMAIL=your_actual_email@example.com
```

**Note:** A valid email address is required for API access to PubMed, Unpaywall, and other services.

### Additional Settings

Edit `config.py` to customize:
- Output directory
- Cache directory
- Search parameters
- API rate limiting

## Output Files

- `all_hits.csv`: All discovered citations
- `used_compendium.csv`: Citations classified as "data used"
- `report.html`: HTML visualization of results

## License

[MIT License](LICENSE)
