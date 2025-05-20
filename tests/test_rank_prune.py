import sys
import os
import pathlib
import pandas as pd

# Add parent directory to Python path so we can import the project modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Now we can import project modules
from utils.keyword_loader import KeywordLoader
from compendium_tracker import CompendiumTracker
import config

# Create test instances
kl = KeywordLoader(config.KEYWORDS_PATH)
tracker = CompendiumTracker("dummy@example.com", skip_fulltext=True)

# Test case with a row that should meet the threshold
row = pd.Series({
    "title": "Hospital integration study using the AHRQ Compendium of U.S. Health Systems",
    "abstract": "We linked the Compendium data to ACO participation files …",
    "journal": "Health Services Research"
})

# This should pass - matches context terms and whitelist journal
def test_score_meets_threshold():
    score = tracker._score_row(row)
    print(f"Score: {score}, Threshold: {config.RELEVANCE_THRESHOLD}")
    assert score >= config.RELEVANCE_THRESHOLD, f"Score {score} should be >= threshold {config.RELEVANCE_THRESHOLD}"

# Test with a negative filter term
def test_negative_filter():
    negative_row = pd.Series({
        "title": "Imidazoline receptor studies using the AHRQ Compendium",
        "abstract": "We examined nanoparticle delivery systems...",
        "journal": "Health Affairs"
    })
    score = tracker._score_row(negative_row)
    print(f"Negative filter score: {score}")
    # This has both positive and negative factors
    assert score < config.RELEVANCE_THRESHOLD, f"Score {score} with negative filter should be < threshold"

if __name__ == "__main__":
    test_score_meets_threshold()
    test_negative_filter()
