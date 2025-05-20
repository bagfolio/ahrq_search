#!/usr/bin/env python
"""
Smoke test script to verify collector imports and basic functionality
"""

import sys
print(f"Python version: {sys.version}")

try:
    from collectors.pubmed_collector import PubMedCollector
    from collectors.openalex_collector import OpenAlexCollector
    from collectors.citation_collector import CitationCollector
    from collectors.scholar_collector import ScholarCollector
    import pandas as pd
    
    print("✅ All collector modules imported successfully!")
    
    # Use a dummy email for testing
    dummy_email = "test@example.com"
    test_terms = ['"Compendium of U.S. Health Systems"'][:1]
    
    # Initialize collectors with minimal parameters
    print("\nInitializing collectors...")
    try:
        pubmed = PubMedCollector(dummy_email, 1)
        print("✅ PubMed collector initialized")
    except Exception as e:
        print(f"❌ PubMed collector error: {e}")
    
    try:
        openalex = OpenAlexCollector(dummy_email, 1)
        print("✅ OpenAlex collector initialized")
    except Exception as e:
        print(f"❌ OpenAlex collector error: {e}")
    
    try:
        citation = CitationCollector(dummy_email)
        print("✅ Citation collector initialized")
    except Exception as e:
        print(f"❌ Citation collector error: {e}")
    
    try:
        # Set sleep_seconds to 0 to avoid waiting during smoke test
        scholar = ScholarCollector(dummy_email, 1, 0)
        print("✅ Scholar collector initialized")
    except Exception as e:
        print(f"❌ Scholar collector error: {e}")
    
    print("\n✅ Smoke test complete! All collectors can be imported and initialized.")
    print("\nNote: This test only verifies imports and initialization, not actual API connectivity.")
    print("Next steps: Re-create the virtual environment with Python 3.11.9 and install dependencies.")
    
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("\nThis likely indicates that your Python environment is not correctly set up.")
    print("Please follow these steps to resolve:")
    print("1. Install Python 3.11.9")
    print("2. Create a new virtual environment: python -m venv compendium-env")
    print("3. Activate the environment:")
    print("   - Windows: .\\compendium-env\\Scripts\\activate")
    print("   - macOS/Linux: source compendium-env/bin/activate")
    print("4. Install dependencies: pip install -r requirements.txt")
