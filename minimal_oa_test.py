#!/usr/bin/env python
"""
Minimal test for OpenAlex collector initialization
"""
from collectors.openalex_collector import OpenAlexCollector
import pyalex

dummy_email = "test@example.com"
print(f"Using pyalex version: {pyalex.__version__}")  # Should be 0.9

try:
    openalex = OpenAlexCollector(dummy_email, 1)
    print("✅ OpenAlex collector initialized successfully (minimal test)")
except Exception as e:
    print(f"❌ OpenAlex collector error (minimal test): {e}")
    import traceback
    traceback.print_exc()
