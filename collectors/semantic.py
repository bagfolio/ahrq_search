"""
Utility module for finding academic papers that explicitly mention the
AHRQ **Compendium of U.S. Health Systems** using Semantic Scholar’s public
Graph API.

Key features
------------
*   **Phrase‑level search (snippet endpoint)** – captures mentions that
    appear anywhere in the title, abstract *or full‑text body* (≈500‑word
    windows).
*   **Variant handling** – searches a configurable list of phrase
    variants so we don’t miss common shorthand such as “AHRQ compendium”.
*   **Batch metadata enrichment** – once we collect distinct `paperId`s
    from all snippets, we hit `/paper/batch` in chunks for full metadata
    (title, year, URL, authors, OA PDF link, citation count, …).
*   **Single public entry point** – `search_compendium_mentions()`, which
    returns a `pandas.DataFrame` ready for downstream analysis or CSV
    export.

This code is *drop‑in* for the existing pipeline: just import the helper
and pass your `SEMANTIC_SCHOLAR_API_KEY` (or leave `api_key=None` if you
are staying under the anon rate limit during dev).
"""
from __future__ import annotations

import itertools
import os
import time
from typing import Iterable, List, Dict, Any, Optional

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Configuration – tweak as needed -------------------------------------------
# ---------------------------------------------------------------------------
DEFAULT_PHRASES: List[str] = [
    "Compendium of U.S. Health Systems",
    "AHRQ Compendium of U.S. Health Systems",
    "Compendium of US Health Systems",
    "US Health Systems Compendium",
    "AHRQ health systems compendium",
    "AHRQ Compendium",
]

# More conservative limits to avoid rate limiting
SNIPPET_LIMIT_PER_PHRASE = 100  # Start with fewer results, can increase later
BATCH_CHUNK = 100                # Smaller batches to avoid large requests
CALLS_PER_MIN = 5                # Very conservative rate (1 request per 12 seconds)
MAX_RETRIES = 3                  # Number of retry attempts for rate-limited requests
RETRY_BACKOFF = 2.0              # Exponential backoff multiplier

# ---------------------------------------------------------------------------
# Low‑level API wrapper ------------------------------------------------------
# ---------------------------------------------------------------------------

class SemanticScholarClient:
    BASE = "https://api.semanticscholar.org"
    GQL = "/graph/v1"

    def __init__(self, api_key: Optional[str] = None):
        self.sess = requests.Session()
        if api_key:
            self.sess.headers["x-api-key"] = api_key
        self.last_call = 0.0

    # ----- throttling helper ------------------------------------------------
    def _sleep_if_necessary(self):
        """More robust client‑side throttling to avoid 429s."""
        elapsed = time.time() - self.last_call
        min_interval = 60.0 / CALLS_PER_MIN
        if elapsed < min_interval:
            sleep_time = min_interval - elapsed
            print(f"Rate limiting: Sleeping for {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)

    # ----- core HTTP helpers -----------------------------------------------
    def _get(self, path: str, **params):
        url = f"{self.BASE}{path}"
        for attempt in range(MAX_RETRIES + 1):  # +1 for the initial attempt
            self._sleep_if_necessary()
            try:
                print(f"Making GET request to {url} (attempt {attempt+1}/{MAX_RETRIES+1})")
                resp = self.sess.get(url, params=params, timeout=30)
                self.last_call = time.time()
                
                if resp.status_code == 429:
                    retry_after = min(60, attempt * RETRY_BACKOFF * 10)  # Exponential backoff
                    print(f"Rate limited (429). Waiting {retry_after:.1f}s before retry...")
                    time.sleep(retry_after)
                    continue
                    
                resp.raise_for_status()
                return resp.json()
                
            except requests.exceptions.RequestException as e:
                if attempt < MAX_RETRIES and ("429" in str(e) or "Too Many Requests" in str(e)):
                    retry_after = min(60, attempt * RETRY_BACKOFF * 10)  # Exponential backoff
                    print(f"Rate limited: {e}. Waiting {retry_after:.1f}s before retry...")
                    time.sleep(retry_after)
                else:
                    print(f"Error making request to {url}: {e}")
                    return {"data": []}
                    
        print(f"Failed after {MAX_RETRIES+1} attempts to {url}")
        return {"data": []}

    def _post(self, path: str, *, json: dict, **params):
        url = f"{self.BASE}{path}"
        for attempt in range(MAX_RETRIES + 1):  # +1 for the initial attempt
            self._sleep_if_necessary()
            try:
                print(f"Making POST request to {url} (attempt {attempt+1}/{MAX_RETRIES+1})")
                resp = self.sess.post(url, params=params, json=json, timeout=30)
                self.last_call = time.time()
                
                if resp.status_code == 429:
                    retry_after = min(60, attempt * RETRY_BACKOFF * 10)  # Exponential backoff
                    print(f"Rate limited (429). Waiting {retry_after:.1f}s before retry...")
                    time.sleep(retry_after)
                    continue
                    
                resp.raise_for_status()
                return resp.json()
                
            except requests.exceptions.RequestException as e:
                if attempt < MAX_RETRIES and ("429" in str(e) or "Too Many Requests" in str(e)):
                    retry_after = min(60, attempt * RETRY_BACKOFF * 10)  # Exponential backoff
                    print(f"Rate limited: {e}. Waiting {retry_after:.1f}s before retry...")
                    time.sleep(retry_after)
                else:
                    print(f"Error making POST request to {url}: {e}")
                    return []
                    
        print(f"Failed after {MAX_RETRIES+1} attempts to {url}")
        return []

    # ----- high‑level endpoints we need ------------------------------------

    def snippet_search(self, query: str, limit: int = 1000) -> List[dict]:
        """Return snippet records (may be empty)."""
        path = f"{self.GQL}/snippet/search"
        params = {"query": query, "limit": limit}
        return self._get(path, **params).get("data", [])

    def paper_batch(self, ids: List[str], fields: str) -> List[dict]:
        path = f"{self.GQL}/paper/batch"
        params = {"fields": fields}
        return self._post(path, params=params, json={"ids": ids})

# ---------------------------------------------------------------------------
# Public helper -------------------------------------------------------------
# ---------------------------------------------------------------------------

def search_compendium_mentions(
    api_key: Optional[str] = None,
    phrases: Iterable[str] | None = None,
    snippet_limit: int = SNIPPET_LIMIT_PER_PHRASE,
) -> pd.DataFrame:
    """Search S2 for any mention of the Compendium phrases.

    Parameters
    ----------
    api_key : str | None
        Personal API key.  *None* is allowed for small, anonymous runs.
    phrases : list[str] | None
        Custom list of literal phrases.  *None* uses `DEFAULT_PHRASES`.
    snippet_limit : int
        How many snippets to pull *per phrase* (max 1000).

    Returns
    -------
    pandas.DataFrame
        One row per *paper* that contains ≥1 snippet match.  Includes a
        list‑of‑dicts column named ``snippets`` with each raw snippet, so
        downstream code can surface context to users if desired.
    """
    cli = SemanticScholarClient(api_key or os.getenv("SEMANTIC_SCHOLAR_API_KEY"))
    phrases = list(phrases or DEFAULT_PHRASES)

    # ---- 1) Collect snippets ------------------------------------------------
    hits: Dict[str, List[dict]] = {}
    for phrase in phrases:
        for snip in cli.snippet_search(phrase, limit=snippet_limit):
            pid = snip.get("paper", {}).get("paperId")
            if not pid:
                continue
            hits.setdefault(pid, []).append(snip)

    if not hits:
        return pd.DataFrame(columns=[
            "paperId", "title", "year", "url", "openAccessPdf", "citationCount", "snippets"
        ])

    # ---- 2) Batch‑fetch metadata -----------------------------------------
    all_ids = list(hits.keys())
    meta_rows: List[dict] = []
    FIELD_LIST = (
        "title,year,url,isOpenAccess,openAccessPdf,citationCount,authors"
    )
    for chunk_start in range(0, len(all_ids), BATCH_CHUNK):
        chunk_ids = all_ids[chunk_start:chunk_start + BATCH_CHUNK]
        meta_rows.extend(cli.paper_batch(chunk_ids, fields=FIELD_LIST))

    # ---- 3) Merge
    for row in meta_rows:
        row["snippets"] = hits.get(row["paperId"], [])

    df = pd.DataFrame(meta_rows)
    # Robust ordering: newest first, then citation count desc
    df = df.sort_values(["year", "citationCount"], ascending=[False, False]).reset_index(drop=True)
    return df

# ---------------------------------------------------------------------------
# CLI usage example ---------------------------------------------------------
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse
    import logging
    import os
    from dotenv import load_dotenv
    
    # Load environment variables from .env file
    try:
        load_dotenv()
    except ImportError:
        print("dotenv package not found. Environment variables may not be loaded correctly.")
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Search Semantic Scholar for AHRQ Compendium mentions")
    parser.add_argument(
        "--limit", 
        type=int, 
        default=SNIPPET_LIMIT_PER_PHRASE,
        help=f"Limit per phrase (default: {SNIPPET_LIMIT_PER_PHRASE})"
    )
    parser.add_argument(
        "--output", 
        default="output/semantic_scholar_results.csv",
        help="Output CSV file path (default: output/semantic_scholar_results.csv)"
    )
    parser.add_argument(
        "--debug", 
        action="store_true",
        help="Enable debug mode with verbose logging"
    )
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO, 
        format="%(asctime)s | %(levelname)s | %(message)s"
    )
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    
    # Set up API key from environment
    api_key = os.getenv("SEMANTIC_SCHOLAR_API_KEY")
    if api_key:
        print("Using Semantic Scholar API key from environment")
    else:
        print("No API key found. Using anonymous access (rate limited)")
    
    print(f"Searching for Compendium mentions in Semantic Scholar (limit={args.limit})...")
    try:
        df = search_compendium_mentions(
            api_key=api_key,
            snippet_limit=args.limit
        )
        if len(df) > 0:
            # Save to CSV
            df.to_csv(args.output, index=False)
            print(f"Found {len(df)} papers mentioning the Compendium")
            print(f"Results saved to {args.output}")
            print("\nSample results:")
            print(df[["title", "year", "citationCount"]].head())
        else:
            print("No papers found mentioning the Compendium")
    except Exception as e:
        print(f"Error occurred: {e}")
        import traceback
        traceback.print_exc()
