#!/usr/bin/env python3
"""
exa_compendium_pipeline_v3.py
=============================

End-to-end discovery of papers that cite / discuss the *Compendium of U.S.
Health Systems* using the Exa API.

Key features
------------
▸ paginates until            : 1 000 results *or* Exa runs out of hits  
▸ date floor                 : 2021-01-01 (ISO 8601)  
▸ domain block-list          : ahrq.gov (override with --exclude)  
▸ category                   : 'research paper' (override with --category)  
▸ no nav/index noise         : quick URL + title heuristic  
▸ CSV columns                : title · url · publishedDate · author ·
                               exa_score · our_score · search_type ·
                               highlight_0/1/2  
▸ dup squashing              : canonical URL (drops query / fragment)  
▸ fully CLI-driven           : see --help  
▸ needs **EXA_API_KEY**      : in env var or .env file next to the script

Author : <you>
Updated: 2025-05-20
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import pathlib
import re
import sys
from datetime import datetime
from typing import Any, Dict, List
from urllib.parse import urlparse, urlunparse

import pandas as pd
import requests

# ───────────────────────────────────────────────────────────────────────────────
# 0.  Environment & configuration helpers
# ───────────────────────────────────────────────────────────────────────────────
# Get the project root (one level up from the script's directory)
ROOT = pathlib.Path(__file__).resolve().parent.parent
OUTPUT_DIR = ROOT / "output"
OUTPUT_DIR.mkdir(exist_ok=True)
ENV_PATH = ROOT / ".env"
logging.info(f"Using .env file from: {ENV_PATH}")


def _load_dotenv(path: pathlib.Path) -> None:
    """Populate os.environ from a .env file (key=value per line)."""
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip("'\""))


_load_dotenv(ENV_PATH)

EXA_KEY = os.getenv("EXA_API_KEY")
if not EXA_KEY:
    sys.exit("❌  Set EXA_API_KEY in the environment or .env file first.")

EXA_SEARCH_URL = "https://api.exa.ai/search"
EXA_CONTENTS_URL = "https://api.exa.ai/contents"

SESSION = requests.Session()
SESSION.headers.update({"x-api-key": EXA_KEY, "Content-Type": "application/json"})

# ───────────────────────────────────────────────────────────────────────────────
# 1.  Constants & quick heuristics
# ───────────────────────────────────────────────────────────────────────────────
PHRASE_VARIANTS = [
    '"Compendium of U.S. Health Systems"',
    '"Compendium of US Health Systems"',
    '"AHRQ Compendium of U.S. Health Systems"',
    '"AHRQ Compendium of US Health Systems"',
]

PEER_DOMAINS = {
    "arxiv.org",
    "ncbi.nlm.nih.gov",
    "pubmed.ncbi.nlm.nih.gov",
    "bmj.com",
    "nejm.org",
    "jamanetwork.com",
    "sciencedirect.com",
    "springer.com",
    "nature.com",
    "onlinelibrary.wiley.com",
    "healthaffairs.org",
}

NAV_TITLE = re.compile(r"(A-Z|Topics|Index|Home\s?Page|Archive|Navigation)", re.I)
PDF_LIKE = re.compile(r"\.pdf($|\?)", re.I)
DOI_LIKE = re.compile(r"doi\.org/|/article/|/abs/|/full/", re.I)


def looks_like_paper(url: str) -> bool:
    """Fast URL heuristic – keep obvious PDFs / DOI / peer domains."""
    return bool(
        PDF_LIKE.search(url)
        or DOI_LIKE.search(url)
        or any(d in url for d in PEER_DOMAINS)
    )


def is_nav_page(title: str, summary: str) -> bool:
    return bool(NAV_TITLE.search(title) or NAV_TITLE.search(summary))


def canonical(url: str) -> str:
    """Normalise the URL to drop trivial differences for de-duplication."""
    p = urlparse(url)
    p = p._replace(scheme="https", netloc=p.netloc.lower(), query="", fragment="")
    return urlunparse(p).rstrip("/")


def our_score(rec: Dict[str, Any]) -> int:
    """Tiny custom relevance tweak on top of Exa's own ranking."""
    url = rec["url"].lower()
    title = rec["title"].lower() if rec.get("title") else ""
    body = " ".join(rec.get("highlights") or []) + (rec.get("summary") or "")

    score = 1  # keyword hit (guaranteed by the query)

    if any(dom in url for dom in PEER_DOMAINS):
        score += 2
    if "compendium" in body.lower() and "health system" in body.lower():
        score += 3
    if is_nav_page(title, body):
        score -= 5
    return score


# ───────────────────────────────────────────────────────────────────────────────
# 2.  Exa helpers
# ───────────────────────────────────────────────────────────────────────────────
def _exa_post(url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    resp = SESSION.post(url, json=payload, timeout=30)
    try:
        resp.raise_for_status()
    except Exception:
        logging.error("Exa error (%s): %s", resp.status_code, resp.text)
        raise
    return resp.json()


def exa_search_paged(
    query: str,
    category: str,
    exclude_domains: List[str],
    start_date: str,
    hard_cap: int = 1_000,
) -> List[Dict[str, Any]]:
    """Loop over /search pages until we hit `hard_cap` or run out of cursors."""
    cursor = None
    hits: List[Dict[str, Any]] = []

    while True:
        body: Dict[str, Any] = {
            "query": query,
            "category": category,
            "type": "auto",
            "numResults": 1000,  # MUST be 100 to get a nextCursor
            "excludeDomains": exclude_domains,
            "startPublishedDate": start_date,
            **({"cursor": cursor} if cursor else {}),
        }
        rsp = _exa_post(EXA_SEARCH_URL, body)
        batch = rsp["results"]
        hits.extend(batch)
        logging.info(
            "Fetched %3d (running total %4d) – cost $%.4f",
            len(batch),
            len(hits),
            rsp["costDollars"]["total"],
        )

        if not batch or len(hits) >= hard_cap:
            break
        cursor = rsp.get("nextCursor")
        if not cursor:
            break

    return hits[:hard_cap]


def exa_fetch_contents(urls: List[str]) -> List[Dict[str, Any]]:
    if not urls:
        return []
    body = {
        "urls": urls,
        "text": False,  # we only need summary + highlights
        "highlights": {"max": 3},
        "summary": {"sentences": 3},
        "livecrawl": "fallback",
    }
    return _exa_post(EXA_CONTENTS_URL, body)["results"]


# ───────────────────────────────────────────────────────────────────────────────
# 3.  Orchestration
# ───────────────────────────────────────────────────────────────────────────────
def run(
    max_hits: int,
    category: str,
    start_date: str,
    exclude_domains: List[str],
) -> None:
    query = " OR ".join(PHRASE_VARIANTS)
    logging.info("Search query = %s", query)

    search_hits = exa_search_paged(
        query=query,
        category=category,
        exclude_domains=exclude_domains,
        start_date=start_date,
        hard_cap=max_hits,
    )

    # quick URL heuristic: keep “paper-ish” links
    paper_urls = [h["url"] for h in search_hits if looks_like_paper(h["url"])]
    logging.info("URL filter kept %d/%d hits", len(paper_urls), len(search_hits))

    contents = exa_fetch_contents(paper_urls)

    # map url → meta (score, resolvedSearchType, etc.)
    meta_map = {h["url"]: h for h in search_hits}

    rows: List[Dict[str, Any]] = []
    for c in contents:
        m = meta_map.get(c["url"], {})
        # Safely get up to 3 highlights, defaulting to empty strings
        highlights = c.get("highlights") or []
        row = {
            "title": c.get("title"),
            "url": c["url"],
            "publishedDate": c.get("publishedDate"),
            "author": c.get("author"),
            "exa_score": m.get("score"),
            "search_type": m.get("resolvedSearchType"),
            "highlight_0": highlights[0] if len(highlights) > 0 else "",
            "highlight_1": highlights[1] if len(highlights) > 1 else "",
            "highlight_2": highlights[2] if len(highlights) > 2 else "",
        }
        row["our_score"] = our_score({**row, **c})
        rows.append(row)

    df = pd.DataFrame(rows)

    # de-dup by canonical URL
    df["canon"] = df["url"].map(canonical)
    before = len(df)
    df.drop_duplicates(subset="canon", inplace=True)
    after = len(df)
    logging.info("De-duplicated %d → %d rows", before, after)
    df.drop(columns="canon", inplace=True)

    # tidy / sort
    df["publishedDate"] = pd.to_datetime(df["publishedDate"], errors="coerce")
    df.sort_values(
        ["our_score", "exa_score", "publishedDate"],
        ascending=[False, False, False],
        inplace=True,
    )

    out_csv = OUTPUT_DIR / "compendium_hits.csv"
    df.to_csv(out_csv, index=False)
    logging.info("✅  Wrote final CSV → %s (%d rows)", out_csv, len(df))

    # optional raw dump for debugging
    raw_jsonl = OUTPUT_DIR / "compendium_raw.jsonl"
    with raw_jsonl.open("w", encoding="utf-8") as f:
        for h in search_hits:
            f.write(json.dumps(h) + "\n")
    logging.debug("Raw search hits dumped to %s", raw_jsonl)


# ───────────────────────────────────────────────────────────────────────────────
# 4.  CLI
# ───────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Discover papers citing the AHRQ Health-System Compendium"
    )
    parser.add_argument(
        "-n",
        "--max-hits",
        type=int,
        default=1_000,
        help="maximum number of hits to harvest (default 1000)",
    )
    parser.add_argument(
        "-c",
        "--category",
        default="research paper",
        help='Exa "category" (e.g. "research paper", "news", "pdf")',
    )
    parser.add_argument(
        "--start",
        default="2021-01-01T00:00:00Z",
        help="ISO-8601 startPublishedDate filter (default 2021-01-01)",
    )
    parser.add_argument(
        "--exclude",
        nargs="*",
        default=["ahrq.gov"],
        help="space-separated list of domains to drop (default ahrq.gov)",
    )
    parser.add_argument(
        "--debug", action="store_true", help="emit verbose DEBUG logging"
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%H:%M:%S",
    )

    logging.info("▶  Starting Exa harvest – want up to %d rows", args.max_hits)
    run(
        max_hits=args.max_hits,
        category=args.category,
        start_date=args.start,
        exclude_domains=args.exclude,
    )
    logging.info("🏁  Done.")
