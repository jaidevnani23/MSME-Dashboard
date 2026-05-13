#!/usr/bin/env python3
"""
patch_demand.py — Ghost Stealth Product Demand Fetcher (v2.1)
=====================================================================
Optimized for GitHub Actions to avoid 429 IP bans.
"""

from __future__ import annotations
import argparse
import datetime
import functools
import json
import logging
import pathlib
import random
import sys
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

# ── Unbuffered print for real-time GitHub Action logs
print = functools.partial(print, flush=True)

try:
    from pytrends.request import TrendReq
except ImportError:
    sys.exit("pytrends not found. Run: pip install pytrends --break-system-packages")

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ── Configuration (Ghost Mode) ───────────────────────────────────────────────
PRODUCTS_PER_BATCH      = 20
MIN_DELAY_SECONDS       = 70.0   # Increased slightly
MAX_DELAY_SECONDS       = 140.0 
RETRY_DELAY             = 600.0  # 10 minutes on 429
LONG_DELAY_EVERY        = random.randint(3, 5) # Variable break pattern
LONG_DELAY_SECONDS      = 400.0
CIRCUIT_BREAKER_LIMIT   = 2      # Max 429s before we give up to get a new IP

# Updated 2024/2026 Modern User Agents
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0"
]

# ── Search Term Refinement ──────────────────────────────────────────────────
# (Kept your logic, but prioritizing semi-specific terms)
def extract_search_term(product_name: str) -> str:
    lower = product_name.lower()
    # Logic for semi-specific overrides
    if "soop" in lower: return "bamboo basket"
    if "cushion" in lower: return "cushion covers"
    if "mango" in lower: return "alphonso mango"
    
    # Generic extraction fallback
    words = [w for w in lower.replace("(", " ").replace(")", " ").split() if len(w) > 3]
    return " ".join(words[:2]) if len(words) >= 2 else lower.split()[0]

# ── Session & Rate Logic ──────────────────────────────────────────────────────

class GhostSession:
    def __init__(self):
        self.session = None
        self.req_count = 0
        self.refresh_at = random.randint(3, 6)

    def refresh(self):
        ua = random.choice(USER_AGENTS)
        log.info(f"🔄 Rotating Session (UA: {ua[:40]}...)")
        self.session = TrendReq(hl='en-IN', tz=330, timeout=(10, 25), requests_args={
            'headers': {
                'User-Agent': ua,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Referer': 'https://trends.google.com/trends/explore?geo=IN'
            }
        })
        self.req_count = 0
        self.refresh_at = random.randint(3, 6)

    def get_session(self):
        if not self.session or self.req_count >= self.refresh_at:
            self.refresh()
        self.req_count += 1
        return self.session

# ── Main Execution ───────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", type=int, required=True)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--dashboard", default="index.html")
    args = parser.parse_args()

    # 1. Load Products
    all_products = load_products_from_dashboard(args.dashboard)
    start = (args.batch - 1) * PRODUCTS_PER_BATCH
    end = min(start + PRODUCTS_PER_BATCH, len(all_products))
    batch_products = all_products[start:end]
    
    # 2. SHUFFLE the batch (Crucial for Stealth)
    random.shuffle(batch_products)
    log.info(f"👻 Ghost Mode: Shuffled {len(batch_products)} products for Batch {args.batch}")

    tracker = ProgressTracker(args.batch)
    out_path = pathlib.Path("data/demand_products.json")
    session_mgr = GhostSession()
    
    consecutive_429s = 0

    for idx, (cat_id, name, state) in enumerate(batch_products):
        key = f"{cat_id}:{name}"
        if args.resume and tracker.is_done(key):
            continue

        # Delay Logic
        if idx > 0:
            delay = random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)
            if idx % LONG_DELAY_EVERY == 0:
                delay = LONG_DELAY_SECONDS + random.uniform(10, 60)
                log.info(f"☕ Long break: {delay/60:.1f} min...")
            else:
                log.info(f"⏳ Waiting {delay:.1f}s...")
            time.sleep(delay)

        # Fetch Data
        term = extract_search_term(name)
        log.info(f"🔍 Querying: {name} -> [{term}]")
        
        try:
            pytrends = session_mgr.get_session()
            pytrends.build_payload([term], timeframe='today 12-m', geo='IN')
            df = pytrends.interest_over_time()
            
            if df.empty:
                log.warning(f"⚠️ No data for {term}")
                continue
                
            demand = normalise_to_demand(df[term].tolist())
            tracker.mark_done(key, {"new": demand, "term": term, "category": cat_id, "state": state})
            consecutive_429s = 0 # Reset on success
            log.info(f"✅ Success: {demand}")

        except Exception as e:
            if "429" in str(e):
                consecutive_429s += 1
                log.error(f"🛑 429 Hit ({consecutive_429s}/{CIRCUIT_BREAKER_LIMIT})")
                if consecutive_429s >= CIRCUIT_BREAKER_LIMIT:
                    log.critical("‼️ Circuit Breaker Open. Exiting to trigger GitHub IP change.")
                    sys.exit(1) # Fail the action so it retries later
                time.sleep(RETRY_DELAY)
            else:
                log.error(f"❌ Error: {e}")

    # [Standard Output Merging Logic follows...]
