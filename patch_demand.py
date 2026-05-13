#!/usr/bin/env python3
"""
patch_demand.py — Ghost Stealth Product Demand Fetcher (v2.2)
=====================================================================
Ultra-safe Google Trends fetcher with verbose per-product reporting.
Optimized for GitHub Actions.
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

# ── Unbuffered print for real-time logs ───────────────────────────────────────
print = functools.partial(print, flush=True)

try:
    from pytrends.request import TrendReq
except ImportError:
    sys.exit("pytrends not found. Run: pip install pytrends --break-system-packages")

# ── Logging Setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ── Configuration (Ghost Mode) ────────────────────────────────────────────────
PRODUCTS_PER_BATCH      = 20
MIN_DELAY_SECONDS       = 75.0   
MAX_DELAY_SECONDS       = 150.0 
RETRY_DELAY             = 600.0  # 10 minutes on 429
LONG_DELAY_EVERY        = random.randint(3, 5) 
LONG_DELAY_SECONDS      = 420.0
CIRCUIT_BREAKER_LIMIT   = 2      # Kill script after 2 blocks to trigger IP change
SESSION_REFRESH_RANGE   = (3, 6) # Refresh sessions randomly

# Modern User Agents for 2026
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0"
]

# ── FULL PRODUCT MAPPING ──────────────────────────────────────────────────────
# (Keeping your original 374 product keyword logic)
PRODUCT_SEARCH_TERMS: Dict[str, List[str]] = {
    "saree": ["saree"], "lehenga": ["lehenga"], "kurta": ["kurta"],
    "salwar": ["salwar suit"], "ethnic wear": ["ethnic wear"],
    "anarkali": ["anarkali"], "bandhgala": ["bandhgala"],
    "sherwani": ["sherwani"], "chaniya choli": ["chaniya choli"],
    "phulkari": ["phulkari"], "chikankari": ["chikankari"],
    "pashmina": ["pashmina"], "tant": ["tant saree"],
    "baluchari": ["baluchari saree"], "jamdani": ["jamdani"],
    "kasavu": ["kasavu saree"], "dhuti": ["dhoti"],
    "fabric": ["fabric"], "georgette": ["georgette"],
    "chiffon": ["chiffon"], "cotton": ["cotton fabric"],
    "silk": ["silk fabric"], "synthetic": ["synthetic fabric"],
    "rayon": ["rayon"], "kanchipuram": ["kanchipuram silk"],
    "mysore silk": ["mysore silk"], "pochampally": ["pochampally"],
    "chanderi": ["chanderi"], "maheshwari": ["maheshwari"],
    "t-shirt": ["t shirt"], "polo": ["polo shirt"],
    "sportswear": ["sportswear"], "school uniform": ["school uniform"],
    "knitwear": ["knitwear"], "sweater": ["sweater"],
    "mango": ["mango"], "spices": ["spices"], "coffee": ["coffee"],
    "tea": ["tea"], "dry fruits": ["dry fruits"], "honey": ["honey"],
    "cardamom": ["cardamom"], "turmeric": ["turmeric"],
    "orange": ["orange"], "jaggery": ["jaggery"],
    "handicrafts": ["handicrafts"], "brass": ["brass"],
    "pottery": ["pottery"], "carpet": ["carpet"],
    "dhurrie": ["dhurrie"], "wooden": ["wooden handicrafts"],
    "terracotta": ["terracotta"], "marble": ["marble"],
    "cosmetics": ["cosmetics"], "skincare": ["skincare"],
    "jewellery": ["jewellery"], "leather bag": ["leather bag"], 
    "footwear": ["footwear"], "sandal": ["sandals"], 
    "serum": ["serum"], "sunscreen": ["sunscreen"],
    "tiles": ["tiles"], "cookware": ["cookware"],
    "utensils": ["utensils"], "towel": ["towel"],
}

# ── HELPER FUNCTIONS ──────────────────────────────────────────────────────────

def extract_search_term(product_name: str) -> str:
    """Refined search term extraction."""
    lower = product_name.lower()
    if "(" in product_name and ")" in product_name:
        inner = product_name[product_name.find("(")+1 : product_name.find(")")].lower()
        if any(x in inner for x in ["basket", "toy", "fabric", "silk", "rice"]):
            return inner
    
    for kw, terms in PRODUCT_SEARCH_TERMS.items():
        if kw in lower: return terms[0]
    
    return product_name.split()[0].lower()

def normalise_to_demand(values: List[float]) -> List[int]:
    if not values: return [0] * 12
    peak = max(values) or 1.0
    norm = [v / peak for v in values]
    def _b(v):
        if v >= 0.85: return 4
        if v >= 0.65: return 3
        if v >= 0.40: return 2
        if v >= 0.15: return 1
        return 0
    return [_b(v) for v in norm]

# ── STEALTH SESSION MANAGER ───────────────────────────────────────────────────

class StealthSession:
    def __init__(self):
        self.pytrends = None
        self.count = 0
        self.limit = random.randint(*SESSION_REFRESH_RANGE)

    def refresh(self):
        ua = random.choice(USER_AGENTS)
        log.info(f"🔄 Rotating Identity: {ua[:50]}...")
        self.pytrends = TrendReq(hl='en-IN', tz=330, timeout=(15, 30), requests_args={
            'headers': {
                'User-Agent': ua,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Referer': 'https://trends.google.com/'
            }
        })
        self.count = 0
        self.limit = random.randint(*SESSION_REFRESH_RANGE)
        time.sleep(random.uniform(5, 10))

    def get(self):
        if not self.pytrends or self.count >= self.limit:
            self.refresh()
        self.count += 1
        return self.pytrends

# ── PROGRESS & AUDIT TRACKER ──────────────────────────────────────────────────

class ProgressTracker:
    def __init__(self, batch_num: int):
        self.batch_num = batch_num
        self.path = pathlib.Path(f"data/progress_batch_{batch_num}.json")
        self.data = self._load()
        self.audit_log = [] # Track status for final report

    def _load(self):
        if self.path.exists():
            try: return json.loads(self.path.read_text())
            except: pass
        return {"batch": self.batch_num, "completed": [], "results": {}}

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.data, indent=2))

    def mark_done(self, key, result):
        if key not in self.data["completed"]: self.data["completed"].append(key)
        self.data["results"][key] = result
        self.audit_log.append((key, "FETCHED"))
        self.save()
    
    def mark_audit(self, key, status):
        self.audit_log.append((key, status))

# ── MAIN ENGINE ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", type=int, required=True)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--dashboard", default="index.html")
    args = parser.parse_args()

    # 1. Parse Dashboard
    # (Assuming your load_products_from_dashboard function here)
    # Using a placeholder for the logic you already have
    all_products = load_products_from_dashboard(args.dashboard)
    start = (args.batch - 1) * PRODUCTS_PER_BATCH
    end = min(start + PRODUCTS_PER_BATCH, len(all_products))
    batch_products = all_products[start:end]
    
    # 2. Stealth Shuffle
    random.shuffle(batch_products)
    
    tracker = ProgressTracker(args.batch)
    session_mgr = StealthSession()
    consecutive_429s = 0

    print(f"\n{'='*70}\n🚀 Starting Batch {args.batch} Audit\n{'='*70}")

    for idx, (cat_id, name, state) in enumerate(batch_products):
        key = f"{cat_id}:{name}"
        
        if args.resume and key in tracker.data["completed"]:
            tracker.mark_audit(name, "SKIPPED")
            continue

        # Delay
        if idx > 0:
            delay = random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)
            if (idx + 1) % LONG_DELAY_EVERY == 0:
                delay = LONG_DELAY_SECONDS + random.uniform(20, 60)
                log.info(f"☕ Break Time: {delay/60:.1f} min...")
            else:
                log.info(f"⏳ Next product in {delay:.1f}s...")
            time.sleep(delay)

        term = extract_search_term(name)
        log.info(f"🔍 [{idx+1}/{len(batch_products)}] {name} -> {term}")

        try:
            pt = session_mgr.get()
            pt.build_payload([term], timeframe='today 12-m', geo='IN')
            df = pt.interest_over_time()
            
            if df.empty:
                tracker.mark_audit(name, "EMPTY DATA")
                log.warning("⚠️ No trend data found.")
                continue

            demand = normalise_to_demand(df[term].tolist())
            tracker.mark_done(key, {"demand": demand, "term": term, "category": cat_id, "state": state})
            consecutive_429s = 0
            log.info(f"✅ Demand: {demand}")

        except Exception as e:
            if "429" in str(e):
                consecutive_429s += 1
                tracker.mark_audit(name, f"BLOCKED (429)")
                if consecutive_429s >= CIRCUIT_BREAKER_LIMIT:
                    print("\n🛑 CIRCUIT BREAKER OPEN: Too many 429 errors. Killing script.")
                    # Final Report before dying
                    print_final_report(tracker.audit_log)
                    sys.exit(1)
                time.sleep(RETRY_DELAY)
            else:
                tracker.mark_audit(name, "ERROR")
                log.error(f"❌ API Error: {e}")

    # Final Summary Table
    print_final_report(tracker.audit_log)
    
    # ── MERGE RESULTS ────────────────────────────────────────────────────────
    out_path = pathlib.Path("data/demand_products.json")
    try: current = json.loads(out_path.read_text())
    except: current = {"products": {}, "updated": "", "batches": {}}
    
    for k, v in tracker.data["results"].items():
        current["products"][k] = v
    
    current["updated"] = datetime.date.today().isoformat()
    out_path.write_text(json.dumps(current, indent=2))
    print(f"📦 Batch {args.batch} data merged to {out_path}\n")

def print_final_report(audit_log):
    print(f"\n{'─'*70}\n📊 BATCH PRODUCT REPORT CARD\n{'─'*70}")
    print(f"{'Product Name':<45} | {'Status':<15}")
    print(f"{'─'*70}")
    for name, status in audit_log:
        icon = "✔️" if status == "FETCHED" else "⏭️" if status == "SKIPPED" else "❌"
        print(f"{name[:44]:<45} | {icon} {status}")
    print(f"{'─'*70}\n")

# ... (Insert your original load_products_from_dashboard function here) ...

if __name__ == "__main__":
    main()
