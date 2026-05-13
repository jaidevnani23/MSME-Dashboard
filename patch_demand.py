#!/usr/bin/env python3
"""
patch_demand.py — Maximum Stealth Product Demand Fetching
=====================================================================
Ultra-conservative approach to avoid 429 errors completely.

CRITICAL CHANGES:
- Much longer initial delays before first request
- Smaller batches (20 products)
- Extreme delays (60-120s between requests)
- Request warm-up period
- Better session management
- Optional proxy support
"""

import argparse
import json
import pathlib
import sys
import time
import datetime
import random
from datetime import datetime as dt
from typing import Dict, List, Tuple, Optional

# Force unbuffered output
import functools
print = functools.partial(print, flush=True)

try:
    from pytrends.request import TrendReq
except ImportError:
    sys.exit("pytrends not found. Run: pip install pytrends --break-system-packages")

# ── ULTRA-CONSERVATIVE CONFIGURATION ─────────────────────────────────────────
PRODUCTS_PER_BATCH = 20          # MUCH smaller batches
MIN_DELAY_SECONDS = 60.0         # 1 minute minimum
MAX_DELAY_SECONDS = 120.0        # 2 minutes maximum
INITIAL_WARMUP_DELAY = 30.0      # Wait before first request
RETRY_ATTEMPTS = 1               # Only 1 retry
RETRY_DELAY = 300.0              # 5 minute backoff
LONG_DELAY_EVERY = 3             # Break every 3 products
LONG_DELAY_SECONDS = 300.0       # 5 minute breaks
MAX_REQUESTS_PER_MINUTE = 1      # VERY conservative (1 per minute)
SESSION_REFRESH_EVERY = 3        # New session every 3 products
JITTER_FACTOR = 0.3
CIRCUIT_BREAKER_THRESHOLD = 2
COOL_DOWN_PERIOD = 600.0         # 10 minute cooldown

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
]

MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

# ── Product-to-search-term mapping (same as before) ──────────────────────────
PRODUCT_SEARCH_TERMS = {
    "saree": ["saree"], "lehenga": ["lehenga"], "kurta": ["kurta"],
    "salwar": ["salwar suit"], "ethnic wear": ["ethnic wear"],
    "anarkali": ["anarkali"], "bandhgala": ["bandhgala"],
    "sherwani": ["sherwani"], "chaniya choli": ["chaniya choli"],
    "phulkari": ["phulkari"], "chikankari": ["chikankari"],
    "pashmina": ["pashmina"], "tant": ["tant saree"],
    "baluchari": ["baluchari saree"], "jamdani": ["jamdani"],
    "kasavu": ["kasavu saree"], "dhuti": ["dhoti"],
    "mundu": ["mundu"], "mekhela": ["mekhela chador"],
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
    "guava": ["guava"], "orange": ["orange"], "jaggery": ["jaggery"],
    "handicrafts": ["handicrafts"], "brass": ["brass"],
    "pottery": ["pottery"], "carpet": ["carpet"],
    "dhurrie": ["dhurrie"], "wooden": ["wooden handicrafts"],
    "terracotta": ["terracotta"], "marble": ["marble"],
    "cosmetics": ["cosmetics"], "skincare": ["skincare"],
    "jewellery": ["jewellery"], "kundan": ["kundan"],
    "leather bag": ["leather bag"], "footwear": ["footwear"],
    "sandal": ["sandals"], "serum": ["serum"], "sunscreen": ["sunscreen"],
    "tiles": ["tiles"], "cookware": ["cookware"],
    "utensils": ["utensils"], "towel": ["towel"],
    "oversized": ["oversized"], "graphic": ["graphic tee"],
}


class AdvancedRateLimiter:
    """Ultra-conservative rate limiter"""
    
    def __init__(self, max_per_minute: int):
        self.max_per_minute = max_per_minute
        self.request_times = []
        self.consecutive_429s = 0
        self.circuit_open = False
        self.circuit_opened_at = None
        self.first_request = True
    
    def add_jitter(self, delay: float) -> float:
        jitter = delay * JITTER_FACTOR * (random.random() * 2 - 1)
        return max(1.0, delay + jitter)
    
    def wait_if_needed(self):
        # First request gets extra warm-up time
        if self.first_request:
            warmup = self.add_jitter(INITIAL_WARMUP_DELAY)
            print(f"    🌡️  Warm-up delay: {warmup:.0f}s (helps avoid detection)...")
            time.sleep(warmup)
            self.first_request = False
        
        # Check circuit breaker
        if self.circuit_open:
            elapsed = time.time() - self.circuit_opened_at
            if elapsed < COOL_DOWN_PERIOD:
                remaining = COOL_DOWN_PERIOD - elapsed
                print(f"    🚨 CIRCUIT BREAKER: Cooling down for {remaining:.0f}s...")
                time.sleep(remaining)
            else:
                print(f"    ✅ Circuit breaker reset")
                self.circuit_open = False
                self.consecutive_429s = 0
        
        now = time.time()
        self.request_times = [t for t in self.request_times if now - t < 60]
        
        if len(self.request_times) >= self.max_per_minute:
            oldest = self.request_times[0]
            base_wait = 60 - (now - oldest)
            wait_time = self.add_jitter(base_wait + random.uniform(10, 20))
            if wait_time > 0:
                print(f"    ⏳ Rate limit: waiting {wait_time:.1f}s...")
                time.sleep(wait_time)
                self.request_times = []
        
        self.request_times.append(time.time())
    
    def record_429(self):
        self.consecutive_429s += 1
        print(f"    ⚠️  429 count: {self.consecutive_429s}/{CIRCUIT_BREAKER_THRESHOLD}")
        
        if self.consecutive_429s >= CIRCUIT_BREAKER_THRESHOLD:
            self.circuit_open = True
            self.circuit_opened_at = time.time()
            print(f"    🚨 CIRCUIT BREAKER TRIGGERED! Entering {COOL_DOWN_PERIOD}s cooldown...")
    
    def record_success(self):
        self.consecutive_429s = 0


class TrendsSessionManager:
    """Session manager with more conservative approach"""
    
    def __init__(self):
        self.session_count = 0
        self.current_session = None
    
    def get_session(self, force_new: bool = False) -> TrendReq:
        if force_new or self.current_session is None:
            user_agent = random.choice(USER_AGENTS)
            print(f"      🔄 Creating new session (#{self.session_count + 1})")
            print(f"      🌐 User-Agent: {user_agent[:60]}...")
            
            # Longer delay before creating session
            time.sleep(random.uniform(5, 12))
            
            self.current_session = TrendReq(
                hl="en-IN",
                tz=330,
                retries=0,  # No automatic retries
                backoff_factor=5.0,
                timeout=(15, 30),
                requests_args={
                    'headers': {
                        'User-Agent': user_agent,
                        'Accept-Language': 'en-IN,en;q=0.9',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'DNT': '1',
                        'Connection': 'keep-alive',
                        'Upgrade-Insecure-Requests': '1',
                    }
                }
            )
            self.session_count += 1
        
        return self.current_session
    
    def should_refresh(self, products_processed: int) -> bool:
        return products_processed > 0 and (products_processed % SESSION_REFRESH_EVERY) == 0


def load_products_from_dashboard(html_path: str) -> List[Tuple[str, str, str]]:
    """Extract all products from the dashboard HTML file."""
    try:
        with open(html_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        start = content.find('const PRODUCTS_BY_CAT = {')
        if start == -1:
            raise ValueError("Could not find PRODUCTS_BY_CAT in dashboard HTML")
        
        brace_count = 0
        json_start = start + len('const PRODUCTS_BY_CAT = ')
        json_end = json_start
        
        for i, char in enumerate(content[json_start:], start=json_start):
            if char == '{':
                brace_count += 1
            elif char == '}':
                brace_count -= 1
                if brace_count == 0:
                    json_end = i + 1
                    break
        
        json_str = content[json_start:json_end]
        products_data = json.loads(json_str)
        
        products = []
        for category, items in products_data.items():
            cat_id = category.lower().replace(' ', '_').replace('&', 'and')
            for item in items:
                products.append((cat_id, item['Product'], item.get('State', 'Unknown')))
        
        return products
        
    except Exception as e:
        print(f"❌ Error loading products from dashboard: {e}")
        sys.exit(1)


def extract_search_terms(product_name: str) -> List[str]:
    """Extract relevant search terms - SIMPLIFIED to use only ONE term"""
    product_lower = product_name.lower()
    
    # Find matching keywords
    matches = []
    for keyword, terms in PRODUCT_SEARCH_TERMS.items():
        pos = product_lower.find(keyword)
        if pos != -1:
            matches.append((pos, keyword, terms))
    
    # Use ONLY the first term from the earliest match
    if matches:
        matches.sort(key=lambda x: x[0])
        _, keyword, terms = matches[0]
        return [terms[0]]  # Only first term
    
    # Fallback: use first meaningful word
    words = product_name.split()
    stop_words = {"the", "a", "an", "and", "or", "of", "for", "with", "in", "&"}
    
    for word in words:
        clean_word = word.lower().strip("(),.-")
        if clean_word not in stop_words and len(clean_word) > 2:
            return [clean_word]
    
    return [words[0].lower()] if words else ["product"]


def fetch_interest(session_mgr: TrendsSessionManager, terms: List[str], 
                   timeframe: str, geo: str, rate_limiter: AdvancedRateLimiter, 
                   retry_count: int = 0) -> Optional[List[float]]:
    """Fetch interest with maximum caution - only ONE term at a time"""
    
    # Use only first term to minimize request complexity
    term = terms[0] if terms else "product"
    
    print(f"      🔍 Querying Google Trends for: {term}")
    
    rate_limiter.wait_if_needed()
    pytrends = session_mgr.get_session()
    
    try:
        pytrends.build_payload([term], timeframe=timeframe, geo=geo)
        df = pytrends.interest_over_time()
        print(f"      ✓ Data received ({len(df)} rows)")
        rate_limiter.record_success()
        
    except Exception as exc:
        error_str = str(exc).lower()
        
        if '429' in error_str or 'too many' in error_str or 'rate' in error_str:
            rate_limiter.record_429()
            
            if retry_count < RETRY_ATTEMPTS:
                backoff_delay = rate_limiter.add_jitter(RETRY_DELAY)
                print(f"      🚫 Rate limited (429), backing off {backoff_delay:.0f}s...")
                print(f"      🔄 Will create fresh session on retry...")
                time.sleep(backoff_delay)
                session_mgr.get_session(force_new=True)
                return fetch_interest(session_mgr, terms, timeframe, geo, 
                                    rate_limiter, retry_count + 1)
            else:
                print(f"      ❌ Rate limit exceeded after {RETRY_ATTEMPTS} retries")
                return None
        else:
            if retry_count < RETRY_ATTEMPTS:
                wait = rate_limiter.add_jitter(RETRY_DELAY / 2)
                print(f"      ⚠️  API error, retrying in {wait:.0f}s... ({exc})")
                time.sleep(wait)
                return fetch_interest(session_mgr, terms, timeframe, geo, 
                                    rate_limiter, retry_count + 1)
            else:
                print(f"      ❌ Failed after {RETRY_ATTEMPTS} retries: {exc}")
                return None

    if df.empty:
        print(f"      ⚠️  No data returned")
        return None

    if "isPartial" in df.columns:
        df = df.drop(columns=["isPartial"])

    df.index = df.index.to_period("M")
    monthly = df.groupby(df.index).mean()
    all_periods = sorted(monthly.index)
    
    if len(all_periods) >= 12:
        last_12_periods = all_periods[-12:]
        monthly_vals = [float(monthly.loc[p].mean()) for p in last_12_periods]
    else:
        monthly_vals = [0.0] * (12 - len(all_periods))
        for period in all_periods:
            monthly_vals.append(float(monthly.loc[period].mean()))

    mx = max(monthly_vals) or 1.0
    normalised = [v / mx for v in monthly_vals]
    return normalised


def normalise_to_demand(values: List[float]) -> List[int]:
    """Maps 0.0–1.0 to demand scores 0–4."""
    def bucket(v: float) -> int:
        if v >= 0.85: return 4
        if v >= 0.65: return 3
        if v >= 0.40: return 2
        if v >= 0.15: return 1
        return 0
    return [bucket(v) for v in values]


class ProgressTracker:
    """Tracks progress"""
    
    def __init__(self, batch_num: int):
        self.batch_num = batch_num
        self.checkpoint_file = pathlib.Path(f"data/progress_batch_{batch_num}.json")
        self.data = self._load()
    
    def _load(self) -> Dict:
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'r') as f:
                    return json.load(f)
            except:
                pass
        
        return {
            "batch": self.batch_num,
            "started": datetime.datetime.now().isoformat(),
            "completed_products": [],
            "failed_products": [],
            "results": {}
        }
    
    def save(self):
        self.checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.checkpoint_file, 'w') as f:
            json.dump(self.data, f, indent=2)
    
    def is_completed(self, product_key: str) -> bool:
        return product_key in self.data["completed_products"]
    
    def mark_completed(self, product_key: str, result: Dict):
        if product_key not in self.data["completed_products"]:
            self.data["completed_products"].append(product_key)
        self.data["results"][product_key] = result
        self.save()
    
    def mark_failed(self, product_key: str, error: str):
        self.data["failed_products"].append({
            "product": product_key,
            "error": error,
            "timestamp": datetime.datetime.now().isoformat()
        })
        self.save()
    
    def get_completion_rate(self, total: int) -> float:
        return (len(self.data["completed_products"]) / total * 100) if total > 0 else 0


def calculate_total_batches(total_products: int) -> int:
    return (total_products + PRODUCTS_PER_BATCH - 1) // PRODUCTS_PER_BATCH


def main():
    parser = argparse.ArgumentParser(description="ULTRA-SAFE Google Trends fetcher")
    parser.add_argument("--batch", type=int, help="Batch number")
    parser.add_argument("--start", type=int, help="Custom start index")
    parser.add_argument("--end", type=int, help="Custom end index")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--timeframe", default="today 12-m")
    parser.add_argument("--geo", default="IN")
    parser.add_argument("--dashboard", default="index.html")
    args = parser.parse_args()
    
    print(f"\n{'='*70}")
    print(f"  ULTRA-SAFE Mode — Product Demand Fetcher")
    print(f"{'='*70}")
    
    dashboard_path = args.dashboard
    if not pathlib.Path(dashboard_path).exists():
        dashboard_path = f"/mnt/user-data/uploads/{dashboard_path}"
    
    all_products = load_products_from_dashboard(dashboard_path)
    total_products = len(all_products)
    total_batches = calculate_total_batches(total_products)
    
    print(f"  ✅ Loaded {total_products} products")
    print(f"  📊 Total batches: {total_batches} ({PRODUCTS_PER_BATCH} products/batch)")
    
    if args.start is not None and args.end is not None:
        start_idx = args.start
        end_idx = args.end
        batch_num = 0
    elif args.batch:
        batch_num = args.batch
        if batch_num > total_batches:
            print(f"❌ Error: Batch {batch_num} exceeds total batches ({total_batches})")
            sys.exit(1)
        start_idx = (batch_num - 1) * PRODUCTS_PER_BATCH
        end_idx = min(start_idx + PRODUCTS_PER_BATCH, total_products)
    else:
        print("❌ Error: Specify either --batch or both --start and --end")
        sys.exit(1)
    
    batch_products = all_products[start_idx:end_idx]
    batch_size = len(batch_products)
    
    print(f"\n  📦 ULTRA-CONSERVATIVE Batch {batch_num}:")
    print(f"     Products: {batch_size}")
    print(f"     Delays: {MIN_DELAY_SECONDS}-{MAX_DELAY_SECONDS}s per request")
    print(f"     Warmup: {INITIAL_WARMUP_DELAY}s before first request")
    print(f"     Breaks: {LONG_DELAY_SECONDS}s every {LONG_DELAY_EVERY} products")
    print(f"     Rate: {MAX_REQUESTS_PER_MINUTE} request/minute MAX")
    
    avg_delay = (MIN_DELAY_SECONDS + MAX_DELAY_SECONDS) / 2
    total_time = (batch_size * avg_delay) + ((batch_size // LONG_DELAY_EVERY) * LONG_DELAY_SECONDS)
    print(f"     Est. Time: {total_time/60:.1f} minutes")
    print(f"{'='*70}\n")
    
    tracker = ProgressTracker(batch_num if batch_num > 0 else 0)
    
    out_path = pathlib.Path("data") / "demand_products.json"
    try:
        existing = json.loads(out_path.read_text(encoding="utf-8"))
        product_map = existing.get("products", {})
    except Exception:
        product_map = {}
    
    session_mgr = TrendsSessionManager()
    rate_limiter = AdvancedRateLimiter(MAX_REQUESTS_PER_MINUTE)
    
    processed_count = 0
    skipped_count = 0
    failed_count = 0
    start_time = time.time()
    
    for idx, (cat_id, product_name, state) in enumerate(batch_products, start=start_idx):
        product_key = f"{cat_id}:{product_name}"
        
        if args.resume and tracker.is_completed(product_key):
            skipped_count += 1
            continue
        
        processed_count += 1
        progress = ((idx - start_idx + 1) / batch_size) * 100
        
        if session_mgr.should_refresh(processed_count):
            print(f"\n  🔄 Session refresh (product #{processed_count})...")
            session_mgr.get_session(force_new=True)
        
        print(f"\n{'─'*70}")
        print(f"  [{idx+1}/{end_idx}] ({progress:.1f}%)")
        print(f"  📦 {product_name}")
        print(f"  📁 {cat_id} | 📍 {state}")
        
        search_terms = extract_search_terms(product_name)
        print(f"  🔍 Term: {search_terms[0]}")
        
        old_demand = product_map.get(product_key, {}).get("demand", [0] * 12)
        
        raw = fetch_interest(session_mgr, search_terms, args.timeframe, 
                            args.geo, rate_limiter)
        
        if raw is None:
            print(f"  ❌ FAILED")
            failed_count += 1
            tracker.mark_failed(product_key, "API fetch failed")
            new_demand = old_demand
        else:
            new_demand = normalise_to_demand(raw)
            
            changes = sum(1 for o, n in zip(old_demand, new_demand) if o != n)
            if changes > 0:
                print(f"  📊 Changes: {changes} months updated")
            else:
                print(f"  ✅ Stable")
            
            print(f"  📈 Demand: {new_demand}")
            
            tracker.mark_completed(product_key, {
                "old": old_demand,
                "new": new_demand,
                "terms": search_terms,
                "category": cat_id,
                "state": state,
            })
        
        if idx < end_idx - 1:
            delay = rate_limiter.add_jitter(
                random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)
            )
            
            if (processed_count % LONG_DELAY_EVERY) == 0:
                long_delay = rate_limiter.add_jitter(LONG_DELAY_SECONDS)
                print(f"  ⏸️  Safety break ({long_delay:.0f}s)...")
                time.sleep(long_delay)
            else:
                time.sleep(delay)
    
    print(f"\n{'='*70}")
    print(f"  Summary: {processed_count} processed | {failed_count} failed")
    print(f"{'='*70}\n")
    
    if args.dry_run:
        print(f"  [dry-run] Would write {len(tracker.data['results'])} products")
        return
    
    out_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        current_data = json.loads(out_path.read_text(encoding="utf-8"))
    except:
        current_data = {
            "updated": datetime.date.today().isoformat(),
            "total_products": 0,
            "products": {},
            "batches": {}
        }
    
    for product_key, result in tracker.data["results"].items():
        current_data["products"][product_key] = {
            "demand": result["new"],
            "terms": result["terms"],
            "category": result["category"],
            "state": result["state"],
        }
    
    current_data["updated"] = datetime.date.today().isoformat()
    current_data["total_products"] = len(current_data["products"])
    current_data["batches"][f"batch_{batch_num if batch_num > 0 else 'custom'}"] = {
        "completed": datetime.datetime.now().isoformat(),
        "range": f"{start_idx}-{end_idx}",
        "count": processed_count,
        "failed": failed_count,
    }
    
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(current_data, f, indent=2)
        f.write("\n")
    
    print(f"  ✅ Wrote {len(current_data['products'])} products to {out_path}\n")


if __name__ == "__main__":
    main()
