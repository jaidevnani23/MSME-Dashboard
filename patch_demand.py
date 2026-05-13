#!/usr/bin/env python3
"""
patch_demand.py — Ultra-Safe Product Demand Fetching with Real-Time Progress
===========================================================================
Fetches Google Trends data with immediate console output for CI/CD monitoring.
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

# Force unbuffered output for real-time logging in GitHub Actions
import functools
print = functools.partial(print, flush=True)

try:
    from pytrends.request import TrendReq
except ImportError:
    sys.exit(
        "pytrends not found.  Run:  pip install pytrends --break-system-packages"
    )


# ── Configuration ─────────────────────────────────────────────────────────────
PRODUCTS_PER_BATCH = 50      
MIN_DELAY_SECONDS = 8.0      
MAX_DELAY_SECONDS = 15.0     
RETRY_ATTEMPTS = 3
RETRY_DELAY = 30.0           
LONG_DELAY_EVERY = 10        
LONG_DELAY_SECONDS = 45.0    
EXPONENTIAL_BACKOFF = True   
MAX_REQUESTS_PER_MINUTE = 4  

MONTH_NAMES = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]

# ── Product-to-search-term mapping ───────────────────────────────────────────
PRODUCT_SEARCH_TERMS = {
    # Clothing & Apparel
    "saree": ["saree", "silk saree"],
    "lehenga": ["lehenga", "bridal lehenga"],
    "kurta": ["kurta", "kurti"],
    "salwar": ["salwar suit", "salwar kameez"],
    "ethnic wear": ["ethnic wear", "indian wear"],
    "anarkali": ["anarkali", "anarkali suit"],
    "bandhgala": ["bandhgala", "nehru jacket"],
    "sherwani": ["sherwani", "wedding sherwani"],
    "chaniya choli": ["chaniya choli", "ghagra choli"],
    "phulkari": ["phulkari", "punjabi suit"],
    "chikankari": ["chikankari", "lucknow embroidery"],
    "pashmina": ["pashmina", "shawl"],
    "tant": ["tant saree", "bengal cotton"],
    "baluchari": ["baluchari saree", "silk saree"],
    "jamdani": ["jamdani", "muslin saree"],
    "kasavu": ["kasavu saree", "kerala saree"],
    "dhuti": ["dhoti", "traditional wear"],
    "mundu": ["mundu", "kerala dress"],
    "mekhela": ["mekhela chador", "assamese dress"],
    
    # Fabrics
    "fabric": ["fabric", "cotton fabric"],
    "georgette": ["georgette", "georgette fabric"],
    "chiffon": ["chiffon", "chiffon fabric"],
    "cotton": ["cotton fabric", "cotton"],
    "silk": ["silk fabric", "silk"],
    "synthetic": ["synthetic fabric", "polyester"],
    "rayon": ["rayon", "rayon fabric"],
    "kanchipuram": ["kanchipuram silk", "kanjeevaram"],
    "mysore silk": ["mysore silk", "silk saree"],
    "pochampally": ["pochampally", "ikat"],
    "chanderi": ["chanderi", "chanderi saree"],
    "maheshwari": ["maheshwari", "handloom saree"],
    
    # Apparel Manufacturing
    "t-shirt": ["t shirt", "tshirt"],
    "polo": ["polo shirt", "polo tshirt"],
    "sportswear": ["sportswear", "activewear"],
    "school uniform": ["school uniform", "uniform"],
    "knitwear": ["knitwear", "sweater"],
    "sweater": ["sweater", "winter wear"],
    
    # Food & Grocery
    "mango": ["mango", "alphonso"],
    "spices": ["spices", "masala"],
    "coffee": ["coffee", "coffee beans"],
    "tea": ["tea", "chai"],
    "dry fruits": ["dry fruits", "nuts"],
    "honey": ["honey", "organic honey"],
    "cardamom": ["cardamom", "elaichi"],
    "turmeric": ["turmeric", "haldi"],
    "guava": ["guava", "fruit"],
    "orange": ["orange", "citrus"],
    "jaggery": ["jaggery", "gur"],
    
    # Handicrafts & Crafts
    "handicrafts": ["handicrafts", "handmade"],
    "brass": ["brass", "brass items"],
    "pottery": ["pottery", "ceramic"],
    "carpet": ["carpet", "rug"],
    "dhurrie": ["dhurrie", "rug"],
    "wooden": ["wooden handicrafts", "wood craft"],
    "terracotta": ["terracotta", "clay"],
    "marble": ["marble", "marble craft"],
    
    # Beauty & Cosmetics
    "cosmetics": ["cosmetics", "makeup"],
    "skincare": ["skincare", "face cream"],
    "jewellery": ["jewellery", "jewelry"],
    "kundan": ["kundan", "polki"],
    "leather bag": ["leather bag", "bag"],
    "footwear": ["footwear", "shoes"],
    "sandal": ["sandals", "footwear"],
    "serum": ["serum", "face serum"],
    "sunscreen": ["sunscreen", "sunblock"],
    
    # Home & Utensils
    "tiles": ["tiles", "ceramic tiles"],
    "cookware": ["cookware", "utensils"],
    "utensils": ["utensils", "cookware"],
    "towel": ["towel", "bath towel"],
}


# ── Rate Limiter ─────────────────────────────────────────────────────────────
class RateLimiter:
    """Enforces strict rate limiting to avoid 429 errors"""
    
    def __init__(self, max_per_minute: int):
        self.max_per_minute = max_per_minute
        self.request_times = []
    
    def wait_if_needed(self):
        """Wait if we've hit the rate limit"""
        now = time.time()
        
        # Remove requests older than 1 minute
        self.request_times = [t for t in self.request_times if now - t < 60]
        
        # If we're at the limit, wait
        if len(self.request_times) >= self.max_per_minute:
            oldest = self.request_times[0]
            wait_time = 60 - (now - oldest) + random.uniform(1, 5)
            if wait_time > 0:
                print(f"    ⏳ Rate limit: waiting {wait_time:.1f}s...")
                time.sleep(wait_time)
                self.request_times = []
        
        # Record this request
        self.request_times.append(time.time())


# ── Dashboard Products Extraction ────────────────────────────────────────────
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


# ── Search Term Extraction ───────────────────────────────────────────────────
def extract_search_terms(product_name: str) -> List[str]:
    """Extract relevant search terms for a product."""
    product_lower = product_name.lower()
    
    matched_terms = []
    for keyword, terms in PRODUCT_SEARCH_TERMS.items():
        if keyword in product_lower:
            matched_terms.extend(terms)
            break
    
    if matched_terms:
        return matched_terms[:2]
    
    words = product_name.split()
    stop_words = {
        "the", "a", "an", "and", "or", "of", "for", "with", "in", "&", 
        "—", "-", "set", "sets", "pack", "piece", "pieces", "online",
        "wholesale", "export", "bulk"
    }
    key_words = []
    
    for word in words:
        if "(" in word or "—" in word:
            break
        clean_word = word.lower().strip("(),.")
        if clean_word not in stop_words and len(clean_word) > 2:
            key_words.append(clean_word)
        if len(key_words) >= 2:
            break
    
    if key_words:
        return [" ".join(key_words)]
    
    fallback_words = [w for w in words[:3] if w.lower() not in stop_words]
    if fallback_words:
        return [" ".join(fallback_words[:2]).lower()]
    
    return [words[0].lower()] if words else ["product"]


# ── Trend Fetching (FIXED VERSION) ───────────────────────────────────────────
def fetch_interest(pytrends: TrendReq, terms: List[str], timeframe: str, 
                   geo: str, rate_limiter: RateLimiter, 
                   retry_count: int = 0) -> Optional[List[float]]:
    """
    Returns a 12-element list of normalised monthly interest (0.0–1.0).
    FIXED: Properly maintains chronological month ordering.
    """
    all_monthly = []
    chunk_size = 5
    
    for i in range(0, len(terms), chunk_size):
        chunk = terms[i: i + chunk_size]
        
        print(f"      🔍 Querying Google Trends for: {', '.join(chunk)}")
        
        rate_limiter.wait_if_needed()
        
        try:
            pytrends.build_payload(chunk, timeframe=timeframe, geo=geo)
            df = pytrends.interest_over_time()
            print(f"      ✓ Data received ({len(df)} rows)")
        except Exception as exc:
            error_str = str(exc).lower()
            
            if '429' in error_str or 'too many' in error_str:
                if retry_count < RETRY_ATTEMPTS:
                    if EXPONENTIAL_BACKOFF:
                        backoff_delay = RETRY_DELAY * (2 ** retry_count)
                    else:
                        backoff_delay = RETRY_DELAY
                    
                    print(f"      🚫 Rate limited (429), backing off {backoff_delay:.0f}s...")
                    time.sleep(backoff_delay)
                    return fetch_interest(pytrends, terms, timeframe, geo, rate_limiter, retry_count + 1)
                else:
                    print(f"      ❌ Rate limit exceeded after {RETRY_ATTEMPTS} retries")
                    return None
            else:
                if retry_count < RETRY_ATTEMPTS:
                    print(f"      ⚠️  API error, retrying in {RETRY_DELAY}s... ({exc})")
                    time.sleep(RETRY_DELAY)
                    return fetch_interest(pytrends, terms, timeframe, geo, rate_limiter, retry_count + 1)
                else:
                    print(f"      ❌ Failed after {RETRY_ATTEMPTS} retries: {exc}")
                    return None

        if df.empty:
            print(f"      ⚠️  No data returned for terms: {chunk}")
            continue

        if "isPartial" in df.columns:
            df = df.drop(columns=["isPartial"])

        df.index = df.index.to_period("M")
        monthly = df.groupby(df.index).mean()

        all_periods = sorted(monthly.index)
        
        # FIXED: Maintain chronological order
        if len(all_periods) >= 12:
            last_12_periods = all_periods[-12:]
            monthly_vals = [float(monthly.loc[p].mean()) for p in last_12_periods]
        else:
            monthly_vals = [0.0] * (12 - len(all_periods))
            for period in all_periods:
                monthly_vals.append(float(monthly.loc[period].mean()))

        mx = max(monthly_vals) or 1.0
        normalised = [v / mx for v in monthly_vals]
        all_monthly.append(normalised)

        time.sleep(2.0)

    if not all_monthly:
        return None

    averaged = [sum(col) / len(col) for col in zip(*all_monthly)]
    return averaged


def normalise_to_demand(values: List[float]) -> List[int]:
    """Maps 0.0–1.0 to demand scores 0–4."""
    def bucket(v: float) -> int:
        if v >= 0.85: return 4
        if v >= 0.65: return 3
        if v >= 0.40: return 2
        if v >= 0.15: return 1
        return 0
    return [bucket(v) for v in values]


# ── Progress Tracking ────────────────────────────────────────────────────────
class ProgressTracker:
    """Tracks progress and allows resume capability"""
    
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
    """Calculate total number of batches needed"""
    return (total_products + PRODUCTS_PER_BATCH - 1) // PRODUCTS_PER_BATCH


# ── Main Execution ───────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Fetch Google Trends product-level demand data"
    )
    parser.add_argument("--batch", type=int, help="Batch number (1-8 for 50 products per batch)")
    parser.add_argument("--start", type=int, help="Custom start index")
    parser.add_argument("--end", type=int, help="Custom end index")
    parser.add_argument("--dry-run", action="store_true", help="Print without writing")
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint")
    parser.add_argument("--timeframe", default="today 12-m", help="pytrends timeframe")
    parser.add_argument("--geo", default="IN", help="Google Trends geo code")
    parser.add_argument("--dashboard", default="index.html", help="Path to dashboard HTML")
    args = parser.parse_args()
    
    print(f"\n{'='*70}")
    print(f"  India Logistics Dashboard — Demand Data Updater")
    print(f"{'='*70}")
    print(f"  Started: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  Loading products from dashboard...")
    
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
    
    print(f"\n  📦 Batch {batch_num} Configuration:")
    print(f"     Range: Products {start_idx} to {end_idx-1}")
    print(f"     Count: {batch_size} products")
    print(f"     Timeframe: {args.timeframe}")
    print(f"     Geo: {args.geo}")
    
    avg_delay = (MIN_DELAY_SECONDS + MAX_DELAY_SECONDS) / 2
    extra_delays = (batch_size // LONG_DELAY_EVERY) * LONG_DELAY_SECONDS
    total_time_seconds = (batch_size * avg_delay) + extra_delays
    estimated_minutes = total_time_seconds / 60
    print(f"     Est. Time: {estimated_minutes:.1f} minutes")
    print(f"{'='*70}\n")
    
    tracker = ProgressTracker(batch_num if batch_num > 0 else 0)
    
    if args.resume and tracker.data["completed_products"]:
        completion = tracker.get_completion_rate(batch_size)
        print(f"  🔄 Resume Mode: {len(tracker.data['completed_products'])} completed ({completion:.1f}%)\n")
    
    out_path = pathlib.Path("data") / "demand_products.json"
    try:
        existing = json.loads(out_path.read_text(encoding="utf-8"))
        product_map = existing.get("products", {})
    except Exception:
        product_map = {}
    
    pytrends = TrendReq(hl="en-IN", tz=330, retries=2, backoff_factor=2.0)
    rate_limiter = RateLimiter(MAX_REQUESTS_PER_MINUTE)
    
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
        
        print(f"\n{'─'*70}")
        print(f"  [{idx+1}/{end_idx}] ({progress:.1f}%) Processing...")
        print(f"  📦 Product: {product_name}")
        print(f"  📁 Category: {cat_id} | 📍 State: {state}")
        
        search_terms = extract_search_terms(product_name)
        terms_display = ' + '.join(['"%s"' % t for t in search_terms])
        print(f"  🔍 Search Terms: {terms_display}")
        
        old_demand = product_map.get(product_key, {}).get("demand", [0] * 12)
        
        raw = fetch_interest(pytrends, search_terms, args.timeframe, args.geo, rate_limiter)
        
        if raw is None:
            print(f"  ❌ FAILED - No trends data")
            failed_count += 1
            tracker.mark_failed(product_key, "API fetch failed")
            new_demand = old_demand
        else:
            new_demand = normalise_to_demand(raw)
            
            diff_parts = []
            for mi, (old, new) in enumerate(zip(old_demand, new_demand)):
                if old != new:
                    arrow = "↑" if new > old else "↓"
                    diff_parts.append(f"{MONTH_NAMES[mi]}:{old}→{new}{arrow}")
            
            if diff_parts:
                print(f"  📊 Changes: {', '.join(diff_parts)}")
            else:
                print(f"  ✅ No changes (stable)")
            
            print(f"  📈 Demand: {new_demand}")
            
            result = {
                "old": old_demand,
                "new": new_demand,
                "terms": search_terms,
                "category": cat_id,
                "state": state,
            }
            tracker.mark_completed(product_key, result)
        
        elapsed = time.time() - start_time
        if processed_count > 0:
            avg_time = elapsed / processed_count
            remaining = batch_size - (idx - start_idx + 1) + skipped_count
            eta_minutes = (avg_time * remaining) / 60
            print(f"  ⏱️  ETA: {eta_minutes:.1f} minutes")
        
        if idx < end_idx - 1:
            delay = random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)
            
            if (processed_count % LONG_DELAY_EVERY) == 0:
                print(f"  ⏸️  Extra safety break ({LONG_DELAY_SECONDS}s)...")
                time.sleep(LONG_DELAY_SECONDS)
            
            time.sleep(delay)
    
    elapsed_total = time.time() - start_time
    
    print(f"\n{'='*70}")
    print(f"  Batch {batch_num} Summary")
    print(f"{'='*70}")
    print(f"   Processed: {processed_count} | Skipped: {skipped_count} | Failed: {failed_count}")
    print(f"   Time: {elapsed_total/60:.1f} minutes | Avg: {elapsed_total/max(processed_count,1):.1f}s/product")
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
    
    print(f"  ✅ Wrote {len(current_data['products'])} total products to {out_path}")
    
    if failed_count > 0:
        print(f"  ⚠️  {failed_count} failed. Rerun with --resume to retry.")
    
    remaining = total_batches - len([k for k in current_data.get("batches", {}).keys() if k.startswith("batch_")])
    if remaining > 0 and batch_num < total_batches:
        print(f"  📅 Next: python patch_demand.py --batch {batch_num + 1}")
    
    print()


if __name__ == "__main__":
    main()
