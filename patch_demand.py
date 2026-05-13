#!/usr/bin/env python3
"""
patch_demand.py — Ultra-Safe Product Demand Fetching with Aggressive Rate Limiting
==================================================================================
Fetches Google Trends data with extreme caution to avoid 429 errors and IP blocks.

STRATEGY: 25 products per batch with long delays and exponential backoff

Usage:
    python patch_demand.py --batch 1    # Products 0-24
    python patch_demand.py --batch 2    # Products 25-49
    etc.
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

try:
    from pytrends.request import TrendReq
except ImportError:
    sys.exit(
        "pytrends not found.  Run:  pip install pytrends --break-system-packages"
    )


# ── Configuration ─────────────────────────────────────────────────────────────
PRODUCTS_PER_BATCH = 25      # Reduced from 50 to 25 for ultra-safe operation
MIN_DELAY_SECONDS = 8.0      # Increased from 4.0 to 8.0
MAX_DELAY_SECONDS = 15.0     # Increased from 7.0 to 15.0
RETRY_ATTEMPTS = 3
RETRY_DELAY = 30.0           # Increased from 15.0 to 30.0 seconds
LONG_DELAY_EVERY = 5         # Reduced from 10 to 5 (more frequent breaks)
LONG_DELAY_SECONDS = 45.0    # Increased from 20.0 to 45.0 seconds
EXPONENTIAL_BACKOFF = True   # Enable exponential backoff on errors
MAX_REQUESTS_PER_MINUTE = 4  # Enforce strict rate limit

MONTH_NAMES = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]

# ── Product-to-search-term mapping (balanced for better trends data) ─────────
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
            wait_time = 60 - (now - oldest) + random.uniform(1, 5)  # Add jitter
            if wait_time > 0:
                print(f"    ⏳ Rate limit: waiting {wait_time:.1f}s...")
                time.sleep(wait_time)
                self.request_times = []  # Clear after waiting
        
        # Record this request
        self.request_times.append(time.time())


# ── Dashboard Products Extraction ────────────────────────────────────────────
def load_products_from_dashboard(html_path: str) -> List[Tuple[str, str, str]]:
    """
    Extract all products from the dashboard HTML file.
    Returns: List of (category_id, product_name, state) tuples
    """
    try:
        with open(html_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Find the PRODUCTS_BY_CAT object
        start = content.find('const PRODUCTS_BY_CAT = {')
        if start == -1:
            raise ValueError("Could not find PRODUCTS_BY_CAT in dashboard HTML")
        
        # Extract the JSON object (find matching closing brace)
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
        
        # Convert to flat list
        products = []
        for category, items in products_data.items():
            # Convert category name to ID (lowercase, replace spaces with underscores)
            cat_id = category.lower().replace(' ', '_').replace('&', 'and')
            for item in items:
                products.append((cat_id, item['Product'], item.get('State', 'Unknown')))
        
        return products
        
    except Exception as e:
        print(f"❌ Error loading products from dashboard: {e}")
        sys.exit(1)


# ── Search Term Extraction ───────────────────────────────────────────────────
def extract_search_terms(product_name: str) -> List[str]:
    """
    Extract relevant search terms for a product based on its name.
    Returns a list of 1-2 search terms optimized for Google Trends.
    Focuses on broader, cleaner terms that are more likely to have data.
    """
    product_lower = product_name.lower()
    
    # Direct keyword matches from our curated list
    matched_terms = []
    for keyword, terms in PRODUCT_SEARCH_TERMS.items():
        if keyword in product_lower:
            matched_terms.extend(terms)
            break
    
    # If we found matches, return them (limit to 2 for better data)
    if matched_terms:
        return matched_terms[:2]
    
    # Fallback: extract clean key product words
    words = product_name.split()
    stop_words = {
        "the", "a", "an", "and", "or", "of", "for", "with", "in", "&", 
        "—", "-", "set", "sets", "pack", "piece", "pieces", "online",
        "wholesale", "export", "bulk"
    }
    key_words = []
    
    for word in words:
        # Skip words in parentheses or after em-dashes
        if "(" in word or "—" in word:
            break
        clean_word = word.lower().strip("(),.")
        if clean_word not in stop_words and len(clean_word) > 2:
            key_words.append(clean_word)
        if len(key_words) >= 2:
            break
    
    # Create search term from key words
    if key_words:
        # Return single combined term (cleaner for trends)
        return [" ".join(key_words)]
    
    # Ultimate fallback: first 2 words of product name
    fallback_words = [w for w in words[:3] if w.lower() not in stop_words]
    if fallback_words:
        return [" ".join(fallback_words[:2]).lower()]
    
    # Last resort: just the first word
    return [words[0].lower()] if words else ["product"]


# ── Trend Fetching ───────────────────────────────────────────────────────────
def fetch_interest(pytrends: TrendReq, terms: List[str], timeframe: str, 
                   geo: str, rate_limiter: RateLimiter, 
                   retry_count: int = 0) -> Optional[List[float]]:
    """
    Returns a 12-element list of normalised monthly interest (0.0–1.0).
    Averages across all provided terms using chunked requests (max 5 per call).
    Returns None on failure after retries.
    
    FIXED: Properly handles month ordering regardless of data completeness.
    """
    all_monthly = []
    chunk_size = 5
    
    for i in range(0, len(terms), chunk_size):
        chunk = terms[i: i + chunk_size]
        
        # Enforce rate limiting before each request
        rate_limiter.wait_if_needed()
        
        try:
            pytrends.build_payload(chunk, timeframe=timeframe, geo=geo)
            df = pytrends.interest_over_time()
        except Exception as exc:
            error_str = str(exc).lower()
            
            # Check if it's a 429 error
            if '429' in error_str or 'too many' in error_str:
                if retry_count < RETRY_ATTEMPTS:
                    # Exponential backoff for 429 errors
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
                # Other errors
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

        # Convert to monthly periods
        df.index = df.index.to_period("M")
        monthly = df.groupby(df.index).mean()

        # Get all periods and ensure we have exactly 12 months
        all_periods = sorted(monthly.index)
        
        # FIXED: Always get the last 12 months chronologically
        if len(all_periods) >= 12:
            # Take the last 12 months
            last_12_periods = all_periods[-12:]
            monthly_vals = [float(monthly.loc[p].mean()) for p in last_12_periods]
        else:
            # If we have less than 12 months, pad with zeros at the start
            # This maintains chronological order
            monthly_vals = [0.0] * (12 - len(all_periods))
            for period in all_periods:
                monthly_vals.append(float(monthly.loc[period].mean()))

        # Normalize to 0.0-1.0
        mx = max(monthly_vals) or 1.0
        normalised = [v / mx for v in monthly_vals]
        all_monthly.append(normalised)

        # Delay between chunks within same product
        time.sleep(2.0)

    if not all_monthly:
        return None

    # Average across all search terms
    averaged = [sum(col) / len(col) for col in zip(*all_monthly)]
    return averaged
                       

def normalise_to_demand(values: List[float]) -> List[int]:
    """
    Maps a 0.0–1.0 float list to integer demand scores 0–4.
    """
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
        """Load existing progress or create new"""
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
        """Save progress to checkpoint file"""
        self.checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.checkpoint_file, 'w') as f:
            json.dump(self.data, f, indent=2)
    
    def is_completed(self, product_key: str) -> bool:
        """Check if product already processed"""
        return product_key in self.data["completed_products"]
    
    def mark_completed(self, product_key: str, result: Dict):
        """Mark product as completed with results"""
        if product_key not in self.data["completed_products"]:
            self.data["completed_products"].append(product_key)
        self.data["results"][product_key] = result
        self.save()
    
    def mark_failed(self, product_key: str, error: str):
        """Mark product as failed"""
        self.data["failed_products"].append({
            "product": product_key,
            "error": error,
            "timestamp": datetime.datetime.now().isoformat()
        })
        self.save()
    
    def get_completion_rate(self, total: int) -> float:
        """Get completion percentage"""
        return (len(self.data["completed_products"]) / total * 100) if total > 0 else 0


# ── Calculate Total Batches ──────────────────────────────────────────────────
def calculate_total_batches(total_products: int) -> int:
    """Calculate total number of batches needed"""
    return (total_products + PRODUCTS_PER_BATCH - 1) // PRODUCTS_PER_BATCH


# ── Main Execution ───────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Fetch Google Trends product-level demand data with ultra-safe rate limiting"
    )
    parser.add_argument(
        "--batch", type=int,
        help="Batch number (1-15 for 25 products per batch)",
    )
    parser.add_argument(
        "--start", type=int,
        help="Custom start index (overrides --batch)",
    )
    parser.add_argument(
        "--end", type=int,
        help="Custom end index (overrides --batch)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print the new demand arrays without writing to disk",
    )
    parser.add_argument(
        "--resume", action="store_true",
        help="Resume from checkpoint (skip already completed products)",
    )
    parser.add_argument(
        "--timeframe", default="today 12-m",
        help="pytrends timeframe string (default: 'today 12-m')",
    )
    parser.add_argument(
        "--geo", default="IN",
        help="Google Trends geo code (default: IN for India)",
    )
    parser.add_argument(
        "--dashboard", default="index.html",
        help="Path to dashboard HTML file",
    )
    args = parser.parse_args()
    
    # Load products from dashboard first to get total count
    print(f"\n{'─'*70}")
    print(f"  India Logistics Dashboard — Ultra-Safe Demand Patcher")
    print(f"{'─'*70}")
    print(f"  Loading products from dashboard...")
    
    dashboard_path = args.dashboard
    if not pathlib.Path(dashboard_path).exists():
        # Try in uploads folder
        dashboard_path = f"/mnt/user-data/uploads/{dashboard_path}"
    
    all_products = load_products_from_dashboard(dashboard_path)
    total_products = len(all_products)
    total_batches = calculate_total_batches(total_products)
    
    print(f"  ✅ Loaded {total_products} products from dashboard")
    print(f"  📊 Total batches needed: {total_batches} ({PRODUCTS_PER_BATCH} products per batch)")
    
    # Determine range
    if args.start is not None and args.end is not None:
        start_idx = args.start
        end_idx = args.end
        batch_num = 0  # Custom range
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
    
    # Show sample search term mappings
    print(f"\n  🔍 Search Term Strategy Examples:")
    print(f"     • 'saree' → \"saree\", \"silk saree\"")
    print(f"     • 't-shirt' → \"t shirt\", \"tshirt\"")
    print(f"     • 'mango' → \"mango\", \"alphonso\"")
    print(f"     • 'handicrafts' → \"handicrafts\", \"handmade\"")
    
    # Select batch
    batch_products = all_products[start_idx:end_idx]
    batch_size = len(batch_products)
    
    print(f"\n  📦 Batch Configuration:")
    if batch_num > 0:
        print(f"     Batch Number : {batch_num} of {total_batches}")
    print(f"     Range        : Products {start_idx} to {end_idx-1}")
    print(f"     Count        : {batch_size} products")
    print(f"     Timeframe    : {args.timeframe}")
    print(f"     Geo          : {args.geo}")
    
    # Estimate time with longer delays
    avg_delay = (MIN_DELAY_SECONDS + MAX_DELAY_SECONDS) / 2
    extra_delays = (batch_size // LONG_DELAY_EVERY) * LONG_DELAY_SECONDS
    total_time_seconds = (batch_size * avg_delay) + extra_delays
    estimated_minutes = total_time_seconds / 60
    print(f"     Est. Time    : {estimated_minutes:.1f} minutes ({estimated_minutes/60:.1f} hours)")
    print(f"     Safety       : 8-15s delay + 45s every 5 products + rate limiter")
    
    # Load progress tracker
    tracker = ProgressTracker(batch_num if batch_num > 0 else 0)
    
    if args.resume and tracker.data["completed_products"]:
        print(f"\n  🔄 Resume Mode:")
        completion = tracker.get_completion_rate(batch_size)
        print(f"     Already completed: {len(tracker.data['completed_products'])} products ({completion:.1f}%)")
        print(f"     Failed: {len(tracker.data['failed_products'])} products")
    
    print(f"{'─'*70}\n")
    
    # Load existing demand data
    out_path = pathlib.Path("data") / "demand_products.json"
    try:
        existing = json.loads(out_path.read_text(encoding="utf-8"))
        product_map = existing.get("products", {})
    except Exception:
        product_map = {}
    
    # Init pytrends with conservative settings
    pytrends = TrendReq(hl="en-IN", tz=330, retries=2, backoff_factor=2.0)
    
    # Init rate limiter
    rate_limiter = RateLimiter(MAX_REQUESTS_PER_MINUTE)
    
    # Process products
    results = {}
    processed_count = 0
    skipped_count = 0
    failed_count = 0
    
    start_time = time.time()
    
    for idx, (cat_id, product_name, state) in enumerate(batch_products, start=start_idx):
        product_key = f"{cat_id}:{product_name}"
        
        # Skip if already completed (resume mode)
        if args.resume and tracker.is_completed(product_key):
            skipped_count += 1
            continue
        
        processed_count += 1
        progress = ((idx - start_idx + 1) / batch_size) * 100
        
        print(f"\n  [{idx+1}/{end_idx}] ({progress:.1f}%) ──────────────────────────────────")
        print(f"    📦 Product: {product_name}")
        print(f"    📁 Category: {cat_id} | 📍 State: {state}")
        
        # Generate search terms
        search_terms = extract_search_terms(product_name)
        terms_display = ' + '.join([f'"{t}"' for t in search_terms])
        print(f"    🔍 Search Terms: {terms_display}")
        
        # Get old demand if exists
        old_demand = product_map.get(product_key, {}).get("demand", [0] * 12)
        
        # Fetch new data with rate limiter
        raw = fetch_interest(pytrends, search_terms, args.timeframe, args.geo, rate_limiter)
        
        if raw is None:
            print(f"    ❌ FAILED - No trends data returned")
            failed_count += 1
            tracker.mark_failed(product_key, "API fetch failed")
            new_demand = old_demand  # Keep old data on failure
        else:
            new_demand = normalise_to_demand(raw)
            
            # Show changes
            diff_parts = []
            for mi, (old, new) in enumerate(zip(old_demand, new_demand)):
                if old != new:
                    arrow = "↑" if new > old else "↓"
                    diff_parts.append(f"{MONTH_NAMES[mi]}:{old}→{new}{arrow}")
            
            if diff_parts:
                print(f"    📊 Changes: {', '.join(diff_parts)}")
            else:
                print(f"    ✅ No changes (stable demand)")
            
            print(f"    📈 Demand Array: {new_demand}")
            
            # Track result
            result = {
                "old": old_demand,
                "new": new_demand,
                "terms": search_terms,
                "category": cat_id,
                "state": state,
            }
            tracker.mark_completed(product_key, result)
        
        # Calculate ETA
        elapsed = time.time() - start_time
        if processed_count > 0:
            avg_time_per_product = elapsed / processed_count
            remaining = batch_size - (idx - start_idx + 1) + skipped_count
            eta_seconds = avg_time_per_product * remaining
            eta_minutes = eta_seconds / 60
            print(f"    ⏱️  ETA: {eta_minutes:.1f} minutes remaining")
        
        # Smart delay with extra safety
        if idx < end_idx - 1:  # Don't delay after last product
            # Regular delay with jitter
            delay = random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)
            
            # Add longer delay every N products to avoid patterns
            if (processed_count % LONG_DELAY_EVERY) == 0:
                print(f"    ⏸️  Extra safety break ({LONG_DELAY_SECONDS}s)...")
                time.sleep(LONG_DELAY_SECONDS)
            
            time.sleep(delay)
    
    # ── Summary ──────────────────────────────────────────────────────────────
    elapsed_total = time.time() - start_time
    
    print(f"{'─'*70}")
    print(f"  Batch {batch_num if batch_num > 0 else 'Custom'} Summary")
    print(f"{'─'*70}")
    print(f"   Processed : {processed_count} products")
    print(f"   Skipped   : {skipped_count} products (already completed)")
    print(f"   Failed    : {failed_count} products")
    print(f"   Time      : {elapsed_total/60:.1f} minutes")
    print(f"   Avg/Product: {elapsed_total/max(processed_count,1):.1f} seconds")
    
    completion = tracker.get_completion_rate(batch_size)
    print(f"   Progress  : {completion:.1f}% of batch complete")
    print(f"{'─'*70}\n")
    
    # ── Write Results ────────────────────────────────────────────────────────
    if args.dry_run:
        print(f"  [dry-run] Would merge {len(tracker.data['results'])} products into {out_path}")
        return
    
    # Merge with existing data
    out_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Load current file
    try:
        current_data = json.loads(out_path.read_text(encoding="utf-8"))
    except:
        current_data = {
            "updated": datetime.date.today().isoformat(),
            "total_products": 0,
            "products": {},
            "batches": {}
        }
    
    # Update with new results
    for product_key, result in tracker.data["results"].items():
        current_data["products"][product_key] = {
            "demand": result["new"],
            "terms": result["terms"],
            "category": result["category"],
            "state": result["state"],
        }
    
    # Update metadata
    current_data["updated"] = datetime.date.today().isoformat()
    current_data["total_products"] = len(current_data["products"])
    current_data["batches"][f"batch_{batch_num if batch_num > 0 else 'custom'}"] = {
        "completed": datetime.datetime.now().isoformat(),
        "range": f"{start_idx}-{end_idx}",
        "count": processed_count,
        "failed": failed_count,
    }
    
    # Write to file
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(current_data, f, indent=2)
        f.write("\n")
    
    print(f"  ✅ Success! {len(current_data['products'])} total products in {out_path}")
    print(f"  📊 Batch {batch_num if batch_num > 0 else 'Custom'} data merged successfully")
    print()
    
    if failed_count > 0:
        print(f"  ⚠️  {failed_count} products failed. Run again with --resume to retry.")
        print(f"  Check data/progress_batch_{batch_num if batch_num > 0 else 0}.json for details")
        print()
    
    # Next steps
    completed_batches = len([k for k in current_data.get("batches", {}).keys() if k.startswith("batch_")])
    remaining = total_batches - completed_batches
    
    if remaining > 0:
        print(f"  📅 Progress: {completed_batches}/{total_batches} batches complete")
        print(f"     {remaining} batches remaining")
        if batch_num < total_batches:
            print(f"     Next: python patch_demand.py --batch {batch_num + 1}")
    else:
        print(f"  🎉 All {total_batches} batches complete! Commit and push data/demand_products.json")
    
    print()


if __name__ == "__main__":
    main()
