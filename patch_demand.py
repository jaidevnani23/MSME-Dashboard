#!/usr/bin/env python3
"""
patch_demand.py — Safe Multi-Day Product Demand Fetching
=========================================================
Fetches Google Trends data for 374 products across multiple days with intelligent
rate limiting, progress tracking, resume capability, and batch management.

STRATEGY: Spreads 374 products over 4 days (93-94 products/day) with safe delays
to avoid IP blocks and rate limits from Google Trends' unofficial API.

Usage:
    pip install pytrends --break-system-packages
    
    # Run daily batches
    python patch_demand.py --batch 1    # Day 1: Products 0-93
    python patch_demand.py --batch 2    # Day 2: Products 94-187
    python patch_demand.py --batch 3    # Day 3: Products 188-280
    python patch_demand.py --batch 4    # Day 4: Products 281-374
    
    # Or specify custom range
    python patch_demand.py --start 0 --end 50
    
    # Test with dry-run
    python patch_demand.py --batch 1 --dry-run
    
    # Resume from failure
    python patch_demand.py --batch 2 --resume

Features:
- ✅ Batch processing (4 days for 374 products)
- ✅ Progress tracking with JSON checkpoint files
- ✅ Resume capability after failures or IP blocks
- ✅ Intelligent rate limiting (3-5 seconds between products)
- ✅ Error handling and retry logic
- ✅ Estimated completion time
- ✅ Daily summaries and change reports
- ✅ Balanced search terms for better data coverage

Requirements: Python 3.8+, pytrends
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
PRODUCTS_PER_BATCH = 94  # 374 ÷ 4 = 93.5, so use 94 for even distribution
MIN_DELAY_SECONDS = 3.0  # Minimum delay between products
MAX_DELAY_SECONDS = 5.0  # Maximum delay between products
RETRY_ATTEMPTS = 2       # Number of retries per product
RETRY_DELAY = 10.0       # Delay before retry (seconds)

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
        return matched_terms[:2]  # Reduced from 3 to 2
    
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
                   geo: str, retry_count: int = 0) -> Optional[List[float]]:
    """
    Returns a 12-element list of normalised monthly interest (0.0–1.0).
    Averages across all provided terms using chunked requests (max 5 per call).
    Returns None on failure after retries.
    """
    all_monthly = []
    chunk_size = 5
    
    for i in range(0, len(terms), chunk_size):
        chunk = terms[i: i + chunk_size]
        try:
            pytrends.build_payload(chunk, timeframe=timeframe, geo=geo)
            df = pytrends.interest_over_time()
        except Exception as exc:
            if retry_count < RETRY_ATTEMPTS:
                print(f"      ⚠️  API error, retrying in {RETRY_DELAY}s... ({exc})")
                time.sleep(RETRY_DELAY)
                return fetch_interest(pytrends, terms, timeframe, geo, retry_count + 1)
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
        if len(all_periods) < 12:
            monthly_vals = [0.0] * 12
            for period, row in monthly.iterrows():
                idx = period.month - 1
                monthly_vals[idx] = float(row.mean())
        else:
            last_12 = all_periods[-12:]
            monthly_vals = [float(monthly.loc[p].mean()) for p in last_12]

        mx = max(monthly_vals) or 1.0
        normalised = [v / mx for v in monthly_vals]
        all_monthly.append(normalised)

        time.sleep(1.2)

    if not all_monthly:
        return None

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


# ── Main Execution ───────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Fetch Google Trends product-level demand data in safe batches"
    )
    parser.add_argument(
        "--batch", type=int, choices=[1, 2, 3, 4],
        help="Batch number (1-4) for pre-defined ranges",
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
        "--dashboard", default="india_logistics_dashboard_v24.html",
        help="Path to dashboard HTML file",
    )
    args = parser.parse_args()
    
    # Determine range
    if args.start is not None and args.end is not None:
        start_idx = args.start
        end_idx = args.end
        batch_num = 0  # Custom range
    elif args.batch:
        batch_num = args.batch
        start_idx = (batch_num - 1) * PRODUCTS_PER_BATCH
        end_idx = min(start_idx + PRODUCTS_PER_BATCH, 374)
    else:
        print("❌ Error: Specify either --batch or both --start and --end")
        sys.exit(1)
    
    # Load products from dashboard
    print(f"\n{'─'*70}")
    print(f"  India Logistics Dashboard — Multi-Day Product Demand Patcher")
    print(f"{'─'*70}")
    print(f"  Loading products from dashboard...")
    
    dashboard_path = args.dashboard
    if not pathlib.Path(dashboard_path).exists():
        # Try in uploads folder
        dashboard_path = f"/mnt/user-data/uploads/{dashboard_path}"
    
    all_products = load_products_from_dashboard(dashboard_path)
    total_products = len(all_products)
    
    print(f"  ✅ Loaded {total_products} products from dashboard")
    
    # Show sample search term mappings
    print(f"\n  🔍 Search Term Strategy Examples:")
    print(f"     • 'saree' → \"saree\", \"silk saree\"")
    print(f"     • 't-shirt' → \"t shirt\", \"tshirt\"")
    print(f"     • 'mango' → \"mango\", \"alphonso\"")
    print(f"     • 'handicrafts' → \"handicrafts\", \"handmade\"")
    print(f"     (Balanced terms - not too specific, not too broad)")
    
    # Select batch
    batch_products = all_products[start_idx:end_idx]
    batch_size = len(batch_products)
    
    print(f"\n  📦 Batch Configuration:")
    if batch_num > 0:
        print(f"     Batch Number : {batch_num} of 4")
    print(f"     Range        : Products {start_idx} to {end_idx-1}")
    print(f"     Count        : {batch_size} products")
    print(f"     Timeframe    : {args.timeframe}")
    print(f"     Geo          : {args.geo}")
    
    # Estimate time
    avg_delay = (MIN_DELAY_SECONDS + MAX_DELAY_SECONDS) / 2
    estimated_minutes = (batch_size * avg_delay) / 60
    print(f"     Est. Time    : {estimated_minutes:.1f} minutes ({estimated_minutes/60:.1f} hours)")
    
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
    
    # Init pytrends
    pytrends = TrendReq(hl="en-IN", tz=330, retries=3, backoff_factor=1.5)
    
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
        print(f"    🔍 Search Terms: {' + '.join([f'"{t}"' for t in search_terms])}")
        
        # Get old demand if exists
        old_demand = product_map.get(product_key, {}).get("demand", [0] * 12)
        
        # Fetch new data
        raw = fetch_interest(pytrends, search_terms, args.timeframe, args.geo)
        
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
        
        # Smart delay: random jitter to avoid pattern detection
        if idx < end_idx - 1:  # Don't delay after last product
            delay = random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)
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
    remaining_batches = []
    for b in range(1, 5):
        batch_start = (b - 1) * PRODUCTS_PER_BATCH
        if batch_start >= len(current_data["products"]):
            remaining_batches.append(b)
    
    if remaining_batches:
        print(f"  📅 Next Steps:")
        print(f"     Run remaining batches: {', '.join(map(str, remaining_batches))}")
        print(f"     Example: python patch_demand.py --batch {remaining_batches[0]}")
    else:
        print(f"  🎉 All batches complete! Commit and push data/demand_products.json")
    
    print()


if __name__ == "__main__":
    main()
