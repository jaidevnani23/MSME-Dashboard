#!/usr/bin/env python3
"""
patch_demand_products.py — Product-Level Google Trends → India Logistics Dashboard
====================================================================================
Fetches 12-month India search interest for individual products within each category,
normalises to 0–4, then writes results to data/demand_products.json.

The dashboard reads data/demand_products.json at page load — commit + push this file
to trigger an automatic redeploy with fresh product-level demand scores.

Usage:
    pip install pytrends --break-system-packages
    python patch_demand_products.py                  # writes data/demand_products.json
    python patch_demand_products.py --dry-run        # prints JSON without writing
    python patch_demand_products.py --sample 5       # sample 5 products per category

Requirements: Python 3.8+, pytrends
"""

import argparse
import json
import pathlib
import sys
import time
import datetime
from datetime import datetime as dt

try:
    from pytrends.request import TrendReq
except ImportError:
    sys.exit(
        "pytrends not found.  Run:  pip install pytrends --break-system-packages"
    )


# ── Product-to-search-term mapping ───────────────────────────────────────────
# Maps product names/patterns to their best search terms for Google Trends
PRODUCT_SEARCH_TERMS = {
    # Clothing & Apparel
    "saree": ["saree online", "designer saree", "silk saree"],
    "lehenga": ["lehenga choli", "bridal lehenga", "designer lehenga"],
    "kurta": ["kurta set", "men kurta", "kurti online"],
    "salwar": ["salwar suit", "salwar kameez", "patiala suit"],
    "ethnic wear": ["ethnic wear", "indian ethnic wear", "festive wear"],
    "anarkali": ["anarkali suit", "anarkali dress"],
    "bandhgala": ["bandhgala kurta", "men bandhgala"],
    "sherwani": ["sherwani", "wedding sherwani"],
    "chaniya choli": ["chaniya choli", "navratri dress"],
    "phulkari": ["phulkari dupatta", "punjabi phulkari"],
    "chikankari": ["chikankari suit", "lucknowi chikankari"],
    "pashmina": ["pashmina shawl", "kashmiri pashmina"],
    
    # Fabrics
    "fabric": ["fabric wholesale", "cotton fabric", "silk fabric"],
    "georgette": ["georgette fabric", "georgette saree"],
    "chiffon": ["chiffon fabric", "chiffon dupatta"],
    "cotton": ["cotton fabric", "organic cotton"],
    "silk": ["silk fabric", "pure silk"],
    "synthetic": ["synthetic fabric", "polyester fabric"],
    
    # Apparel Manufacturing
    "t-shirt": ["t shirt export", "cotton t-shirt", "graphic tees"],
    "polo": ["polo shirt", "mens polo"],
    "sportswear": ["sportswear", "gym wear", "activewear"],
    "school uniform": ["school uniform", "school dress"],
    "knitwear": ["knitwear", "knitted garments"],
    "sweater": ["sweater", "woollen sweater", "winter wear"],
    
    # Food & Grocery
    "mango": ["alphonso mango", "mango pulp", "ratnagiri mango"],
    "spices": ["indian spices", "spice powder", "masala"],
    "coffee": ["filter coffee", "arabica coffee", "coffee beans"],
    "tea": ["darjeeling tea", "green tea", "assam tea"],
    "dry fruits": ["dry fruits", "almonds", "cashew nuts"],
    "honey": ["organic honey", "raw honey"],
    "cardamom": ["cardamom", "elaichi"],
    "turmeric": ["turmeric powder", "haldi"],
    
    # Handicrafts & Crafts
    "handicrafts": ["indian handicrafts", "handmade crafts"],
    "brass": ["brass diya", "brass handicrafts"],
    "pottery": ["blue pottery", "ceramic pottery"],
    "carpet": ["hand knotted carpet", "wool carpet"],
    "dhurrie": ["cotton dhurrie", "handloom rug"],
    "wooden": ["wooden handicrafts", "carved wood"],
    
    # Beauty & Cosmetics
    "cosmetics": ["cosmetics online", "makeup products"],
    "skincare": ["skincare products", "face cream"],
    "jewellery": ["imitation jewellery", "fashion jewellery"],
    "kundan": ["kundan jewellery", "kundan polki"],
    "leather bag": ["leather bag", "handbag"],
    "footwear": ["footwear online", "leather shoes"],
    "sandal": ["sandals", "ethnic footwear"],
    
    # Home & Utensils
    "tiles": ["ceramic tiles", "floor tiles"],
    "cookware": ["cookware set", "non stick cookware"],
    "utensils": ["kitchen utensils", "stainless steel"],
}

MONTH_NAMES = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]


# ── Extract search terms from product name ───────────────────────────────────
def extract_search_terms(product_name: str) -> list:
    """
    Extract relevant search terms for a product based on its name.
    Returns a list of 1-3 search terms optimized for Google Trends.
    """
    product_lower = product_name.lower()
    
    # Direct keyword matches
    matched_terms = []
    for keyword, terms in PRODUCT_SEARCH_TERMS.items():
        if keyword in product_lower:
            matched_terms.extend(terms)
            break
    
    # If we found matches, return them
    if matched_terms:
        return matched_terms[:3]  # Limit to 3 terms
    
    # Fallback: extract key product words
    # Remove common filler words and brackets content
    words = product_name.split()
    stop_words = {"the", "a", "an", "and", "or", "of", "for", "with", "in", "&", "—", "-"}
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
        return [" ".join(key_words)]
    
    # Ultimate fallback: first 2-3 words of product name
    return [" ".join(words[:2]).lower()]


# ── Trend fetching ────────────────────────────────────────────────────────────
def fetch_interest(pytrends: TrendReq, terms: list, timeframe: str, geo: str) -> list:
    """
    Returns a 12-element list of normalised monthly interest (0.0–1.0).
    Averages across all provided terms using chunked requests (max 5 per call).
    """
    all_monthly = []

    chunk_size = 5
    for i in range(0, len(terms), chunk_size):
        chunk = terms[i: i + chunk_size]
        try:
            pytrends.build_payload(chunk, timeframe=timeframe, geo=geo)
            df = pytrends.interest_over_time()
        except Exception as exc:
            print(f"    ⚠  Trends API error for {chunk}: {exc}")
            continue

        if df.empty:
            print(f"    ⚠  No data returned for {chunk}")
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
        return [0.0] * 12

    averaged = [sum(col) / len(col) for col in zip(*all_monthly)]
    return averaged


def normalise_to_demand(values: list) -> list:
    """
    Maps a 0.0–1.0 float list to integer demand scores 0–4.
    Bucket boundaries mirror the manual scoring used in the dashboard.
    """
    def bucket(v: float) -> int:
        if v >= 0.85: return 4
        if v >= 0.65: return 3
        if v >= 0.40: return 2
        if v >= 0.15: return 1
        return 0

    return [bucket(v) for v in values]


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Fetch Google Trends product-level demand data"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print the new demand arrays without writing to disk",
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
        "--sample", type=int, default=None,
        help="Sample N products per category for testing (default: all)",
    )
    args = parser.parse_args()

    # Load dashboard product structure from the HTML file
    # In production, you'd extract this from the actual dashboard
    # For now, we'll create a simplified structure based on the categories
    
    PRODUCT_GROUPS = {
        "clothing": [
            "Oversized graphic tees",
            "Anarkali suits & party-wear lehengas",
            "Men's bandhgala kurtas",
            "Diwali special sarees",
            "Chaniya choli sets",
            "Banarasi silk sarees",
            "Phulkari embroidered dupatta",
        ],
        "apparel_mfg": [
            "Export T-shirts & polo shirts",
            "Sportswear & compression tights",
            "Kids' cotton school uniforms",
            "Ludhiana acrylic sweaters",
        ],
        "fabric": [
            "Rayon georgette saree fabric",
            "Cotton knit fabric",
            "Silk sarees",
            "Synthetic fabric",
        ],
        "food": [
            "Alphonso mango pulp",
            "Chikmagalur arabica coffee beans",
            "Darjeeling tea",
            "Dry fruits wholesale",
            "Cardamom pods",
            "Turmeric powder",
        ],
        "crafts": [
            "Moradabad brass diyas",
            "Jaipur blue pottery",
            "Hand-knotted wool carpets",
            "Handicrafts wholesale",
        ],
        "beauty": [
            "Imitation jewellery",
            "Leather bags",
            "Cosmetics wholesale",
            "Footwear retail",
            "Mysore Sandal Soap",
        ],
        "home": [
            "Ceramic floor tiles",
            "Cookware sets",
            "Kitchen utensils",
        ],
    }

    # ── Load existing product demand if available ──
    out_path = pathlib.Path("data") / "demand_products.json"
    try:
        existing = json.loads(out_path.read_text(encoding="utf-8"))
        product_map = existing.get("products", {})
    except Exception:
        product_map = {}

    # ── Init pytrends ──
    pytrends = TrendReq(hl="en-IN", tz=330, retries=3, backoff_factor=1.5)

    print(f"\n{'─'*70}")
    print(f"  India Logistics Dashboard — Product-Level Demand Patcher")
    print(f"  Timeframe : {args.timeframe}  |  Geo : {args.geo}")
    print(f"  Output    : {out_path}")
    if args.sample:
        print(f"  Sample    : {args.sample} products per category")
    print(f"{'─'*70}\n")

    results = {}
    total_products = 0

    for group_id, products in PRODUCT_GROUPS.items():
        print(f"  [{group_id}]")
        
        # Sample if requested
        if args.sample:
            products = products[:args.sample]
        
        for product_name in products:
            total_products += 1
            
            # Generate search terms
            search_terms = extract_search_terms(product_name)
            print(f"    → {product_name}")
            print(f"      Terms: {', '.join(search_terms)}")
            
            # Get old demand if exists
            product_key = f"{group_id}:{product_name}"
            old_demand = product_map.get(product_key, {}).get("demand", [0] * 12)
            
            # Fetch new data
            raw = fetch_interest(pytrends, search_terms, args.timeframe, args.geo)
            new_demand = normalise_to_demand(raw)
            
            # Show changes
            diff_parts = []
            for mi, (old, new) in enumerate(zip(old_demand, new_demand)):
                if old != new:
                    arrow = "↑" if new > old else "↓"
                    diff_parts.append(f"{MONTH_NAMES[mi]}: {old}→{new}{arrow}")
            
            if diff_parts:
                print(f"      Changes: {', '.join(diff_parts)}")
            else:
                print(f"      No changes")
            
            print(f"      Demand: {new_demand}")
            
            # Store results
            if group_id not in results:
                results[group_id] = {}
            
            results[group_id][product_name] = {
                "old": old_demand,
                "new": new_demand,
                "terms": search_terms,
            }
            
            # Rate limiting
            time.sleep(2.0)
        
        print()

    # ── Dry run stops here ──
    if args.dry_run:
        print(f"\n  [dry-run] Would write {total_products} products to {out_path}")
        summary = {
            "updated": datetime.date.today().isoformat(),
            "total_products": total_products,
            "products": {
                f"{gid}:{pname}": {
                    "demand": data["new"],
                    "terms": data["terms"],
                }
                for gid, products in results.items()
                for pname, data in products.items()
            },
        }
        print(json.dumps(summary, indent=2))
        print()
        return

    # ── Write demand_products.json ──
    out_path.parent.mkdir(parents=True, exist_ok=True)

    output = {
        "updated": datetime.date.today().isoformat(),
        "total_products": total_products,
        "products": {
            f"{gid}:{pname}": {
                "demand": data["new"],
                "terms": data["terms"],
                "group": gid,
            }
            for gid, products in results.items()
            for pname, data in products.items()
        },
        "prev": {
            f"{gid}:{pname}": data["old"]
            for gid, products in results.items()
            for pname, data in products.items()
        },
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)
        f.write("\n")

    print(f"\n  ✅  Done. {total_products} products written to {out_path}")
    print(f"  Commit and push data/demand_products.json for Vercel to pick it up.\n")


if __name__ == "__main__":
    main()
