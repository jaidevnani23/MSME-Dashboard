#!/usr/bin/env python3
"""
patch_demand.py — Safe Multi-Day Product Demand Fetching
=========================================================
Fetches Google Trends data for 374 products with semi-specific search terms
optimized for relevance and search volume. Enhanced noise removal ensures
clean, relevant search queries.

Usage:
    python patch_demand.py --batch 1
    python patch_demand.py --resume --batch 2
    python patch_demand.py --status
"""

import argparse
import json
import pathlib
import sys
import time
import datetime
import random
from datetime import datetime as dt, timedelta
from typing import Dict, List, Tuple, Optional

try:
    from pytrends.request import TrendReq
except ImportError:
    sys.exit(
        "pytrends not found. Run: pip install pytrends --break-system-packages"
    )

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

PRODUCTS_PER_BATCH = 94
MIN_DELAY_SECONDS = 3.0
MAX_DELAY_SECONDS = 5.0
RETRY_ATTEMPTS = 2
RETRY_DELAY = 10.0
OUTPUT_DIR = pathlib.Path("demand_data")
CHECKPOINT_FILE = OUTPUT_DIR / "checkpoint.json"

# ══════════════════════════════════════════════════════════════════════════════
# NOISE REMOVAL CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

# Comprehensive noise word removal for cleaner search terms
NOISE_WORDS = {
    # Articles and conjunctions
    "the", "a", "an", "and", "or", "of", "for", "with", "in", "at", "by", "from",
    
    # Geographic locations (too specific for trends)
    "jodhpur", "assam", "kerala", "lucknow", "banarasi", "kashmiri", "bengal",
    "mumbai", "delhi", "bangalore", "chennai", "kolkata", "hyderabad", "pune",
    "jaipur", "varanasi", "kanpur", "agra", "mysore", "madurai", "gujarat",
    
    # Religious/Cultural contexts
    "chhath", "puja", "pooja", "ritual", "festival", "diwali", "holi", "eid",
    "durga", "ganesh", "wedding", "bridal", "marriage",
    
    # Quality descriptors (too vague)
    "traditional", "handcrafted", "handmade", "authentic", "premium", "original",
    "genuine", "pure", "natural", "organic", "artisan", "artisans", "classic",
    "modern", "contemporary", "vintage", "antique", "luxury", "exclusive",
    
    # Certification markers
    "gi", "tagged", "certified", "registered", "protected",
    
    # Generic product terms
    "set", "sets", "piece", "pieces", "pack", "packs", "collection", "range",
    
    # Country/nationality
    "india", "indian", "desi", "bharat",
    
    # Punctuation artifacts
    "&", "—", "-", "(", ")", "[", "]", "{", "}", "/", "\\", "|",
}

# ══════════════════════════════════════════════════════════════════════════════
# REFINED PRODUCT-TO-SEARCH-TERM MAPPING
# ══════════════════════════════════════════════════════════════════════════════

PRODUCT_SEARCH_TERMS = {
    # Textiles & Clothing
    "saree": ["silk saree", "designer saree", "cotton saree"],
    "lehenga": ["lehenga choli", "party lehenga"],
    "kurta": ["kurta set", "mens kurta", "cotton kurta"],
    "salwar": ["salwar suit", "patiala suit", "punjabi suit"],
    "anarkali": ["anarkali suit", "anarkali dress"],
    "dupatta": ["dupatta design", "silk dupatta"],
    "pashmina": ["pashmina shawl", "cashmere shawl"],
    "shawl": ["wool shawl", "embroidered shawl"],
    "stole": ["silk stole", "woolen stole"],
    
    # Embroidery Styles
    "chikankari": ["chikankari kurti", "chikankari embroidery"],
    "phulkari": ["phulkari dupatta", "phulkari embroidery"],
    "kantha": ["kantha stitch", "kantha embroidery"],
    "zardozi": ["zardozi work", "zardozi embroidery"],
    
    # Saree Types
    "kasavu": ["kasavu saree", "kerala saree"],
    "kanchipuram": ["kanchipuram silk", "kanchi saree"],
    "tant": ["tant saree", "bengal cotton"],
    "patola": ["patola silk", "patola saree"],
    "ikat": ["ikat fabric", "ikat saree"],
    "bandhani": ["bandhani saree", "tie dye fabric"],
    
    # Fabrics
    "silk": ["silk fabric", "mulberry silk"],
    "cotton": ["cotton fabric", "pure cotton"],
    "khadi": ["khadi fabric", "khadi cotton"],
    "muslin": ["muslin fabric", "muslin cotton"],
    "linen": ["linen fabric", "linen cloth"],
    "wool": ["wool fabric", "merino wool"],
    "jute": ["jute fabric", "jute bags"],
    "bamboo": ["bamboo fabric", "bamboo fiber"],
    
    # Food & Spices
    "mango": ["alphonso mango", "kesar mango"],
    "tea": ["darjeeling tea", "assam tea", "green tea"],
    "coffee": ["filter coffee", "arabica coffee"],
    "honey": ["raw honey", "multiflora honey"],
    "turmeric": ["turmeric powder", "haldi"],
    "saffron": ["saffron threads", "kesar"],
    "jaggery": ["jaggery powder", "gur"],
    "millet": ["foxtail millet", "pearl millet", "ragi"],
    "rice": ["basmati rice", "brown rice"],
    "spice": ["spice mix", "garam masala"],
    "pickle": ["mango pickle", "lemon pickle"],
    "chutney": ["mint chutney", "coconut chutney"],
    
    # Handicrafts
    "basket": ["bamboo basket", "cane basket"],
    "mat": ["floor mat", "yoga mat"],
    "toy": ["wooden toys", "educational toys"],
    "pottery": ["ceramic pottery", "clay pots"],
    "terracotta": ["terracotta pots", "clay planters"],
    "carpet": ["wool carpet", "hand knotted rug"],
    "dhurrie": ["cotton dhurrie", "dhurrie rug"],
    
    # Home Decor
    "cushion": ["cushion covers", "throw pillows"],
    "curtain": ["cotton curtains", "door curtains"],
    "tablecloth": ["table runner", "table linen"],
    "towel": ["bath towel", "hand towel"],
    "bedsheet": ["cotton bedsheet", "bed linen"],
    "quilt": ["cotton quilt", "razai"],
    
    # Metalware
    "brass": ["brass utensils", "brass decor"],
    "copper": ["copper bottle", "copper utensils"],
    "bronze": ["bronze statue", "bronze items"],
    "bell": ["brass bell", "temple bell"],
    
    # Jewelry
    "jewelry": ["silver jewelry", "fashion jewelry"],
    "necklace": ["gold necklace", "beaded necklace"],
    "earrings": ["jhumka earrings", "gold earrings"],
    "bangle": ["gold bangles", "silver bangles"],
    "ring": ["silver ring", "gold ring"],
    
    # Art & Painting
    "painting": ["canvas painting", "wall art"],
    "madhubani": ["madhubani painting", "madhubani art"],
    "warli": ["warli painting", "warli art"],
    "pichwai": ["pichwai painting", "pichwai art"],
    
    # Leather Goods
    "leather": ["leather bag", "leather wallet"],
    "bag": ["leather bag", "handbag"],
    "wallet": ["leather wallet", "mens wallet"],
    "belt": ["leather belt", "dress belt"],
    
    # Furniture
    "chair": ["wooden chair", "dining chair"],
    "table": ["coffee table", "dining table"],
    "stool": ["bar stool", "wooden stool"],
    "cabinet": ["storage cabinet", "wooden cabinet"],
}

# ══════════════════════════════════════════════════════════════════════════════
# SEARCH TERM EXTRACTION LOGIC
# ══════════════════════════════════════════════════════════════════════════════

def clean_word(word: str) -> str:
    """Remove punctuation and normalize word."""
    return ''.join(c for c in word if c.isalnum()).lower()

def is_noise_word(word: str) -> bool:
    """Check if word is in noise list."""
    cleaned = clean_word(word)
    return cleaned in NOISE_WORDS or len(cleaned) <= 2

def extract_search_terms(product_name: str) -> List[str]:
    """
    Extract clean, relevant search terms for Google Trends.
    
    Strategy:
    1. Check for mapped terms (highest priority)
    2. Extract from parentheses (semi-specific context)
    3. Material + Category combination
    4. Smart noise removal fallback
    5. Last resort: single most meaningful word
    
    Args:
        product_name: Raw product name from dataset
        
    Returns:
        List of 1-3 search terms optimized for Google Trends
    """
    name_lower = product_name.lower()
    
    # ──────────────────────────────────────────────────────────────────────────
    # STEP 1: Direct Mapping (Highest Confidence)
    # ──────────────────────────────────────────────────────────────────────────
    for keyword, terms in sorted(
        PRODUCT_SEARCH_TERMS.items(), 
        key=lambda x: len(x[0]), 
        reverse=True
    ):
        if keyword in name_lower:
            return terms
    
    # ──────────────────────────────────────────────────────────────────────────
    # STEP 2: Parentheses Extraction (Generic Context)
    # ──────────────────────────────────────────────────────────────────────────
    if "(" in product_name and ")" in product_name:
        inner = product_name[
            product_name.find("(") + 1 : product_name.find(")")
        ].lower()
        inner_words = [w for w in inner.split() if not is_noise_word(w)]
        if inner_words:
            # Take first 2 meaningful words from parentheses
            return [" ".join(inner_words[:2])]
    
    # ──────────────────────────────────────────────────────────────────────────
    # STEP 3: Material + Category Combination
    # ──────────────────────────────────────────────────────────────────────────
    materials = [
        "silk", "cotton", "bamboo", "leather", "brass", "wooden", "wool", 
        "clay", "ceramic", "copper", "bronze", "jute", "cane", "terracotta",
        "muslin", "khadi", "linen", "silver", "gold"
    ]
    
    categories = [
        "saree", "basket", "bag", "toy", "towel", "suit", "painting", 
        "furniture", "honey", "rice", "jewelry", "carpet", "pottery",
        "shawl", "kurta", "dupatta", "cushion", "mat", "quilt", "wallet",
        "belt", "necklace", "earrings", "chair", "table"
    ]
    
    found_material = next((m for m in materials if m in name_lower), None)
    found_category = next((c for c in categories if c in name_lower), None)
    
    if found_material and found_category:
        return [f"{found_material} {found_category}"]
    
    if found_category:
        return [found_category]
    
    if found_material:
        # Try to find any category word
        words = [w for w in name_lower.split() if not is_noise_word(w)]
        if len(words) >= 2:
            return [f"{found_material} {words[-1]}"]
        return [found_material]
    
    # ──────────────────────────────────────────────────────────────────────────
    # STEP 4: Smart Noise Removal
    # ──────────────────────────────────────────────────────────────────────────
    # Remove all noise words and reconstruct
    words = [
        clean_word(w) 
        for w in name_lower.replace("(", " ").replace(")", " ").split() 
        if not is_noise_word(w)
    ]
    
    if len(words) >= 2:
        # Take first 2-3 meaningful words
        return [" ".join(words[:min(3, len(words))])]
    
    # ──────────────────────────────────────────────────────────────────────────
    # STEP 5: Last Resort - Single Most Meaningful Word
    # ──────────────────────────────────────────────────────────────────────────
    if words:
        return [words[0]]
    
    # Emergency fallback: first word of original name
    first_word = clean_word(product_name.split()[0]) if product_name else "product"
    return [first_word]

# ══════════════════════════════════════════════════════════════════════════════
# PYTRENDS INTEGRATION
# ══════════════════════════════════════════════════════════════════════════════

def init_pytrends() -> TrendReq:
    """Initialize pytrends with safe defaults."""
    return TrendReq(
        hl='en-US',
        tz=330,  # IST timezone
        timeout=(10, 25),
        retries=2,
        backoff_factor=0.5
    )

def fetch_trends_for_product(
    pytrends: TrendReq,
    product_name: str,
    search_terms: List[str],
    timeframe: str = "today 12-m"
) -> Optional[Dict]:
    """
    Fetch Google Trends data for a product.
    
    Args:
        pytrends: TrendReq instance
        product_name: Original product name
        search_terms: List of search terms to query
        timeframe: Google Trends timeframe string
        
    Returns:
        Dict with trends data or None if failed
    """
    for attempt in range(RETRY_ATTEMPTS):
        try:
            # Use first search term (most relevant)
            query = search_terms[0]
            
            pytrends.build_payload(
                kw_list=[query],
                cat=0,
                timeframe=timeframe,
                geo='IN',  # India
                gprop=''
            )
            
            # Get interest over time
            interest_df = pytrends.interest_over_time()
            
            if interest_df.empty:
                print(f"  ⚠️  No data for '{query}'")
                return None
            
            # Convert to serializable format
            data = {
                "product_name": product_name,
                "search_term": query,
                "alternative_terms": search_terms[1:] if len(search_terms) > 1 else [],
                "timeframe": timeframe,
                "data_points": interest_df[query].to_dict(),
                "average_interest": float(interest_df[query].mean()),
                "max_interest": float(interest_df[query].max()),
                "min_interest": float(interest_df[query].min()),
                "fetched_at": dt.now().isoformat()
            }
            
            print(f"  ✓ '{query}' (avg: {data['average_interest']:.1f})")
            return data
            
        except Exception as e:
            if attempt < RETRY_ATTEMPTS - 1:
                print(f"  ⚠️  Retry {attempt + 1}/{RETRY_ATTEMPTS} after error: {e}")
                time.sleep(RETRY_DELAY)
            else:
                print(f"  ✗ Failed after {RETRY_ATTEMPTS} attempts: {e}")
                return None
    
    return None

# ══════════════════════════════════════════════════════════════════════════════
# CHECKPOINT & BATCH MANAGEMENT
# ══════════════════════════════════════════════════════════════════════════════

def load_checkpoint() -> Dict:
    """Load checkpoint data if exists."""
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE, 'r') as f:
            return json.load(f)
    return {
        "completed_batches": [],
        "failed_products": [],
        "last_updated": None
    }

def save_checkpoint(checkpoint: Dict):
    """Save checkpoint data."""
    OUTPUT_DIR.mkdir(exist_ok=True)
    checkpoint["last_updated"] = dt.now().isoformat()
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(checkpoint, f, indent=2)

def save_batch_results(batch_num: int, results: List[Dict]):
    """Save batch results to JSON file."""
    OUTPUT_DIR.mkdir(exist_ok=True)
    output_file = OUTPUT_DIR / f"batch_{batch_num:02d}.json"
    
    with open(output_file, 'w') as f:
        json.dump({
            "batch_number": batch_num,
            "products_count": len(results),
            "fetched_at": dt.now().isoformat(),
            "results": results
        }, f, indent=2)
    
    print(f"\n💾 Saved {len(results)} products to {output_file}")

# ══════════════════════════════════════════════════════════════════════════════
# MAIN EXECUTION LOGIC
# ══════════════════════════════════════════════════════════════════════════════

def process_batch(
    batch_num: int,
    products: List[str],
    resume: bool = False
) -> Tuple[List[Dict], List[str]]:
    """
    Process a batch of products.
    
    Args:
        batch_num: Batch number
        products: List of product names
        resume: Whether this is a resumed session
        
    Returns:
        Tuple of (successful_results, failed_products)
    """
    print(f"\n{'='*70}")
    print(f"📦 BATCH {batch_num} — Processing {len(products)} products")
    print(f"{'='*70}\n")
    
    pytrends = init_pytrends()
    results = []
    failed = []
    
    for idx, product in enumerate(products, 1):
        print(f"[{idx}/{len(products)}] {product}")
        
        # Extract search terms
        search_terms = extract_search_terms(product)
        print(f"  → Terms: {', '.join(search_terms)}")
        
        # Fetch trends
        data = fetch_trends_for_product(pytrends, product, search_terms)
        
        if data:
            results.append(data)
        else:
            failed.append(product)
        
        # Rate limiting delay
        if idx < len(products):
            delay = random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)
            time.sleep(delay)
    
    return results, failed

def main():
    parser = argparse.ArgumentParser(
        description="Fetch Google Trends data for MSME products"
    )
    parser.add_argument(
        "--batch",
        type=int,
        choices=[1, 2, 3, 4],
        help="Batch number to process (1-4)"
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from checkpoint"
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show current progress status"
    )
    
    args = parser.parse_args()
    
    # Load checkpoint
    checkpoint = load_checkpoint()
    
    # Status check
    if args.status:
        print("\n📊 Current Status:")
        print(f"  Completed batches: {checkpoint['completed_batches']}")
        print(f"  Failed products: {len(checkpoint['failed_products'])}")
        if checkpoint['last_updated']:
            print(f"  Last updated: {checkpoint['last_updated']}")
        return
    
    # Validate batch argument
    if not args.batch:
        parser.error("--batch is required (use --status to check progress)")
    
    # Check if batch already completed
    if args.batch in checkpoint['completed_batches'] and not args.resume:
        print(f"⚠️  Batch {args.batch} already completed. Use --resume to reprocess.")
        return
    
    # Load your product list here
    # Example: products = load_products_from_file()
    # For now, using a placeholder
    all_products = [f"Product_{i}" for i in range(1, 375)]  # Replace with actual products
    
    # Calculate batch range
    start_idx = (args.batch - 1) * PRODUCTS_PER_BATCH
    end_idx = min(start_idx + PRODUCTS_PER_BATCH, len(all_products))
    batch_products = all_products[start_idx:end_idx]
    
    # Process batch
    results, failed = process_batch(args.batch, batch_products, args.resume)
    
    # Save results
    save_batch_results(args.batch, results)
    
    # Update checkpoint
    if args.batch not in checkpoint['completed_batches']:
        checkpoint['completed_batches'].append(args.batch)
    checkpoint['failed_products'].extend(failed)
    save_checkpoint(checkpoint)
    
    # Summary
    print(f"\n{'='*70}")
    print(f"✅ Batch {args.batch} Complete")
    print(f"{'='*70}")
    print(f"  Success: {len(results)}/{len(batch_products)}")
    print(f"  Failed: {len(failed)}")
    if failed:
        print(f"  Failed products: {', '.join(failed[:5])}")
        if len(failed) > 5:
            print(f"  ... and {len(failed) - 5} more")
    print()

if __name__ == "__main__":
    main()
