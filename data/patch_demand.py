#!/usr/bin/env python3
"""
patch_demand.py — Google Trends → India Logistics Dashboard demand patcher
===========================================================================
Fetches 12-month India search interest for each category group, normalises
to 0–4, then writes the results to data/demand.json.

Vercel reads data/demand.json at page load — commit + push this file to
trigger an automatic redeploy with fresh demand scores.

Usage:
    pip install pytrends --break-system-packages
    python patch_demand.py                  # writes data/demand.json
    python patch_demand.py --dry-run        # prints JSON without writing

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


# ── Search terms per group ────────────────────────────────────────────────────
GROUP_TERMS = {
    "clothing": [
        "ethnic wear",
        "saree wholesale",
        "lehenga choli",
        "festive wear",
    ],
    "apparel_mfg": [
        "garment manufacturing",
        "knitwear export",
        "tirupur garments",
        "school uniform supplier",
    ],
    "fabric": [
        "fabric wholesale india",
        "silk saree fabric",
        "handloom fabric",
        "synthetic fabric surat",
    ],
    "food": [
        "dry fruits wholesale",
        "spices wholesale india",
        "festive food hamper",
        "organic food india",
    ],
    "crafts": [
        "handicrafts wholesale india",
        "diwali decoration wholesale",
        "handmade gifts india",
        "pooja items wholesale",
    ],
    "beauty": [
        "cosmetics wholesale india",
        "imitation jewellery wholesale",
        "leather bags wholesale",
        "ayurvedic skincare",
    ],
    "home": [
        "cookware wholesale india",
        "home decor wholesale",
        "kitchen utensils india",
        "tiles wholesale morbi",
    ],
}

MONTH_NAMES = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]


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
        description="Fetch Google Trends demand data and write to data/demand.json"
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
    args = parser.parse_args()

    # ── Load existing demand.json as the "old" baseline ──
    out_path = pathlib.Path("data") / "demand.json"
    try:
        existing = json.loads(out_path.read_text(encoding="utf-8"))
        group_map = {
            gid: {"demand": vals}
            for gid, vals in existing.get("groups", {}).items()
        }
    except Exception:
        # First run or file missing — use zeros as baseline
        group_map = {gid: {"demand": [0] * 12} for gid in GROUP_TERMS}

    # ── Init pytrends ──
    pytrends = TrendReq(hl="en-IN", tz=330, retries=3, backoff_factor=1.5)

    print(f"\n{'─'*60}")
    print(f"  India Logistics Dashboard — Demand Patcher")
    print(f"  Timeframe : {args.timeframe}  |  Geo : {args.geo}")
    print(f"  Output    : {out_path}")
    print(f"{'─'*60}\n")

    results = {}

    for group_id, terms in GROUP_TERMS.items():
        old_demand = group_map.get(group_id, {}).get("demand", [0] * 12)
        print(f"  [{group_id}]  fetching {len(terms)} terms …")

        raw = fetch_interest(pytrends, terms, args.timeframe, args.geo)
        new_demand = normalise_to_demand(raw)

        # Show month-by-month diff
        diff_parts = []
        for mi, (old, new) in enumerate(zip(old_demand, new_demand)):
            if old != new:
                arrow = "↑" if new > old else "↓"
                diff_parts.append(f"{MONTH_NAMES[mi]}: {old}→{new}{arrow}")

        if diff_parts:
            print(f"    Changes : {', '.join(diff_parts)}")
        else:
            print(f"    No changes")

        print(f"    Old     : {old_demand}")
        print(f"    New     : {new_demand}")

        results[group_id] = {"old": old_demand, "new": new_demand}

        time.sleep(2.0)

    # ── Dry run stops here ──
    if args.dry_run:
        print("\n  [dry-run] Would write this to data/demand.json:")
        print(json.dumps({
            "updated": datetime.date.today().isoformat(),
            "groups": {gid: data["new"] for gid, data in results.items()},
            "prev":   {gid: data["old"] for gid, data in results.items()},
        }, indent=2))
        print()
        return

    # ── Write demand.json ──────────────────────────────────────────
    out_path.parent.mkdir(parents=True, exist_ok=True)

    output = {
        "updated": datetime.date.today().isoformat(),
        "groups": {gid: data["new"] for gid, data in results.items()},
        "prev":   {gid: data["old"] for gid, data in results.items()},
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)
        f.write("\n")

    print(f"\n  ✅  Done. Written to {out_path}")
    print(f"  Commit and push data/demand.json for Vercel to pick it up.\n")


if __name__ == "__main__":
    main()
