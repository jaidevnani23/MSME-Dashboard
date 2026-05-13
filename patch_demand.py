#!/usr/bin/env python3
"""
patch_demand_ultra_safe.py — Maximum Stealth Product Demand Fetching
=====================================================================
Ultra-conservative approach to avoid 429 errors completely.

Configuration (no time values changed):
- 20 products per batch
- 60–120 s between requests with ±30% jitter
- 30 s warm-up before first request
- 1 retry per product; 300 s back-off on 429
- 5-minute safety break every 3 products
- Circuit breaker after 2 consecutive 429s → 600 s cooldown
- Session refresh every 3 products
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

# ── Unbuffered print (kept for backward compat; logging is preferred internally)
print = functools.partial(print, flush=True)

try:
    from pytrends.request import TrendReq
except ImportError:
    sys.exit("pytrends not found.  Run: pip install pytrends --break-system-packages")

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("demand_fetcher.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ── Configuration (time values UNCHANGED) ────────────────────────────────────
PRODUCTS_PER_BATCH      = 20
MIN_DELAY_SECONDS       = 60.0
MAX_DELAY_SECONDS       = 120.0
INITIAL_WARMUP_DELAY    = 30.0
RETRY_ATTEMPTS          = 1
RETRY_DELAY             = 300.0
LONG_DELAY_EVERY        = 3
LONG_DELAY_SECONDS      = 300.0
MAX_REQUESTS_PER_MINUTE = 1
SESSION_REFRESH_EVERY   = 3
JITTER_FACTOR           = 0.3
CIRCUIT_BREAKER_THRESHOLD = 2
COOL_DOWN_PERIOD        = 600.0

USER_AGENTS: List[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]

# ── Product → search-term mapping ────────────────────────────────────────────
PRODUCT_SEARCH_TERMS: Dict[str, List[str]] = {
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

# Pre-sort by keyword length (longest first) so "mysore silk" beats "silk"
_SORTED_TERMS = sorted(PRODUCT_SEARCH_TERMS.items(), key=lambda kv: -len(kv[0]))

STOP_WORDS = frozenset({"the", "a", "an", "and", "or", "of", "for", "with", "in", "&"})


# ── Helpers ───────────────────────────────────────────────────────────────────

def jitter(base: float) -> float:
    """Apply ±JITTER_FACTOR noise to *base*, always returning ≥ 1 s."""
    offset = base * JITTER_FACTOR * (random.random() * 2 - 1)
    return max(1.0, base + offset)


def eta_str(seconds: float) -> str:
    """Format seconds into a human-readable HH:MM:SS string."""
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s   = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"


def extract_search_term(product_name: str) -> str:
    """
    Return the single best Google Trends search term for *product_name*.

    Strategy (in priority order):
    1. Longest matching keyword from PRODUCT_SEARCH_TERMS (avoids "silk"
       beating "mysore silk").
    2. First non-stop word longer than 2 characters.
    3. First word, lowercased.
    """
    lower = product_name.lower()
    for keyword, terms in _SORTED_TERMS:
        if keyword in lower:
            return terms[0]

    words = product_name.split()
    for word in words:
        clean = word.lower().strip("(),.-")
        if clean not in STOP_WORDS and len(clean) > 2:
            return clean

    return words[0].lower() if words else "product"


def normalise_to_demand(values: List[float]) -> List[int]:
    """
    Map raw Google Trends values (0–100) to demand buckets (0–4).

    Normalises against the period maximum first so the bucketing is
    relative to peak interest, not absolute search volume.
    """
    if not values:
        return [0] * 12
    peak = max(values) or 1.0
    normalised = [v / peak for v in values]

    def _bucket(v: float) -> int:
        if v >= 0.85: return 4
        if v >= 0.65: return 3
        if v >= 0.40: return 2
        if v >= 0.15: return 1
        return 0

    return [_bucket(v) for v in normalised]


# ── Data loading ──────────────────────────────────────────────────────────────

def load_products_from_dashboard(html_path: str) -> List[Tuple[str, str, str]]:
    """
    Parse ``PRODUCTS_BY_CAT`` from the dashboard HTML and return a flat list
    of ``(category_id, product_name, state)`` tuples.
    """
    path = pathlib.Path(html_path)
    if not path.exists():
        alt = pathlib.Path("/mnt/user-data/uploads") / html_path
        if alt.exists():
            path = alt
        else:
            log.error("Dashboard not found: %s", html_path)
            sys.exit(1)

    content = path.read_text(encoding="utf-8")
    marker  = "const PRODUCTS_BY_CAT = "
    start   = content.find(marker)
    if start == -1:
        log.error("Could not find PRODUCTS_BY_CAT in %s", path)
        sys.exit(1)

    # Walk forward to find the matching closing brace
    json_start  = start + len(marker)
    brace_depth = 0
    json_end    = json_start

    for i, ch in enumerate(content[json_start:], start=json_start):
        if ch == "{":
            brace_depth += 1
        elif ch == "}":
            brace_depth -= 1
            if brace_depth == 0:
                json_end = i + 1
                break
    else:
        log.error("Unmatched braces in PRODUCTS_BY_CAT")
        sys.exit(1)

    products_data: Dict = json.loads(content[json_start:json_end])

    products: List[Tuple[str, str, str]] = []
    for category, items in products_data.items():
        cat_id = category.lower().replace(" ", "_").replace("&", "and")
        for item in items:
            products.append((cat_id, item["Product"], item.get("State", "Unknown")))

    return products


# ── Rate limiting & circuit breaker ──────────────────────────────────────────

@dataclass
class RateLimiter:
    """
    Sliding-window rate limiter with an integrated circuit breaker.

    The window tracks the timestamps of recent requests; if the window
    is saturated the limiter sleeps until capacity is available again.
    The circuit breaker opens after CIRCUIT_BREAKER_THRESHOLD consecutive
    429 responses and forces a COOL_DOWN_PERIOD pause.
    """

    max_per_minute: int
    _request_times: List[float]     = field(default_factory=list, repr=False)
    _consecutive_429s: int          = field(default=0, repr=False)
    _circuit_open: bool             = field(default=False, repr=False)
    _circuit_opened_at: float       = field(default=0.0, repr=False)
    _is_first_request: bool         = field(default=True, repr=False)

    # ── public interface ──────────────────────────────────────────────────────

    def wait(self) -> None:
        """Block until it is safe to fire the next request."""
        self._handle_warmup()
        self._handle_circuit_breaker()
        self._handle_sliding_window()
        self._request_times.append(time.time())

    def record_429(self) -> None:
        self._consecutive_429s += 1
        log.warning("429 count: %d/%d", self._consecutive_429s, CIRCUIT_BREAKER_THRESHOLD)
        if self._consecutive_429s >= CIRCUIT_BREAKER_THRESHOLD:
            self._circuit_open      = True
            self._circuit_opened_at = time.time()
            log.error("Circuit breaker OPEN — entering %ds cooldown", COOL_DOWN_PERIOD)

    def record_success(self) -> None:
        self._consecutive_429s = 0

    # ── private helpers ───────────────────────────────────────────────────────

    def _handle_warmup(self) -> None:
        if self._is_first_request:
            delay = jitter(INITIAL_WARMUP_DELAY)
            log.info("Warm-up delay %.0fs (stealth)…", delay)
            time.sleep(delay)
            self._is_first_request = False

    def _handle_circuit_breaker(self) -> None:
        if not self._circuit_open:
            return
        elapsed   = time.time() - self._circuit_opened_at
        remaining = COOL_DOWN_PERIOD - elapsed
        if remaining > 0:
            log.warning("Circuit breaker cooling down — %.0fs remaining…", remaining)
            time.sleep(remaining)
        log.info("Circuit breaker CLOSED")
        self._circuit_open      = False
        self._consecutive_429s  = 0

    def _handle_sliding_window(self) -> None:
        now = time.time()
        # Drop timestamps older than 60 s
        self._request_times = [t for t in self._request_times if now - t < 60]

        if len(self._request_times) >= self.max_per_minute:
            oldest    = self._request_times[0]
            base_wait = 60.0 - (now - oldest)
            wait      = jitter(base_wait + random.uniform(10, 20))
            if wait > 0:
                log.info("Rate window full — waiting %.1fs…", wait)
                time.sleep(wait)
            self._request_times.clear()


# ── Session management ────────────────────────────────────────────────────────

class SessionManager:
    """Creates and rotates TrendReq sessions with randomised User-Agents."""

    def __init__(self) -> None:
        self._session: Optional[TrendReq] = None
        self._count   = 0

    @property
    def session_count(self) -> int:
        return self._count

    def get(self, force_new: bool = False) -> TrendReq:
        if force_new or self._session is None:
            self._create()
        return self._session  # type: ignore[return-value]

    def should_refresh(self, products_processed: int) -> bool:
        return products_processed > 0 and products_processed % SESSION_REFRESH_EVERY == 0

    def _create(self) -> None:
        ua = random.choice(USER_AGENTS)
        log.info("New Trends session #%d  UA: %.60s…", self._count + 1, ua)
        time.sleep(random.uniform(5, 12))
        self._session = TrendReq(
            hl="en-IN",
            tz=330,
            retries=0,
            backoff_factor=5.0,
            timeout=(15, 30),
            requests_args={
                "headers": {
                    "User-Agent":                ua,
                    "Accept-Language":           "en-IN,en;q=0.9",
                    "Accept":                    "text/html,application/xhtml+xml,"
                                                 "application/xml;q=0.9,image/webp,*/*;q=0.8",
                    "Accept-Encoding":           "gzip, deflate, br",
                    "DNT":                       "1",
                    "Connection":                "keep-alive",
                    "Upgrade-Insecure-Requests": "1",
                }
            },
        )
        self._count += 1


# ── Google Trends fetching ────────────────────────────────────────────────────

def fetch_monthly_interest(
    session_mgr:  SessionManager,
    term:         str,
    timeframe:    str,
    geo:          str,
    rate_limiter: RateLimiter,
    attempt:      int = 0,
) -> Optional[List[float]]:
    """
    Fetch 12 months of normalised (0.0–1.0) Google Trends interest for *term*.

    Returns ``None`` on permanent failure (all retries exhausted).
    The caller is responsible for converting the result to demand buckets.
    """
    log.info("Querying Trends: %r  (attempt %d)", term, attempt + 1)
    rate_limiter.wait()
    pytrends = session_mgr.get()

    try:
        pytrends.build_payload([term], timeframe=timeframe, geo=geo)
        df = pytrends.interest_over_time()
        log.info("Data received — %d rows", len(df))
        rate_limiter.record_success()

    except Exception as exc:
        return _handle_fetch_error(
            exc, session_mgr, term, timeframe, geo, rate_limiter, attempt
        )

    if df.empty:
        log.warning("Empty response for %r", term)
        return None

    if "isPartial" in df.columns:
        df = df.drop(columns=["isPartial"])

    df.index   = df.index.to_period("M")
    monthly    = df.groupby(df.index).mean()
    all_periods = sorted(monthly.index)

    # Pad to exactly 12 months (oldest months become 0)
    pad         = max(0, 12 - len(all_periods))
    raw_values  = [0.0] * pad + [float(monthly.loc[p].mean()) for p in all_periods[-12:]]
    return raw_values[-12:]  # guarantee exactly 12 values


def _handle_fetch_error(
    exc:          Exception,
    session_mgr:  SessionManager,
    term:         str,
    timeframe:    str,
    geo:          str,
    rate_limiter: RateLimiter,
    attempt:      int,
) -> Optional[List[float]]:
    """Classify the exception and decide whether to retry."""
    err = str(exc).lower()
    is_rate_limit = any(token in err for token in ("429", "too many", "rate"))

    if is_rate_limit:
        rate_limiter.record_429()

    if attempt >= RETRY_ATTEMPTS:
        log.error("Giving up on %r after %d attempt(s): %s", term, attempt + 1, exc)
        return None

    backoff = jitter(RETRY_DELAY if is_rate_limit else RETRY_DELAY / 2)
    log.warning("%s error — retrying in %.0fs: %s",
                "Rate-limit" if is_rate_limit else "API", backoff, exc)
    time.sleep(backoff)

    if is_rate_limit:
        session_mgr.get(force_new=True)

    return fetch_monthly_interest(
        session_mgr, term, timeframe, geo, rate_limiter, attempt + 1
    )


# ── Progress / checkpoint ─────────────────────────────────────────────────────

@dataclass
class ProgressTracker:
    """
    Persist per-batch progress to a JSON checkpoint so interrupted runs
    can be resumed with ``--resume``.
    """

    batch_num:       int
    _data:           Dict = field(default_factory=dict, repr=False)
    _checkpoint_path: pathlib.Path = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._checkpoint_path = pathlib.Path("data") / f"progress_batch_{self.batch_num}.json"
        self._data = self._load()

    # ── public interface ──────────────────────────────────────────────────────

    @property
    def results(self) -> Dict:
        return self._data["results"]

    def is_done(self, key: str) -> bool:
        return key in self._data["completed"]

    def mark_done(self, key: str, result: Dict) -> None:
        self._data["completed"].add(key)
        self._data["results"][key] = result
        self._save()

    def mark_failed(self, key: str, reason: str) -> None:
        self._data["failed"].append({
            "product":   key,
            "reason":    reason,
            "timestamp": datetime.datetime.now().isoformat(),
        })
        self._save()

    def completion_pct(self, total: int) -> float:
        return len(self._data["completed"]) / total * 100 if total else 0.0

    # ── persistence ───────────────────────────────────────────────────────────

    def _load(self) -> Dict:
        if self._checkpoint_path.exists():
            try:
                raw = json.loads(self._checkpoint_path.read_text(encoding="utf-8"))
                # Migrate legacy list → set for O(1) lookup
                raw["completed"] = set(raw.get("completed", []))
                raw.setdefault("failed",  [])
                raw.setdefault("results", {})
                return raw
            except Exception as exc:  # noqa: BLE001
                log.warning("Checkpoint unreadable (%s) — starting fresh", exc)

        return {
            "batch":     self.batch_num,
            "started":   datetime.datetime.now().isoformat(),
            "completed": set(),
            "failed":    [],
            "results":   {},
        }

    def _save(self) -> None:
        self._checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        serialisable = {**self._data, "completed": list(self._data["completed"])}
        self._checkpoint_path.write_text(
            json.dumps(serialisable, indent=2), encoding="utf-8"
        )


# ── Output helpers ────────────────────────────────────────────────────────────

def load_output(path: pathlib.Path) -> Dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {
            "updated":        datetime.date.today().isoformat(),
            "total_products": 0,
            "products":       {},
            "batches":        {},
        }


def write_output(path: pathlib.Path, data: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


# ── CLI ───────────────────────────────────────────────────────────────────────

def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Ultra-safe Google Trends demand fetcher",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--batch",      type=int,  help="Batch number (1-indexed)")
    p.add_argument("--start",      type=int,  help="Custom start index (0-indexed, inclusive)")
    p.add_argument("--end",        type=int,  help="Custom end index (0-indexed, exclusive)")
    p.add_argument("--timeframe",  default="today 12-m")
    p.add_argument("--geo",        default="IN")
    p.add_argument("--dashboard",  default="index.html")
    p.add_argument("--dry-run",    action="store_true",
                   help="Parse & plan without making any API calls")
    p.add_argument("--resume",     action="store_true",
                   help="Skip products already in the checkpoint")
    p.add_argument("--list-batches", action="store_true",
                   help="Print batch summary and exit")
    return p


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    args = build_arg_parser().parse_args()

    all_products   = load_products_from_dashboard(args.dashboard)
    total_products = len(all_products)
    total_batches  = (total_products + PRODUCTS_PER_BATCH - 1) // PRODUCTS_PER_BATCH

    # ── --list-batches ──────────────────────────────────────────────────────
    if args.list_batches:
        print(f"\n{'─'*55}")
        print(f"  {'Batch':>6}  {'Range':>12}  {'Count':>6}  {'Status'}")
        print(f"{'─'*55}")
        out_path = pathlib.Path("data/demand_products.json")
        existing = load_output(out_path)
        for b in range(1, total_batches + 1):
            s, e    = (b - 1) * PRODUCTS_PER_BATCH, min(b * PRODUCTS_PER_BATCH, total_products)
            key     = f"batch_{b}"
            meta    = existing.get("batches", {}).get(key)
            status  = f"✅ {meta['completed'][:10]}" if meta else "⏳ pending"
            print(f"  {b:>6}  {s:>6}–{e:<5}  {e-s:>6}  {status}")
        print(f"{'─'*55}")
        print(f"  Total: {total_products} products across {total_batches} batches\n")
        return

    # ── Resolve batch range ─────────────────────────────────────────────────
    if args.start is not None and args.end is not None:
        start_idx, end_idx, batch_num = args.start, args.end, 0
    elif args.batch:
        batch_num = args.batch
        if batch_num > total_batches:
            log.error("Batch %d exceeds total (%d)", batch_num, total_batches)
            sys.exit(1)
        start_idx = (batch_num - 1) * PRODUCTS_PER_BATCH
        end_idx   = min(start_idx + PRODUCTS_PER_BATCH, total_products)
    else:
        log.error("Specify --batch N  or  --start X --end Y")
        sys.exit(1)

    batch_products = all_products[start_idx:end_idx]
    batch_size     = len(batch_products)

    avg_delay    = (MIN_DELAY_SECONDS + MAX_DELAY_SECONDS) / 2
    break_count  = batch_size // LONG_DELAY_EVERY
    est_seconds  = batch_size * avg_delay + break_count * LONG_DELAY_SECONDS

    print(f"\n{'='*70}")
    print(f"  ULTRA-SAFE Demand Fetcher  —  Batch {batch_num or 'custom'}")
    print(f"{'='*70}")
    print(f"  Products:  {batch_size}  (index {start_idx}–{end_idx-1})")
    print(f"  Delays:    {MIN_DELAY_SECONDS:.0f}–{MAX_DELAY_SECONDS:.0f}s  |  "
          f"warm-up {INITIAL_WARMUP_DELAY:.0f}s  |  break/{LONG_DELAY_EVERY} prods {LONG_DELAY_SECONDS:.0f}s")
    print(f"  Rate:      {MAX_REQUESTS_PER_MINUTE} req/min  |  "
          f"retry backoff {RETRY_DELAY:.0f}s  |  cooldown {COOL_DOWN_PERIOD:.0f}s")
    print(f"  Est. time: {eta_str(est_seconds)}  (≈{est_seconds/60:.0f} min)")
    print(f"{'='*70}\n")

    if args.dry_run:
        for i, (cat, name, state) in enumerate(batch_products, start=start_idx):
            term = extract_search_term(name)
            print(f"  [{i+1:>4}]  {name:<40}  term={term!r:<25}  cat={cat}  state={state}")
        print(f"\n  [dry-run] {batch_size} products would be queried.  No API calls made.\n")
        return

    tracker     = ProgressTracker(batch_num)
    out_path    = pathlib.Path("data/demand_products.json")
    product_map = load_output(out_path).get("products", {})

    session_mgr  = SessionManager()
    rate_limiter = RateLimiter(MAX_REQUESTS_PER_MINUTE)

    processed = skipped = failed = 0
    run_start = time.time()

    for idx, (cat_id, product_name, state) in enumerate(batch_products, start=start_idx):
        product_key = f"{cat_id}:{product_name}"

        if args.resume and tracker.is_done(product_key):
            skipped += 1
            continue

        processed += 1

        if session_mgr.should_refresh(processed):
            log.info("Refreshing session at product #%d", processed)
            session_mgr.get(force_new=True)

        # ── Progress header ──────────────────────────────────────────────
        pos_in_batch = idx - start_idx + 1
        pct          = pos_in_batch / batch_size * 100
        elapsed      = time.time() - run_start
        rate         = processed / elapsed if elapsed else 0
        remaining    = (batch_size - pos_in_batch) / rate if rate else 0

        print(f"\n{'─'*70}")
        print(f"  [{idx+1}/{end_idx}]  {pct:.1f}%  |  "
              f"elapsed {eta_str(elapsed)}  |  ETA {eta_str(remaining)}")
        print(f"  📦 {product_name}")
        print(f"  📁 {cat_id}  |  📍 {state}")

        term     = extract_search_term(product_name)
        old_demand = product_map.get(product_key, {}).get("demand", [0] * 12)
        print(f"  🔍 Term: {term!r}")

        raw = fetch_monthly_interest(
            session_mgr, term, args.timeframe, args.geo, rate_limiter
        )

        if raw is None:
            failed += 1
            new_demand = old_demand
            tracker.mark_failed(product_key, "API fetch failed")
            log.error("FAILED: %s", product_key)
        else:
            new_demand = normalise_to_demand(raw)
            changes    = sum(o != n for o, n in zip(old_demand, new_demand))
            status     = f"{changes} months updated" if changes else "stable"
            log.info("Demand %s  [%s]", new_demand, status)
            tracker.mark_done(product_key, {
                "old":      old_demand,
                "new":      new_demand,
                "term":     term,
                "category": cat_id,
                "state":    state,
            })

        # ── Inter-request delay ──────────────────────────────────────────
        is_last = idx == end_idx - 1
        if not is_last:
            if processed % LONG_DELAY_EVERY == 0:
                delay = jitter(LONG_DELAY_SECONDS)
                log.info("Safety break %.0fs…", delay)
            else:
                delay = jitter(random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS))
                log.info("Next request in %.1fs…", delay)
            time.sleep(delay)

    # ── Write output ─────────────────────────────────────────────────────────
    total_elapsed = time.time() - run_start
    print(f"\n{'='*70}")
    print(f"  Done  —  processed {processed}  |  skipped {skipped}  |  failed {failed}")
    print(f"  Total time: {eta_str(total_elapsed)}")
    print(f"{'='*70}\n")

    output = load_output(out_path)
    output["products"].update({
        key: {
            "demand":   result["new"],
            "term":     result["term"],
            "category": result["category"],
            "state":    result["state"],
        }
        for key, result in tracker.results.items()
    })
    output["updated"]        = datetime.date.today().isoformat()
    output["total_products"] = len(output["products"])
    output["batches"][f"batch_{batch_num or 'custom'}"] = {
        "completed": datetime.datetime.now().isoformat(),
        "range":     f"{start_idx}–{end_idx}",
        "count":     processed,
        "failed":    failed,
    }

    write_output(out_path, output)
    log.info("Wrote %d products to %s", len(output["products"]), out_path)


if __name__ == "__main__":
    main()
