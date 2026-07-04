"""Fetch resolved binary (Yes/No) Polymarket markets and record each market's
Yes and No price at six points relative to its resolution time: 6mo, 3mo, 1mo,
1wk, and 1day before resolution, plus at resolution. Writes Data/polymarket.csv.

Prices come from public Polymarket HTTP APIs (no auth):
  - Gamma  (market list + metadata): https://gamma-api.polymarket.com/markets
  - CLOB   (per-token price series):  https://clob.polymarket.com/prices-history

The realized winner comes from Gamma's settled `outcomePrices` (already in the
market-list response, so it costs nothing): ["1","0"] => Yes, ["0","1"] => No,
within GAMMA_SETTLE_TOL. Only when that vector is ambiguous do we fall back to
the authoritative on-chain Conditional Tokens Framework (CTF) payout vector on
Polygon, keyed by the market's conditionId, via web3.py.
"""

import argparse
import json
import os
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
from tqdm import tqdm
from web3 import Web3

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
GAMMA_URL = "https://gamma-api.polymarket.com/markets"
CLOB_URL = "https://clob.polymarket.com/prices-history"

# Public Polygon RPCs, tried in order (some endpoints rate-limit or go down).
POLYGON_RPCS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://1rpc.io/matic",
    "https://polygon.drpc.org",
]
# Gnosis Conditional Tokens contract on Polygon (Polymarket's CTF).
CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
CTF_ABI = [
    {"constant": True, "stateMutability": "view", "type": "function",
     "name": "payoutNumerators",
     "inputs": [{"name": "", "type": "bytes32"}, {"name": "", "type": "uint256"}],
     "outputs": [{"name": "", "type": "uint256"}]},
    {"constant": True, "stateMutability": "view", "type": "function",
     "name": "payoutDenominator",
     "inputs": [{"name": "", "type": "bytes32"}],
     "outputs": [{"name": "", "type": "uint256"}]},
]

MAX_MARKETS = 1000      # cap kept binary markets for a test run; set None for full pull
FLUSH_EVERY = 25      # checkpoint the CSV to disk every N processed markets
DERIVE_NO = True      # if True, fetch only the Yes token and set No = 1 - Yes
WORKERS = 6           # parallel per-market workers; total CLOB rate ~= WORKERS /
                      # (REQUEST_SLEEP + latency). Back off if 429s dominate.
PAGE_LIMIT = 500      # requested page size; Gamma usually caps pages at ~100
                      # (offset advances by rows returned, so this only affects
                      #  how many markets each request asks for, not coverage)
FIDELITY = 60         # fine price-history resolution (minutes); good for short markets
FIDELITY_DAILY = 1440 # coarse fallback (1 day); hourly returns empty for long markets
LONG_MARKET_DAYS = 14 # markets longer than this try daily fidelity first
REQUEST_SLEEP = 0.8   # seconds between HTTP calls *per thread* (429 backoff in
                      # get_json self-corrects if the combined rate is too hot)
RPC_SLEEP = 0.8       # seconds between on-chain calls (be polite to public RPCs)
GAMMA_SETTLE_TOL = 1e-3  # outcomePrices this close to a unit vector counts as settled
TIMEOUT = 25          # per-request timeout (seconds)
MAX_RETRIES = 5       # retries on network error / HTTP 429

_HERE = os.path.dirname(os.path.abspath(__file__))
_DATA_DIR = os.path.join(_HERE, "..", "Data")
CACHE_DIR = os.path.join(_DATA_DIR, "cache", "prices_history")
PAYOUT_CACHE_DIR = os.path.join(_DATA_DIR, "cache", "payouts")
OUT_CSV = os.path.join(_DATA_DIR, "polymarket.csv")

# (label, timedelta before T0). Order drives the CSV column order.
HORIZONS = [
    ("6mo", timedelta(days=182)),
    ("3mo", timedelta(days=91)),
    ("1mo", timedelta(days=30)),
    ("1wk", timedelta(days=7)),
    ("1day", timedelta(days=1)),
]
# Tolerance for the missing-horizon test: if the earliest history point is more
# than this much later than the target, the market didn't exist that far back.
NEAR_TOL = timedelta(hours=12)


# --------------------------------------------------------------------------- #
# HTTP helper
# --------------------------------------------------------------------------- #
def get_json(url, params):
    """GET with timeout, raise_for_status, and exponential backoff on network
    errors and HTTP 429. Sleeps REQUEST_SLEEP after each successful call to stay
    under Polymarket's undocumented read limit."""
    backoff = 1.0
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, params=params, timeout=TIMEOUT)
        except requests.RequestException:
            # Connection-level error (timeout, DNS, reset) -> retry with backoff.
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(backoff)
            backoff *= 2
            continue
        if resp.status_code == 429:
            # Rate limited: respect Retry-After when present, else backoff.
            wait = float(resp.headers.get("Retry-After", backoff))
            time.sleep(wait)
            backoff *= 2
            continue
        if resp.status_code >= 500:
            # Transient server error (Gamma throws occasional 500s) -> retry with
            # backoff. One of these mid-enumeration would otherwise kill a
            # multi-hour pull.
            if attempt == MAX_RETRIES - 1:
                resp.raise_for_status()
            time.sleep(backoff)
            backoff *= 2
            continue
        # Any other 4xx raises immediately: a client error like Gamma's 422 for
        # an out-of-range offset won't change on retry, so fail fast instead of
        # looping MAX_RETRIES times and then crashing.
        resp.raise_for_status()
        time.sleep(REQUEST_SLEEP)
        return resp.json()
    raise RuntimeError("unreachable")


# --------------------------------------------------------------------------- #
# Winner: Gamma settled outcomePrices, on-chain CTF payout vector as fallback
# --------------------------------------------------------------------------- #
def winner_from_gamma(outcome_prices):
    """Winner from Gamma's `outcomePrices` field (stringified JSON pair).

      1    -> Yes won   (prices ~ ["1","0"])
      0    -> No won    (prices ~ ["0","1"])
      None -> ambiguous -> caller must fall back to the on-chain payout vector

    CLOB-era markets settle to exactly ["1","0"]/["0","1"], but pre-2023
    AMM-era markets settle to the final pool price, which carries dust (e.g.
    0.9999994 / 0.0000006) — hence GAMMA_SETTLE_TOL instead of exact equality.
    A mislabel would need the LOSING side priced >= 1-tol after resolution,
    i.e. 999:1 free money nobody arbitraged; anything genuinely ambiguous
    (voided, split, mid-dispute) stays None and gets the authoritative check.
    """
    try:
        vals = outcome_prices if isinstance(outcome_prices, list) \
            else json.loads(outcome_prices or "[]")
        p = [float(x) for x in vals]
    except (json.JSONDecodeError, TypeError, ValueError):
        return None
    if len(p) != 2:
        return None
    if p[0] >= 1 - GAMMA_SETTLE_TOL and p[1] <= GAMMA_SETTLE_TOL:
        return 1  # Yes
    if p[1] >= 1 - GAMMA_SETTLE_TOL and p[0] <= GAMMA_SETTLE_TOL:
        return 0  # No
    return None


# Outcome labeling is the ground truth of the whole dataset; fail loudly at
# startup if the parse ever breaks.
assert winner_from_gamma('["1", "0"]') == 1
assert winner_from_gamma('["0.9999994", "0.0000006"]') == 1
assert winner_from_gamma('["0", "1"]') == 0
assert winner_from_gamma('["0.5", "0.5"]') is None
assert winner_from_gamma(None) is None and winner_from_gamma("junk") is None

_ctf_contract = None  # lazily-initialised, falls back across POLYGON_RPCS


def _get_ctf_contract():
    """Connect to the first responsive Polygon RPC and return the CTF contract."""
    global _ctf_contract
    if _ctf_contract is not None:
        return _ctf_contract
    addr = Web3.to_checksum_address(CTF_ADDRESS)
    for rpc in POLYGON_RPCS:
        try:
            w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": TIMEOUT}))
            if w3.is_connected():
                _ctf_contract = w3.eth.contract(address=addr, abi=CTF_ABI)
                tqdm.write(f"Connected to Polygon RPC: {rpc}")
                return _ctf_contract
        except Exception:
            continue
    raise RuntimeError("No responsive Polygon RPC in POLYGON_RPCS")


def get_winner(condition_id):
    """Return the realized winner for a market, read from the on-chain CTF
    payout vector and cached by conditionId.

      1  -> Yes won   (index 0 paid out)
      0  -> No won     (index 1 paid out)
      None -> unresolved / voided / not a clean binary payout (flag, don't guess)

    Payout numerators are scaled by payoutDenominator (e.g. 0 and 1e18), so a
    "payout of 1" means numerator == denominator. Index 0 maps to Yes and index
    1 to No, matching the Gamma outcomes order ["Yes", "No"].
    """
    cache_path = os.path.join(PAYOUT_CACHE_DIR, f"{condition_id}.json")
    if os.path.exists(cache_path):
        with open(cache_path) as f:
            data = json.load(f)
    else:
        contract = _get_ctf_contract()
        den = contract.functions.payoutDenominator(condition_id).call()
        n0 = contract.functions.payoutNumerators(condition_id, 0).call()
        n1 = contract.functions.payoutNumerators(condition_id, 1).call()
        time.sleep(RPC_SLEEP)
        data = {"denominator": den, "n0": n0, "n1": n1}
        with open(cache_path, "w") as f:
            json.dump(data, f)

    den, n0, n1 = data["denominator"], data["n0"], data["n1"]
    if den == 0:
        return None  # condition not resolved on-chain
    # Clean binary resolution: exactly one leg pays the full denominator.
    if n0 == den and n1 == 0:
        return 1  # Yes
    if n1 == den and n0 == 0:
        return 0  # No
    return None  # split / partial / voided -> flag rather than guess


# --------------------------------------------------------------------------- #
# Time parsing
# --------------------------------------------------------------------------- #
def parse_ts(value):
    """Parse a Polymarket timestamp into a tz-aware UTC datetime, or None.

    Handles both observed formats:
      closedTime: '2021-02-22 15:50:58+00'      (space-separated, 2-digit offset)
      endDate:    '2020-11-04T00:00:00Z'        (ISO 8601 with Z)
    """
    if not value:
        return None
    s = str(value).strip()
    s = s.replace("Z", "+00:00")
    s = s.replace(" ", "T", 1) if (" " in s and "T" not in s) else s
    # A bare 2-digit offset like '+00' must become '+00:00'.
    if len(s) >= 3 and s[-3] in "+-" and s[-3:].count(":") == 0:
        s = s + ":00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


# --------------------------------------------------------------------------- #
# Step 1 — enumerate resolved markets
# --------------------------------------------------------------------------- #
# Gamma rejects offset beyond ~2000 with HTTP 422, so plain offset pagination
# can only reach the newest ~2000 markets. We page with offset only up to this
# cap, then slide a date window (start_date_max) back to continue (keyset pagination).
OFFSET_CAP = 2000


def _emit_market(m, stats):
    """Filter one raw Gamma market to a binary Yes/No row dict, or return None.
    Updates stats counters."""
    stats["scanned"] += 1
    # outcomes / clobTokenIds arrive as stringified JSON.
    try:
        outcomes = json.loads(m.get("outcomes") or "[]")
        token_ids = json.loads(m.get("clobTokenIds") or "[]")
    except (json.JSONDecodeError, TypeError):
        return None
    # Binary filter: exactly ["Yes","No"] (any case) with two token ids.
    if [str(o).lower() for o in outcomes] != ["yes", "no"]:
        return None
    if len(token_ids) != 2 or not m.get("conditionId"):
        return None
    stats["kept"] += 1
    return {
        "question": m.get("question"),
        "yes_token": token_ids[0],  # token order matches outcome order
        "no_token": token_ids[1],
        "condition_id": m.get("conditionId"),
        "closed_time": m.get("closedTime"),
        "end_date": m.get("endDate"),
        "start_date": m.get("startDate"),  # used to pick price-history fidelity
        "category": m.get("category"),
        "outcome_prices": m.get("outcomePrices"),  # settled result, free winner lookup
    }


def iter_resolved_markets(stats):
    """Yield binary Yes/No markets one at a time across the *entire* closed-market
    history, newest startDate first. Updates stats['scanned'] / stats['kept'].

    Because Gamma caps offset at ~2000, we use keyset pagination: page by offset
    within a date window, and when we approach the offset cap slide the window's
    upper bound (start_date_max) back to the oldest startDate seen so far, then
    keep going. Markets are de-duplicated by id, since window boundaries overlap.

    The window key MUST be startDate, not endDate: startDates carry sub-second
    precision and are effectively unique, so the window always advances. Recurring
    hourly/daily markets share one exact endDate in clusters deeper than the
    offset cap, so an endDate window could never page past them — that stalled
    full pulls at ~10 months of history.
    """
    seen = set()
    window_max = None  # start_date_max bound; None => start at the newest markets
    while True:
        offset = 0
        oldest_start = None
        new_this_window = 0
        while True:
            params = {
                "closed": "true",
                "order": "startDate",
                "ascending": "false",
                "limit": PAGE_LIMIT,
                "offset": offset,
            }
            if window_max is not None:
                params["start_date_max"] = window_max
            page = get_json(GAMMA_URL, params)
            if not isinstance(page, list) or not page:
                break  # window exhausted
            for m in page:
                # startDate descending => the last item seen is the oldest so far.
                if m.get("startDate"):
                    oldest_start = m["startDate"]
                mid = m.get("id")
                if mid in seen:
                    continue  # overlap from a previous window
                seen.add(mid)
                # Count every new market, not just kept binary ones: a window of
                # new-but-non-binary markets is progress, not the end of history.
                new_this_window += 1
                row = _emit_market(m, stats)
                if row is not None:
                    yield row
                    if MAX_MARKETS is not None and stats["kept"] >= MAX_MARKETS:
                        return
            offset += len(page)  # Gamma returns ~100/page regardless of limit
            if offset >= OFFSET_CAP:
                break  # approaching the offset cap => slide the window instead
        tqdm.write(f"  ...scanned {stats['scanned']} markets, kept {stats['kept']} "
                   f"binary (window start_date_max={window_max})")
        # Stop when a whole window produced nothing new: end of history.
        if oldest_start is None or new_this_window == 0:
            break
        window_max = oldest_start  # slide the window back; overlap deduped via `seen`


# --------------------------------------------------------------------------- #
# Step 2 — resolution time (T0)
# --------------------------------------------------------------------------- #
def resolve_t0(market):
    """T0 = market resolution time. Prefer the explicit `closedTime` (when the
    market actually resolved); fall back to `endDate`. Returns a UTC datetime
    or None."""
    return parse_ts(market["closed_time"]) or parse_ts(market["end_date"])


# --------------------------------------------------------------------------- #
# Step 3 — price history per token (cached for resumability)
# --------------------------------------------------------------------------- #
def fetch_token_history(token_id, prefer_daily=False):
    """Return the prices-history list [{'t': unix_sec, 'p': price}, ...] for a
    token, cached to Data/cache/prices_history/{token_id}.json for resumability.

    The CLOB endpoint silently returns an EMPTY series when the requested
    fidelity is too fine for the market's span (e.g. hourly fidelity=60 over a
    months-long market yields nothing, while daily fidelity=1440 returns the full
    history). So we try one fidelity and fall back to the other if it's empty.
    `prefer_daily` chooses which to try first (set for long markets) to avoid a
    wasted request in the common case; the fallback still covers mis-guesses.
    """
    cache_path = os.path.join(CACHE_DIR, f"{token_id}.json")
    if os.path.exists(cache_path):
        with open(cache_path) as f:
            return json.load(f)
    fidelities = [FIDELITY_DAILY, FIDELITY] if prefer_daily else [FIDELITY, FIDELITY_DAILY]
    history = []
    for fid in fidelities:
        data = get_json(
            CLOB_URL, {"market": token_id, "interval": "max", "fidelity": fid}
        )
        history = data.get("history", [])
        if history:
            break  # got a usable series; no need to try the other fidelity
    with open(cache_path, "w") as f:
        json.dump(history, f)  # cache even an empty result (truly no data)
    return history


# --------------------------------------------------------------------------- #
# Step 4 — snapshot extraction (nearest available data point)
# --------------------------------------------------------------------------- #
def nearest_point(history, target_ts):
    """Return the price of the point whose timestamp is nearest (in absolute
    value) to target_ts, or None if history is empty."""
    if not history:
        return None
    nearest = min(history, key=lambda pt: abs(pt["t"] - target_ts))
    return nearest["p"]


def extract_snapshots(history, t0):
    """Given a token's history and resolution time T0, return:
      - dict {label: price_or_None} for each horizon plus 'resolution'
      - list of horizon labels deemed missing (market didn't exist that far back)

    A horizon is missing when the earliest available timestamp is later than the
    target time (beyond NEAR_TOL): the market simply didn't exist back then, so
    we leave the cell blank rather than snapping to an unrelated early point.
    """
    snaps = {}
    missing = []
    if not history:
        return snaps, [label for label, _ in HORIZONS]
    earliest_t = min(pt["t"] for pt in history)
    t0_ts = t0.timestamp()
    for label, delta in HORIZONS:
        target = (t0 - delta).timestamp()
        if earliest_t > target + NEAR_TOL.total_seconds():
            missing.append(label)
            snaps[label] = None
        else:
            # Nearest-point rule: pick the observation closest in time to target.
            snaps[label] = nearest_point(history, target)
    # "at resolution": the last (latest) point, nearest to T0.
    snaps["resolution"] = nearest_point(history, t0_ts)
    return snaps, missing


# --------------------------------------------------------------------------- #
# Step 5 — orchestrate + write CSV
# --------------------------------------------------------------------------- #
COLUMNS = [
    "question",
    "yes_6mo", "no_6mo", "yes_3mo", "no_3mo", "yes_1mo", "no_1mo",
    "yes_1wk", "no_1wk", "yes_1day", "no_1day", "yes_resolution", "no_resolution",
    "category", "outcome", "condition_id", "resolution_date", "missing_horizons",
]


def build_row(market, t0):
    """Fetch history, extract snapshots, look up the on-chain winner, and build a
    single output row. Returns (row_dict, all_horizons_present) or (None, False)
    when price history is empty/insufficient."""
    # Long markets need daily fidelity (hourly comes back empty); pick the likely
    # fidelity up front from the market's lifespan to avoid a wasted request.
    start = parse_ts(market.get("start_date"))
    prefer_daily = start is not None and (t0 - start).days > LONG_MARKET_DAYS

    yes_hist = fetch_token_history(market["yes_token"], prefer_daily)
    if not yes_hist:
        return None, False  # no usable price history => skip

    yes_snaps, yes_missing = extract_snapshots(yes_hist, t0)
    if yes_snaps.get("resolution") is None:
        return None, False

    if DERIVE_NO:
        # Derive No from Yes (No = 1 - Yes), halving CLOB calls.
        no_snaps = {k: (None if v is None else 1.0 - v) for k, v in yes_snaps.items()}
        missing = yes_missing
    else:
        no_hist = fetch_token_history(market["no_token"], prefer_daily)
        no_snaps, no_missing = extract_snapshots(no_hist, t0) if no_hist else ({}, [])
        order = [h[0] for h in HORIZONS]
        missing = sorted(set(yes_missing) | set(no_missing), key=order.index)

    # Realized winner: Gamma's settled outcomePrices (free, already fetched)
    # first; on-chain CTF payout vector only when Gamma is ambiguous. This
    # removes the 3 Polygon RPC calls per market that dominated run time.
    outcome = winner_from_gamma(market["outcome_prices"])
    if outcome is None:
        outcome = get_winner(market["condition_id"])

    row = {
        "question": market["question"],
        "category": market["category"],
        "outcome": outcome,
        "condition_id": market["condition_id"],
        "resolution_date": t0.isoformat(),
        "missing_horizons": ",".join(missing),
    }
    for label, _ in HORIZONS:
        row[f"yes_{label}"] = yes_snaps.get(label)
        row[f"no_{label}"] = no_snaps.get(label)
    row["yes_resolution"] = yes_snaps.get("resolution")
    row["no_resolution"] = no_snaps.get("resolution")

    return row, (len(missing) == 0)


def unique_path(path):
    """Return `path` if free, else the first `base(n).ext` that doesn't exist,
    so an existing file is never overwritten (browser-style numbering)."""
    if not os.path.exists(path):
        return path
    base, ext = os.path.splitext(path)
    n = 1
    while os.path.exists(f"{base}({n}){ext}"):
        n += 1
    return f"{base}({n}){ext}"


def write_csv(rows, out_path):
    """Write all accumulated rows to `out_path` atomically (temp file +
    os.replace), so an interrupt mid-write can never leave a half-written/corrupt
    CSV."""
    df = pd.DataFrame(rows, columns=COLUMNS)
    tmp = out_path + ".tmp"
    df.to_csv(tmp, index=False)
    os.replace(tmp, out_path)


def main(name=None):
    start_time = time.monotonic()
    os.makedirs(CACHE_DIR, exist_ok=True)
    os.makedirs(PAYOUT_CACHE_DIR, exist_ok=True)
    os.makedirs(_DATA_DIR, exist_ok=True)

    # Pick a non-overwriting output name once, up front, and reuse it for every
    # checkpoint + the final flush. Resolving it per write would scatter one run
    # across <name>.csv, <name>(1).csv, ... as each checkpoint sees the previous
    # file already there. A custom --name lands in Data/ and gets the same
    # numbering treatment as the default.
    base_csv = OUT_CSV if name is None else os.path.join(_DATA_DIR, f"{name}.csv")
    out_csv = unique_path(base_csv)

    print("Streaming resolved binary markets from Gamma...")
    stats = {"scanned": 0, "kept": 0}
    rows = []
    n_all_horizons = 0
    n_unresolved = 0
    skipped_empty = 0
    skipped_error = 0
    # Markets stream from the enumerator (main thread, Gamma) into a small
    # thread pool that does the per-market work (CLOB history + winner), since
    # per-call latency + polite sleeps, not bandwidth, dominate run time. Rows
    # are collected and checkpointed in enumeration order by draining the FIFO
    # from the front; blocking on the oldest future is the backpressure that
    # keeps at most ~2 batches in flight.
    #
    # tqdm total: MAX_MARKETS is an *exact* total when set (the generator returns
    # after yielding exactly that many kept markets), giving a determinate % + ETA
    # bar. When None (full pull) the total is unknown up front, so tqdm falls back
    # to an indeterminate counter (count + elapsed + rate, no % / ETA).
    bar = tqdm(iter_resolved_markets(stats), total=MAX_MARKETS,
               unit="market", desc="Processing markets")
    done = 0
    pending = deque()  # (future, market) in enumeration order

    def process(market):
        """Worker: full per-market pipeline. Returns (row_or_None, all_present)."""
        t0 = resolve_t0(market)
        if t0 is None:
            return None, False
        return build_row(market, t0)

    def drain(down_to):
        """Consume completed futures from the front of `pending` until at most
        `down_to` remain, updating counters and checkpointing. Isolates
        per-market failures: one bad market must not kill a multi-hour run."""
        nonlocal done, n_all_horizons, n_unresolved, skipped_empty, skipped_error
        while len(pending) > down_to:
            future, market = pending.popleft()
            done += 1
            try:
                row, all_present = future.result()
            except Exception as exc:
                skipped_error += 1
                tqdm.write(f"  ! skipped {market.get('condition_id')}: "
                           f"{type(exc).__name__}: {exc}")
            else:
                if row is None:
                    skipped_empty += 1
                else:
                    rows.append(row)
                    if all_present:
                        n_all_horizons += 1
                    if row["outcome"] is None:
                        n_unresolved += 1  # kept but flagged (blank outcome)
            # Surface live counts on the bar (elapsed time + rate are shown by
            # tqdm itself); refresh every 10 markets to keep overhead negligible.
            if done % 10 == 0:
                bar.set_postfix(kept=len(rows),
                                skipped=skipped_empty + skipped_error,
                                refresh=False)
            if done % FLUSH_EVERY == 0:
                write_csv(rows, out_csv)  # checkpoint to disk

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for market in bar:
            pending.append((pool.submit(process, market), market))
            drain(WORKERS * 2)
        drain(0)

    bar.close()
    write_csv(rows, out_csv)  # final flush

    elapsed = timedelta(seconds=time.monotonic() - start_time)
    print("\n=== Summary ===")
    print(f"Total resolved scanned    : {stats['scanned']}")
    print(f"Binary kept               : {stats['kept']}")
    print(f"Rows written              : {len(rows)} -> {os.path.relpath(out_csv)}")
    print(f"Markets w/ all 6 horizons : {n_all_horizons}")
    print(f"Unresolved/voided (flagged, blank outcome): {n_unresolved}")
    print(f"Skipped (no usable history/time)         : {skipped_empty}")
    print(f"Skipped (errors)                         : {skipped_error}")
    print(f"Total run time                           : {elapsed}")


def _max_markets_arg(s):
    """argparse type: an int cap, or None for a full pull ('all'/'none'/'0')."""
    if s.lower() in ("all", "none", "0"):
        return None
    try:
        n = int(s)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"max_markets must be a non-negative integer or 'all', got {s!r}")
    if n < 0:
        raise argparse.ArgumentTypeError("max_markets must be >= 0")
    return n


def _name_arg(s):
    """argparse type for --name: a bare output filename placed in Data/. Strips
    any directory part and a trailing extension, and rejects empty names or names
    containing characters illegal in a Windows filename (fail fast, before any
    fetching starts)."""
    stem = os.path.splitext(os.path.basename(s.strip()))[0]
    if not stem:
        raise argparse.ArgumentTypeError("name must not be empty")
    bad = set('<>:"/\\|?*') & set(stem)
    if bad:
        raise argparse.ArgumentTypeError(
            f"name has illegal character(s): {''.join(sorted(bad))}")
    return stem


def parse_args():
    """Optional positional override for the module-level MAX_MARKETS cap, e.g.
    `python fetch_polymarket.py 50`. Omitting it uses the file default; passing
    'all' (or 0) forces a full pull. `--name` sets a custom output base name."""
    p = argparse.ArgumentParser(
        description="Fetch resolved binary Polymarket markets to Data/<name>.csv.")
    p.add_argument(
        "max_markets", nargs="?", type=_max_markets_arg, default=MAX_MARKETS,
        help=f"Cap on kept binary markets (default from file: {MAX_MARKETS}). "
             "Pass an integer like 50, or 'all' for the full history.")
    p.add_argument(
        "-o", "--name", type=_name_arg, default=None,
        help="Output base filename (no extension), written to Data/ as "
             "<name>.csv and numbered if it already exists. Default: polymarket.")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    MAX_MARKETS = args.max_markets  # CLI overrides the file default
    main(args.name)
