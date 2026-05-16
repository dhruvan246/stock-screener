"""
build_history.py
================
Builds a 10-year-deep historical dataset for backtesting the screener.

Per stock, captures:
  - Monthly snapshots (close, 52W high, 52W low) from yfinance daily history
  - Quarterly Net Profit / Sales (13 quarters, ~3 years) from screener.in
  - Annual Net Profit / Sales (12 years) from screener.in

At backtest time, for any month-end date D, we know the close, the
trailing-52W high/low, and (using the most recent quarterly or annual
report that would have been published by D) the YoY growth metrics.

Setup: pip install requests beautifulsoup4 pandas yfinance
Run: python build_history.py
Output: history.json
"""

import io
import json
import math
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup

# ---- shared config ------------------------------------------------------

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.screener.in/",
    "Connection": "keep-alive",
}
NIFTY500_URL = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"
BASE = "https://www.screener.in"
NUM_RE = re.compile(r"-?[\d,]+(?:\.\d+)?")

HISTORY_YEARS = 10
REQUEST_TIMEOUT = 12
RATE_LIMIT_BACKOFF = 8
RATE_LIMIT_GIVEUP = 60


_thread_local = threading.local()
def _session() -> requests.Session:
    s = getattr(_thread_local, "session", None)
    if s is None:
        s = requests.Session()
        s.headers.update(HEADERS)
        _thread_local.session = s
    return s


class RateLimitTracker:
    def __init__(self):
        self.consecutive = 0
        self.total = 0
        self.aborted = False
        self.lock = threading.Lock()

    def hit(self):
        with self.lock:
            self.consecutive += 1
            self.total += 1
            if self.consecutive >= RATE_LIMIT_GIVEUP:
                self.aborted = True

    def reset(self):
        with self.lock:
            self.consecutive = 0

    def should_skip(self) -> bool:
        return self.aborted


# ---- helpers ------------------------------------------------------------

def get_nifty500():
    r = requests.get(NIFTY500_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return [s.strip() for s in pd.read_csv(io.StringIO(r.text))["Symbol"]]


def _num(s):
    if s is None:
        return None
    s = str(s).replace(",", "").replace("₹", "").replace("%", "").strip()
    if s in ("", "-", "—", "N/A"):
        return None
    try:
        f = float(s)
        return None if math.isnan(f) or math.isinf(f) else f
    except ValueError:
        return None


def _has_real_data(html: str) -> bool:
    m = re.search(
        r'<span class="name">\s*Current Price\s*</span>.*?'
        r'<span class="(?:nowrap )?number">\s*([0-9])',
        html,
        flags=re.DOTALL,
    )
    return bool(m)


def _table_with_headers(soup, section_id):
    """Return {'headers': [...], 'rows': {label: [values...]}} or None."""
    sect = soup.select_one("section#" + section_id)
    if not sect:
        return None
    headers = [th.get_text(strip=True) for th in sect.select("thead th")]
    rows = {}
    for tr in sect.select("tbody tr"):
        cells = tr.find_all("td")
        if not cells:
            continue
        name = cells[0].get_text(" ", strip=True).rstrip(" +").strip()
        vals = [_num(c.get_text(strip=True)) for c in cells[1:]]
        rows[name] = vals
    return {"headers": headers[1:], "rows": rows}  # skip the leading blank header


# ---- screener.in fundamentals fetch -------------------------------------

def fetch_fundamentals(symbol: str, tracker: RateLimitTracker, polite_sleep: float = 0.0):
    """Fetch quarterly + annual P&L history for one symbol from screener.in.

    Returns a dict like:
      {
        "name": "...",
        "quarterly": {"periods": ["Mar 2023", ...], "net_profit": [...], "sales": [...], "opm_pct": [...]},
        "annual":    {"periods": ["Mar 2015", ...], "net_profit": [...], "sales": [...]},
      }
    Or a sentinel tuple on failure: ("rate_limit", sym) / ("missing", sym).
    """
    if tracker.should_skip():
        return ("error", symbol)
    if polite_sleep:
        time.sleep(polite_sleep)

    sess = _session()
    r = None
    saw_rl = False
    for path in (f"/company/{symbol}/consolidated/", f"/company/{symbol}/"):
        try:
            resp = sess.get(BASE + path, timeout=REQUEST_TIMEOUT)
        except requests.RequestException:
            continue
        if resp.status_code == 200 and "Quarterly Results" in resp.text:
            if _has_real_data(resp.text):
                tracker.reset()
                r = resp
                break
            continue
        if resp.status_code in (403, 429):
            tracker.hit()
            saw_rl = True
            time.sleep(RATE_LIMIT_BACKOFF)
            break

    if r is None:
        return ("rate_limit", symbol) if saw_rl else ("missing", symbol)

    soup = BeautifulSoup(r.text, "html.parser")
    name_el = soup.select_one("h1.h2") or soup.select_one("h1")
    name = name_el.get_text(strip=True) if name_el else None

    q = _table_with_headers(soup, "quarters")
    pl = _table_with_headers(soup, "profit-loss")

    out = {"name": name, "quarterly": None, "annual": None}

    if q:
        np_row = q["rows"].get("Net Profit") or q["rows"].get("Profit after tax")
        sales_row = q["rows"].get("Sales") or q["rows"].get("Revenue")
        opm_row = q["rows"].get("OPM %")
        out["quarterly"] = {
            "periods": q["headers"],
            "net_profit": np_row,
            "sales": sales_row,
            "opm_pct": opm_row,
        }

    if pl:
        np_row = pl["rows"].get("Net Profit") or pl["rows"].get("Profit after tax")
        sales_row = pl["rows"].get("Sales") or pl["rows"].get("Revenue")
        out["annual"] = {
            "periods": pl["headers"],
            "net_profit": np_row,
            "sales": sales_row,
        }

    return out


# ---- yfinance price history --------------------------------------------

def fetch_prices(symbol: str):
    """Fetch ~HISTORY_YEARS of daily adj close from yfinance for NSE ticker SYMBOL.NS.

    Returns a list of monthly snapshots:
      [{"d": "YYYY-MM-DD", "close": float, "high_52w": float, "low_52w": float}, ...]
    """
    yticker = symbol + ".NS"
    try:
        df = yf.download(
            yticker,
            period=f"{HISTORY_YEARS + 1}y",
            interval="1d",
            progress=False,
            auto_adjust=True,
            threads=False,
        )
    except Exception:
        return None
    if df is None or df.empty:
        return None
    # Squeeze multi-level columns if yfinance returned them
    if hasattr(df.columns, "nlevels") and df.columns.nlevels > 1:
        df.columns = df.columns.get_level_values(0)
    # Need Close and High/Low (use Close-based 52w high/low for consistency with
    # screener.in's "High/Low" which is intraday but Close-based is more stable)
    df = df.dropna(subset=["Close"])

    # Compute 252-trading-day rolling high/low on Close
    df["roll_high"] = df["Close"].rolling(252, min_periods=20).max()
    df["roll_low"]  = df["Close"].rolling(252, min_periods=20).min()

    # Resample to month-end (use last available trading day in each month)
    monthly = df.resample("ME").last()  # ME = month-end frequency

    out = []
    for d, row in monthly.iterrows():
        c = row.get("Close")
        if pd.isna(c):
            continue
        out.append({
            "d": d.strftime("%Y-%m-%d"),
            "close": round(float(c), 2),
            "high_52w": (round(float(row["roll_high"]), 2) if not pd.isna(row.get("roll_high")) else None),
            "low_52w":  (round(float(row["roll_low"]),  2) if not pd.isna(row.get("roll_low"))  else None),
        })
    return out


# ---- per-symbol orchestration ------------------------------------------

def process_one(symbol: str, tracker: RateLimitTracker, polite_sleep: float = 0.0):
    fund = fetch_fundamentals(symbol, tracker, polite_sleep=polite_sleep)
    if isinstance(fund, tuple):
        return (fund[0], symbol)  # propagate sentinel

    prices = fetch_prices(symbol)
    if prices is None:
        prices = []

    return {
        "symbol": symbol,
        "name": fund.get("name"),
        "monthly": prices,
        "quarterly": fund.get("quarterly"),
        "annual": fund.get("annual"),
    }


# ---- pass runner --------------------------------------------------------

def _scrape_pass(symbols, workers, polite_sleep, label, log):
    tracker = RateLimitTracker()
    records = []
    missed_rl = []
    missed_other = []
    t0 = time.time()

    log(f"  pass '{label}' starting: {len(symbols)} symbols, workers={workers}, polite_sleep={polite_sleep}s")

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(process_one, s, tracker, polite_sleep): s for s in symbols}
        for i, fut in enumerate(as_completed(futures), 1):
            try:
                res = fut.result()
            except Exception as e:
                log(f"    ! {str(e)[:120]}")
                missed_other.append(futures[fut])
                continue
            if isinstance(res, dict):
                records.append(res)
            elif isinstance(res, tuple):
                kind, sym = res
                if kind == "rate_limit":
                    missed_rl.append(sym)
                else:
                    missed_other.append(sym)
            if i % 25 == 0:
                log(f"    {i}/{len(symbols)} ({int(time.time()-t0)}s kept={len(records)} rl={tracker.total})")
            if tracker.should_skip():
                log(f"    ! rate-limit streak hit {RATE_LIMIT_GIVEUP} — aborting this pass")
                break

    seen_syms = {r["symbol"] for r in records} | set(missed_rl) | set(missed_other)
    for s in symbols:
        if s not in seen_syms:
            missed_rl.append(s)

    log(f"  pass '{label}' done in {int(time.time()-t0)}s: kept={len(records)}, rate_limited={len(missed_rl)}, missing={len(missed_other)}")
    return records, missed_rl, missed_other


# ---- main ---------------------------------------------------------------

def main():
    prog = open("history_progress.txt", "w", buffering=1, encoding="utf-8")
    def log(msg):
        prog.write(msg + "\n"); prog.flush()
        print(msg, flush=True)

    log("Fetching Nifty 500 list...")
    symbols = get_nifty500()
    log(f"{len(symbols)} symbols. Building 10y history...")

    overall_t0 = time.time()

    # Pass 1: 3 workers — most stocks
    p1_recs, p1_rl, p1_miss = _scrape_pass(symbols, workers=3, polite_sleep=0.0, label="fast", log=log)

    # Pass 2: sequential retry for rate-limited stocks
    p2_recs, p2_rl, p2_miss = [], [], []
    if p1_rl:
        log("Cooling down 30s before retry pass...")
        time.sleep(30)
        p2_recs, p2_rl, p2_miss = _scrape_pass(p1_rl, workers=1, polite_sleep=1.5, label="retry", log=log)

    # Pass 3: very-slow last chance
    p3_recs = []
    if p2_rl:
        log("Cooling down 30s before final pass...")
        time.sleep(30)
        p3_recs, _, _ = _scrape_pass(p2_rl, workers=1, polite_sleep=3.0, label="final", log=log)

    records = p1_recs + p2_recs + p3_recs
    log(f"Total stocks captured: {len(records)} ({int(time.time()-overall_t0)}s)")

    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "universe": "Nifty 500",
        "history_years": HISTORY_YEARS,
        "sources": ["yfinance (daily prices)", "screener.in (quarterly+annual P&L)"],
        "count": len(records),
        "stocks": sorted(records, key=lambda r: r["symbol"]),
    }
    with open("history.json", "w", encoding="utf-8") as f:
        json.dump(out, f, separators=(",", ":"), ensure_ascii=False)
    log(f"Done. {len(records)} stocks -> history.json")

    if len(records) < 100:
        log(f"FATAL: only {len(records)} stocks — not overwriting.")
        sys.exit(1)


if __name__ == "__main__":
    main()
