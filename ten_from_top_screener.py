"""
10-from-top Stock Screener
==========================

A self-built rebuild of the Trendlyne screener "10% from top" (#491229).

Filters Nifty 500 stocks where ALL three hold:
  1. % distance from 52-week LOW  > 100   (stock has more than doubled from its yearly low)
  2. % distance from 52-week HIGH < 10    (currently within 10% of its yearly high)
  3. Latest-quarter Net Profit YoY growth > 0

Data source: Yahoo Finance via the free `yfinance` library.

Setup
-----
    pip install yfinance pandas requests

Run
---
    python ten_from_top_screener.py

Output
------
    Prints matching stocks to the console and writes screener_results.csv next to the script.

Notes
-----
* Free data is noisier than Trendlyne's paid feed. Expect the result list to overlap
  by ~70-80% with Trendlyne, not match exactly.
* yfinance is rate-limited; the full Nifty 500 scan takes ~3-5 minutes.
"""

import io
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
import yfinance as yf


# ------------------------------------------------------------------
# 1. Build the stock universe (Nifty 500)
# ------------------------------------------------------------------
NIFTY500_URL = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"


def get_nifty500_symbols():
    """Download the official Nifty 500 constituents list from NSE."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0 Safari/537.36"
        ),
        "Accept": "text/csv,application/csv",
    }
    resp = requests.get(NIFTY500_URL, headers=headers, timeout=30)
    resp.raise_for_status()
    df = pd.read_csv(io.StringIO(resp.text))
    # Column is "Symbol" — append .NS so yfinance treats it as NSE
    return [s.strip() + ".NS" for s in df["Symbol"].tolist()]


# ------------------------------------------------------------------
# 2. Compute the screening fields for one stock
# ------------------------------------------------------------------
NET_INCOME_KEYS = [
    "Net Income",
    "NetIncome",
    "Net Income Common Stockholders",
    "Net Income From Continuing Operation Net Minority Interest",
]


def _find_net_income_row(quarterly_stmt):
    """yfinance sometimes labels the net income row differently. Try a few."""
    if quarterly_stmt is None or quarterly_stmt.empty:
        return None
    for key in NET_INCOME_KEYS:
        if key in quarterly_stmt.index:
            return quarterly_stmt.loc[key].dropna()
    return None


def screen_one(symbol):
    """Return a dict of metrics if the stock passes all three filters, else None."""
    try:
        tkr = yf.Ticker(symbol)

        # --- price-based filters ---
        hist = tkr.history(period="1y", auto_adjust=False)
        if hist.empty or len(hist) < 50:
            return None
        ltp = float(hist["Close"].iloc[-1])
        hi52 = float(hist["High"].max())
        lo52 = float(hist["Low"].min())
        if lo52 <= 0 or hi52 <= 0:
            return None

        dist_from_high = (hi52 - ltp) / hi52 * 100.0    # smaller = closer to high
        dist_from_low = (ltp - lo52) / lo52 * 100.0     # larger = further from low

        # Filters 1 and 2
        if not (dist_from_low > 100 and dist_from_high < 10):
            return None

        # --- fundamentals filter ---
        ni = _find_net_income_row(tkr.quarterly_income_stmt)
        if ni is None or len(ni) < 2:
            return None
        # Index is the quarter-end date; columns are most-recent-first.
        # Yahoo's history sometimes has gaps, so do NOT blindly use iloc[4].
        # Instead match the same calendar quarter from a year ago (within ~45 days).
        latest_date = ni.index[0]
        latest = ni.iloc[0]
        target = latest_date - pd.DateOffset(years=1)
        gaps = pd.Series((ni.index - target).days, index=ni.index).abs()
        match_idx = gaps.idxmin()
        if abs((match_idx - target).days) > 45 or match_idx == latest_date:
            return None
        yoy = ni.loc[match_idx]
        if pd.isna(latest) or pd.isna(yoy) or yoy == 0:
            return None
        np_growth = (latest - yoy) / abs(yoy) * 100.0
        if np_growth <= 0:
            return None

        # --- enrichment columns (best-effort, blank on failure) ---
        try:
            info = tkr.info or {}
        except Exception:
            info = {}
        mcap_cr = (info.get("marketCap") or 0) / 1e7

        return {
            "Symbol": symbol.replace(".NS", ""),
            "LTP": round(ltp, 2),
            "% from 52W High": round(dist_from_high, 2),
            "% from 52W Low": round(dist_from_low, 2),
            "Net Profit YoY %": round(np_growth, 2),
            "Market Cap (Cr)": round(mcap_cr, 1) if mcap_cr else None,
            "PE TTM": info.get("trailingPE"),
            "PB": info.get("priceToBook"),
        }
    except Exception:
        return None


# ------------------------------------------------------------------
# 3. Main — fan out, collect, sort, save
# ------------------------------------------------------------------
def main(max_workers=8):
    print("Fetching Nifty 500 list...")
    symbols = get_nifty500_symbols()
    print(f"Loaded {len(symbols)} symbols. Starting screen...\n")

    results = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(screen_one, s): s for s in symbols}
        for i, fut in enumerate(as_completed(futures), 1):
            r = fut.result()
            if r:
                results.append(r)
            if i % 50 == 0:
                elapsed = time.time() - t0
                print(f"  processed {i}/{len(symbols)}  ({elapsed:.0f}s elapsed, "
                      f"{len(results)} hits so far)")

    if not results:
        print("\nNo matches found.")
        return

    df = pd.DataFrame(results).sort_values("% from 52W High").reset_index(drop=True)
    df.index += 1   # 1-based rank

    print(f"\n{'='*78}")
    print(f"{len(df)} stocks match the screener")
    print(f"{'='*78}")
    # pandas display options for the console
    with pd.option_context("display.max_rows", None,
                           "display.width", 140,
                           "display.float_format", lambda v: f"{v:,.2f}"):
        print(df)

    out = "screener_results.csv"
    df.to_csv(out, index_label="Rank")
    print(f"\nSaved -> {out}")


if __name__ == "__main__":
    sys.exit(main())
