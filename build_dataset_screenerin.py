"""
build_dataset_screenerin.py
===========================
Pulls fundamentals for the Nifty 500 directly from screener.in, which mirrors
BSE/NSE filings. Replaces yfinance as the data source so quarterly figures
match the latest filings.

Two-pass strategy:
  Pass 1: 3 workers, 12s timeout, fast. Captures ~70-80% before screener.in
          starts rate-limiting bursts of concurrent requests.
  Pass 2: sequential (1 worker) with 1.5s polite pauses, retrying only the
          symbols Pass 1 didn't get. Picks up most of the remaining stocks
          since the slow drip-feed doesn't trigger the WAF.

Setup: pip install requests beautifulsoup4 pandas
Run: python build_dataset_screenerin.py
Output: dataset.json
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
from bs4 import BeautifulSoup

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

REQUEST_TIMEOUT = 12       # seconds per HTTP request
RATE_LIMIT_BACKOFF = 8     # seconds to wait inside a worker after a 429/403
RATE_LIMIT_GIVEUP = 60     # bail out of a pass if this many *consecutive* RL responses


# Per-thread session so each worker keeps its own connection alive.
_thread_local = threading.local()
def _session() -> requests.Session:
    s = getattr(_thread_local, "session", None)
    if s is None:
        s = requests.Session()
        s.headers.update(HEADERS)
        _thread_local.session = s
    return s


class RateLimitTracker:
    """Shared signal so workers can bail collectively when screener.in
    flips into block mode."""

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


def _ratios_top(soup):
    out = {}
    for li in soup.select("ul#top-ratios li"):
        name_el = li.select_one(".name")
        val_el = li.select_one(".nowrap.value")
        if not name_el or not val_el:
            continue
        name = name_el.get_text(strip=True)
        nums = NUM_RE.findall(val_el.get_text(" ", strip=True))
        out[name] = [_num(x) for x in nums]
    return out


def _table_rows(soup, section_id):
    out = {}
    sect = soup.select_one("section#" + section_id)
    if not sect:
        return out
    for tr in sect.select("tbody tr"):
        cells = tr.find_all("td")
        if not cells:
            continue
        name = cells[0].get_text(" ", strip=True).rstrip(" +").strip()
        vals = [_num(c.get_text(strip=True)) for c in cells[1:]]
        out[name] = vals
    return out


def _name_sector(soup):
    name = soup.select_one("h1.h2") or soup.select_one("h1")
    sector = soup.select_one("a.shorten[href^='/company/compare/']")
    return (
        name.get_text(strip=True) if name else None,
        sector.get_text(strip=True) if sector else None,
    )


def _yoy(row):
    if not row or len(row) < 5:
        return None
    latest, prior = row[-1], row[-5]
    if prior in (None, 0) or latest is None:
        return None
    return (latest - prior) / abs(prior) * 100.0


def _qoq(row):
    if not row or len(row) < 2:
        return None
    latest, prior = row[-1], row[-2]
    if prior in (None, 0) or latest is None:
        return None
    return (latest - prior) / abs(prior) * 100.0


def _r(v, dp=2):
    return None if v is None else round(v, dp)


def fetch_one(symbol, tracker: RateLimitTracker, polite_sleep: float = 0.0):
    """Return parsed record dict on success, or a sentinel tuple on failure:
       ("rate_limit", symbol)  — got 403/429; should retry later
       ("missing",    symbol)  — page wasn't found / no quarterly results
       ("error",      symbol)  — network or parse error
    """
    if tracker.should_skip():
        return ("error", symbol)

    if polite_sleep:
        time.sleep(polite_sleep)

    sess = _session()
    r = None
    saw_rate_limit = False
    for path in (
        "/company/" + symbol + "/consolidated/",
        "/company/" + symbol + "/",
    ):
        try:
            resp = sess.get(BASE + path, timeout=REQUEST_TIMEOUT)
        except requests.RequestException:
            continue

        if resp.status_code == 200 and "Quarterly Results" in resp.text:
            tracker.reset()
            r = resp
            break

        if resp.status_code in (403, 429):
            tracker.hit()
            saw_rate_limit = True
            # Brief backoff; do NOT try the second path — it'll fail too.
            time.sleep(RATE_LIMIT_BACKOFF)
            break

        # 404 or 200-with-no-quarters: try the next path silently.

    if r is None:
        return ("rate_limit", symbol) if saw_rate_limit else ("missing", symbol)

    soup = BeautifulSoup(r.text, "html.parser")
    chips = _ratios_top(soup)
    q = _table_rows(soup, "quarters")
    bs = _table_rows(soup, "balance-sheet")
    name, sector = _name_sector(soup)

    def chip(*keys):
        for k in keys:
            if k in chips and chips[k]:
                return chips[k][0]
        return None

    def chip_pair(*keys):
        for k in keys:
            v = chips.get(k)
            if v and len(v) >= 2:
                return v[0], v[1]
        return None, None

    np_row = q.get("Net Profit") or q.get("Profit after tax")
    sales_row = q.get("Sales") or q.get("Revenue")
    opm_row = q.get("OPM %")

    ltp = chip("Current Price")
    hi52, lo52 = chip_pair("High / Low")
    pct_from_high = (hi52 - ltp) / hi52 * 100.0 if (hi52 and ltp) else None
    pct_from_low = (ltp - lo52) / lo52 * 100.0 if (lo52 and ltp) else None

    borrowings = (bs.get("Borrowings") or [None])[-1] if bs.get("Borrowings") else None
    reserves = (bs.get("Reserves") or [None])[-1] if bs.get("Reserves") else None
    de = None
    if borrowings is not None and reserves not in (None, 0):
        de = borrowings / reserves

    return {
        "symbol": symbol,
        "name": name,
        "sector": sector,
        "industry": None,
        "ltp": _r(ltp),
        "high_52w": _r(hi52),
        "low_52w": _r(lo52),
        "pct_from_high": _r(pct_from_high),
        "pct_from_low": _r(pct_from_low),
        "return_1y_pct": None,
        "pct_from_ma50": None,
        "pct_from_ma200": None,
        "beta": None,
        "market_cap_cr": _r(chip("Market Cap"), 1),
        "enterprise_val_cr": None,
        "pe_ttm": _r(chip("Stock P/E")),
        "pe_forward": None,
        "pb": _r(chip("Price to book value", "P/B Ratio")),
        "ps": None,
        "peg": _r(chip("PEG Ratio")),
        "ev_ebitda": None,
        "ev_revenue": None,
        "roe_pct": _r(chip("ROE")),
        "roa_pct": None,
        "profit_margin_pct": None,
        "operating_margin_pct": _r(opm_row[-1]) if opm_row else None,
        "gross_margin_pct": None,
        "debt_to_equity": _r(de),
        "current_ratio": None,
        "quick_ratio": None,
        "revenue_growth_pct": None,
        "earnings_growth_pct": None,
        "np_yoy_pct": _r(_yoy(np_row)),
        "np_qoq_pct": _r(_qoq(np_row)),
        "rev_yoy_pct": _r(_yoy(sales_row)),
        "rev_qoq_pct": _r(_qoq(sales_row)),
        "dividend_yield_pct": _r(chip("Dividend Yield")),
        "payout_ratio_pct": _r(chip("Dividend Payout")),
        "roce_pct": _r(chip("ROCE")),
    }


def _metrics_definition():
    return [
        {"key": "ltp", "label": "Last Price", "group": "Price", "unit": ""},
        {"key": "market_cap_cr", "label": "Market Cap", "group": "Size", "unit": "Cr"},
        {"key": "pe_ttm", "label": "Stock P/E", "group": "Valuation", "unit": "x"},
        {"key": "pb", "label": "Price / Book", "group": "Valuation", "unit": "x"},
        {"key": "peg", "label": "PEG Ratio", "group": "Valuation", "unit": ""},
        {"key": "roe_pct", "label": "Return on Equity", "group": "Profitability", "unit": "%"},
        {"key": "roce_pct", "label": "Return on Capital Employed","group": "Profitability", "unit": "%"},
        {"key": "operating_margin_pct", "label": "Operating Margin (qtr)", "group": "Profitability", "unit": "%"},
        {"key": "debt_to_equity", "label": "Borrowings / Reserves", "group": "Solvency", "unit": ""},
        {"key": "np_yoy_pct", "label": "Net Profit YoY (qtr)", "group": "Growth", "unit": "%"},
        {"key": "np_qoq_pct", "label": "Net Profit QoQ", "group": "Growth", "unit": "%"},
        {"key": "rev_yoy_pct", "label": "Sales YoY (qtr)", "group": "Growth", "unit": "%"},
        {"key": "rev_qoq_pct", "label": "Sales QoQ", "group": "Growth", "unit": "%"},
        {"key": "pct_from_high", "label": "% from 52W High", "group": "Technical", "unit": "%"},
        {"key": "pct_from_low", "label": "% from 52W Low", "group": "Technical", "unit": "%"},
        {"key": "dividend_yield_pct", "label": "Dividend Yield", "group": "Income", "unit": "%"},
        {"key": "payout_ratio_pct", "label": "Dividend Payout", "group": "Income", "unit": "%"},
    ]


def _scrape_pass(symbols, workers, polite_sleep, label, log):
    """Run one scraping pass. Returns (records, missed_rate_limit, missed_other)."""
    tracker = RateLimitTracker()
    records = []
    missed_rl = []
    missed_other = []
    t0 = time.time()

    log("  pass '" + label + "' starting: " + str(len(symbols))
        + " symbols, workers=" + str(workers)
        + ", polite_sleep=" + str(polite_sleep) + "s")

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {
            ex.submit(fetch_one, s, tracker, polite_sleep): s
            for s in symbols
        }
        for i, fut in enumerate(as_completed(futures), 1):
            try:
                res = fut.result()
            except Exception as e:
                log("    ! " + str(e)[:120])
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
                log("    " + str(i) + "/" + str(len(symbols))
                    + " (" + str(int(time.time() - t0)) + "s kept=" + str(len(records))
                    + " rate_limited=" + str(tracker.total) + ")")

            if tracker.should_skip():
                log("    ! rate-limit streak hit " + str(RATE_LIMIT_GIVEUP)
                    + " — aborting this pass")
                # Remaining futures will see should_skip() and short-circuit;
                # whatever symbols they were assigned go into missed_rl.
                break

    # Any symbols whose futures never completed (because we broke out) end up
    # missed too. Collect them.
    seen_syms = (
        {r["symbol"] for r in records}
        | set(missed_rl)
        | set(missed_other)
    )
    for s in symbols:
        if s not in seen_syms:
            missed_rl.append(s)

    log("  pass '" + label + "' done in " + str(int(time.time() - t0)) + "s: "
        + "kept=" + str(len(records))
        + ", rate_limited=" + str(len(missed_rl))
        + ", missing=" + str(len(missed_other)))
    return records, missed_rl, missed_other


def main():
    prog = open("scrape_progress.txt", "w", buffering=1, encoding="utf-8")

    def log(msg):
        prog.write(msg + "\n")
        prog.flush()
        print(msg, flush=True)

    log("Fetching Nifty 500 list...")
    symbols = get_nifty500()
    log(str(len(symbols)) + " symbols. Scraping screener.in...")

    overall_t0 = time.time()

    # ---- Pass 1: fast, 3 workers, no per-request sleep ----
    pass1_records, pass1_rl, pass1_missing = _scrape_pass(
        symbols, workers=3, polite_sleep=0.0, label="fast", log=log
    )

    # ---- Cooldown so screener.in stops seeing us as a burst ----
    if pass1_rl:
        log("Cooling down 30s before retry pass...")
        time.sleep(30)

    # ---- Pass 2: sequential, slow, only retry the rate-limited symbols ----
    pass2_records = []
    pass2_rl = []
    pass2_missing = []
    if pass1_rl:
        pass2_records, pass2_rl, pass2_missing = _scrape_pass(
            pass1_rl, workers=1, polite_sleep=1.5, label="retry", log=log
        )

    # ---- Pass 3: very-slow last-chance for stragglers still rate-limited ----
    pass3_records = []
    if pass2_rl:
        log("Cooling down 30s before final pass...")
        time.sleep(30)
        pass3_records, _, _ = _scrape_pass(
            pass2_rl, workers=1, polite_sleep=3.0, label="final", log=log
        )

    records = pass1_records + pass2_records + pass3_records
    log("Total kept after all passes: " + str(len(records))
        + " (took " + str(int(time.time() - overall_t0)) + "s)")
    if pass1_missing or pass2_missing:
        log("  not on screener.in (no quarterly page): "
            + str(len(pass1_missing) + len(pass2_missing)))

    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "universe": "Nifty 500",
        "source": "screener.in",
        "count": len(records),
        "metrics": _metrics_definition(),
        "stocks": sorted(records, key=lambda r: r.get("symbol", "")),
    }
    with open("dataset.json", "w", encoding="utf-8") as f:
        json.dump(out, f, separators=(",", ":"), ensure_ascii=False)
    log("Done. " + str(len(records)) + " stocks -> dataset.json")

    # Fail loudly if the dataset is way smaller than we expect, so the
    # workflow doesn't silently commit a broken dataset over a good one.
    if len(records) < 100:
        log("FATAL: only " + str(len(records)) + " stocks scraped — not overwriting.")
        sys.exit(1)


if __name__ == "__main__":
    main()
