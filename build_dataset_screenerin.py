"""
build_dataset_screenerin.py
===========================
Pulls fundamentals for the Nifty 500 directly from screener.in, which mirrors
BSE/NSE filings. Replaces yfinance as the data source so quarterly figures
match the latest filings.

Setup:  pip install requests beautifulsoup4 pandas
Run:    python build_dataset_screenerin.py
Output: dataset.json
"""

import io
import json
import math
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import pandas as pd
import requests
from bs4 import BeautifulSoup

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) personal-screener/1.0"}
NIFTY500_URL = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"
BASE = "https://www.screener.in"
NUM_RE = re.compile(r"-?[\d,]+(?:\.\d+)?")


def get_nifty500():
    r = requests.get(NIFTY500_URL, headers=UA, timeout=30)
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


def fetch_one(symbol, delay=0.4):
    if delay:
        time.sleep(delay)
    r = None
    for path in ("/company/" + symbol + "/consolidated/", "/company/" + symbol + "/"):
        try:
            resp = requests.get(BASE + path, headers=UA, timeout=20)
        except requests.RequestException:
            continue
        if resp.status_code == 200 and "Quarterly Results" in resp.text:
            r = resp
            break
    if r is None:
        return None

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
        {"key": "ltp",                  "label": "Last Price",                "group": "Price",         "unit": ""},
        {"key": "market_cap_cr",        "label": "Market Cap",                "group": "Size",          "unit": "Cr"},
        {"key": "pe_ttm",               "label": "Stock P/E",                 "group": "Valuation",     "unit": "x"},
        {"key": "pb",                   "label": "Price / Book",              "group": "Valuation",     "unit": "x"},
        {"key": "peg",                  "label": "PEG Ratio",                 "group": "Valuation",     "unit": ""},
        {"key": "roe_pct",              "label": "Return on Equity",          "group": "Profitability", "unit": "%"},
        {"key": "roce_pct",             "label": "Return on Capital Employed","group": "Profitability", "unit": "%"},
        {"key": "operating_margin_pct", "label": "Operating Margin (qtr)",    "group": "Profitability", "unit": "%"},
        {"key": "debt_to_equity",       "label": "Borrowings / Reserves",     "group": "Solvency",      "unit": ""},
        {"key": "np_yoy_pct",           "label": "Net Profit YoY (qtr)",      "group": "Growth",        "unit": "%"},
        {"key": "np_qoq_pct",           "label": "Net Profit QoQ",            "group": "Growth",        "unit": "%"},
        {"key": "rev_yoy_pct",          "label": "Sales YoY (qtr)",           "group": "Growth",        "unit": "%"},
        {"key": "rev_qoq_pct",          "label": "Sales QoQ",                 "group": "Growth",        "unit": "%"},
        {"key": "pct_from_high",        "label": "% from 52W High",           "group": "Technical",     "unit": "%"},
        {"key": "pct_from_low",         "label": "% from 52W Low",            "group": "Technical",     "unit": "%"},
        {"key": "dividend_yield_pct",   "label": "Dividend Yield",            "group": "Income",        "unit": "%"},
        {"key": "payout_ratio_pct",     "label": "Dividend Payout",           "group": "Income",        "unit": "%"},
    ]


def main(workers=6):
    prog = open("scrape_progress.txt", "w", buffering=1, encoding="utf-8")

    def log(msg):
        prog.write(msg + "\n")
        prog.flush()
        print(msg, flush=True)

    log("Fetching Nifty 500 list...")
    symbols = get_nifty500()
    log("  " + str(len(symbols)) + " symbols. Scraping screener.in...")

    records = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(fetch_one, s): s for s in symbols}
        for i, fut in enumerate(as_completed(futures), 1):
            try:
                rec = fut.result()
            except Exception as e:
                log("  ! " + str(e)[:120])
                rec = None
            if rec:
                records.append(rec)
            if i % 25 == 0:
                log("  " + str(i) + "/" + str(len(symbols))
                    + "  (" + str(int(time.time() - t0)) + "s  kept=" + str(len(records)) + ")")

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
    log("Done. " + str(len(records)) + " stocks -> dataset.json  in " + str(int(time.time() - t0)) + "s")


if __name__ == "__main__":
    main()
