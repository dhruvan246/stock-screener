# 10-from-top Stock Screener

A self-built rebuild of the Trendlyne screener
[**"10% from top"** (#491229)](https://trendlyne.com/fundamentals/stock-screener/491229/10-from-top/),
which filters Indian-listed stocks meeting all three:

1. **% distance from 52-week low > 100** — the stock has more than doubled from its yearly low.
2. **% distance from 52-week high < 10** — currently within 10% of its yearly high.
3. **Latest-quarter Net Profit YoY growth > 0** — positive year-over-year quarterly net profit.

Data source: Yahoo Finance (free, via `yfinance`). Universe: Nifty 500.

## Run it

### Option A — from the raw URL (Python + deps required)

```bash
pip install yfinance pandas requests
curl -sSL https://raw.githubusercontent.com/<your-username>/stock-screener/main/ten_from_top_screener.py | python -
```

### Option B — clone and run locally

```bash
git clone https://github.com/<your-username>/stock-screener.git
cd stock-screener
pip install yfinance pandas requests
python ten_from_top_screener.py
```

### Option C — Windows one-click

Double-click `run2.bat` after placing it in the same folder as `ten_from_top_screener.py`.
It runs `pip install` then the screener, logging everything to `run2.log`
and saving results to `screener_results.csv`.

## Output

Matching stocks are printed to the console and written to `screener_results.csv`
with these columns:

| Column | Meaning |
|---|---|
| Symbol | NSE ticker |
| LTP | Last traded price |
| % from 52W High | Distance from 52-week high (smaller = closer to high) |
| % from 52W Low | Distance from 52-week low (larger = further from low) |
| Net Profit YoY % | Latest-quarter Net Income vs same quarter prior year |
| Market Cap (Cr) | Market capitalisation in INR crores |
| PE TTM | Trailing P/E ratio |
| PB | Price-to-book ratio |

## Caveats vs Trendlyne

- We screen Nifty 500 (~500 names) while Trendlyne screens the full Indian
  universe (~5000+), so this returns a subset of Trendlyne's matches.
- yfinance is rate-limited; a full Nifty 500 scan takes ~30–60 seconds.
- Quarterly history on Yahoo Finance occasionally has gaps. The script
  matches the same calendar quarter from a year ago within ±45 days,
  which mirrors Trendlyne's definition.
- Free fundamentals can lag the latest filing by a few days.

## Customise

Edit `ten_from_top_screener.py` to change:

- **Thresholds:** the `100`, `10`, and `0` numbers in `screen_one()`.
- **Universe:** swap `get_nifty500_symbols()` for any list of NSE tickers
  (append `.NS` for Yahoo's NSE namespace).
- **Columns:** add more fields from `tkr.info` to the returned dict.
