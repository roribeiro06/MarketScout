"""Generate sample_report_1.txt and sample_report_2.txt for format preview.

- Indices: REAL data (fetched via get_indices_snapshot).
- Stocks / Rising Stars / Crypto / etc.: MOCK data only — for layout preview.

To send a REAL report to Telegram (correct prices, volume, criteria), run:
  python run_screener.py
with dry_run: false in config.yaml. Do not send mock data to Telegram.
"""
import sys
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

# Add project root so we can import from run_screener
sys.path.insert(0, str(Path(__file__).resolve().parent))
from run_screener import format_stock_message, get_indices_snapshot

def _stock(symbol, company_name, sector, price, vol, one_day, one_week, one_month, passes_d, passes_w, passes_m, **kw):
    return {
        "symbol": symbol,
        "company_name": company_name,
        "sector": sector,
        "price": price,
        "volume": vol,
        "one_day_pct": one_day,
        "one_week_pct": one_week,
        "one_month_pct": one_month,
        "one_6m_pct": kw.get("one_6m_pct"),
        "one_year_pct": kw.get("one_year_pct"),
        "three_year_pct": kw.get("three_year_pct"),
        "passes_day": passes_d,
        "passes_week": passes_w,
        "passes_month": passes_m,
    }

def main():
    collection_time = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M %Z")
    # Mock: 2 big stocks (Communication Services), 2 Rising Stars
    results = [
        _stock("GOOGL", "Alphabet Inc.", "Communication Services", 305.72, 38_470_000, -1.06, -5.31, -8.13, False, True, False, one_6m_pct=50.88, one_year_pct=67.16, three_year_pct=209.98),
        _stock("NFLX", "Netflix, Inc.", "Communication Services", 76.87, 42_230_000, 1.33, -6.48, -12.70, False, True, True, one_6m_pct=-37.53, one_year_pct=-25.17, three_year_pct=109.55),
        _stock("SMCI", "Super Micro Computer", "Rising Stars", 45.20, 15_000_000, 4.50, 12.30, -18.20, True, True, True, one_6m_pct=20.0, one_year_pct=80.0, three_year_pct=200.0),
        _stock("PLTR", "Palantir Technologies", "Rising Stars", 28.50, 55_000_000, -2.80, -11.20, 22.50, True, True, True, one_6m_pct=-10.0, one_year_pct=15.0, three_year_pct=100.0),
    ]
    # Crypto
    results.append(_stock("BTC-USD", "Bitcoin", "Crypto", 97500.0, 0, 2.5, -5.0, 8.0, False, True, False, one_6m_pct=20.0, one_year_pct=60.0, three_year_pct=150.0))
    results.append(_stock("ETH-USD", "Ethereum", "Crypto", 3450.0, 0, -1.2, 3.5, -12.0, False, False, True, one_6m_pct=15.0, one_year_pct=40.0, three_year_pct=120.0))
    # Commodity
    results.append({
        "symbol": "NG=F",
        "company_name": "Natural Gas",
        "sector": "Commodities",
        "price": 3.24,
        "volume": 222_300,
        "one_day_pct": 0.81,
        "one_week_pct": -5.23,
        "one_month_pct": 3.68,
        "one_6m_pct": 14.15,
        "one_year_pct": -10.61,
        "three_year_pct": 34.84,
        "passes_day": False,
        "passes_week": True,
        "passes_month": False,
    })
    # Forex
    results.append({
        "symbol": "USDJPY=X",
        "company_name": "US Dollar / Japanese Yen",
        "sector": "Forex",
        "price": 152.8210,
        "volume": 0,
        "one_day_pct": -0.29,
        "one_week_pct": -2.53,
        "one_month_pct": -3.64,
        "one_6m_pct": 3.32,
        "one_year_pct": 1.06,
        "three_year_pct": 14.96,
        "passes_day": False,
        "passes_week": True,
        "passes_month": True,
    })
    # ETF
    results.append({
        "symbol": "SLV",
        "company_name": "iShares Silver Trust",
        "sector": "ETFs",
        "asset_class": "Commodities",
        "price": 69.72,
        "volume": 69_390_000,
        "one_day_pct": 2.94,
        "one_week_pct": -0.67,
        "one_month_pct": -16.32,
        "one_6m_pct": 102.09,
        "one_year_pct": 137.63,
        "three_year_pct": 240.43,
        "passes_day": False,
        "passes_week": False,
        "passes_month": True,
    })

    # Use real indices (all 5: S&P 500, Nasdaq, Dow 30, Russell 2000, VIX)
    indices_data = get_indices_snapshot()

    msg1, msg2 = format_stock_message(
        results,
        crypto_count=2,
        forex_count=1,
        commodity_count=1,
        etf_count=1,
        indices_data=indices_data,
        etf_asset_class_order=["Equity", "Fixed Income", "Commodities", "Currency", "Asset Location", "Alternatives"],
        collection_time=collection_time,
    )
    Path("sample_report_1.txt").write_text(msg1, encoding="utf-8")
    Path("sample_report_2.txt").write_text(msg2, encoding="utf-8")
    print("Wrote sample_report_1.txt and sample_report_2.txt")
    print("(Stocks/crypto/etc. are MOCK data. For a real report to Telegram, run: python run_screener.py)")

if __name__ == "__main__":
    main()
