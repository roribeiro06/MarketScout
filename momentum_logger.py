"""
Momentum logger:
- Universe: all NYSE + NASDAQ common stocks
- Criteria: 3-of-4 thresholds in the SAME direction (all positive or all negative):
    * 1D >= 3%, 7D >= 5%, 30D >= 15%, 90D >= 30%
    * or 1D <= -3%, 7D <= -5%, 30D <= -15%, 90D <= -30%
- Volume filter: daily dollar volume >= $250M
- Log: CSV (6 months retention) with:
    symbol, name, sector, timestamp, price, 1y target, daily dollar volume,
    pct_1d, pct_7d, pct_30d, pct_90d, pct_1y, pct_3y

Daily run (4pm ET via GitHub Actions):
- Scan universe, append matches to log (prune >6 months)
- Build two HTML reports and send to Telegram:
    * Rising Stars: today's daily volume >= $250M and < $1B
    * Big Ones: today's daily volume >= $1B

Backtest (manual, one-time as desired):
- python momentum_logger.py --mode backtest --start 2026-01-01
- Populates the same CSV using end-of-day data for each day.
"""

import csv
import os
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from zoneinfo import ZoneInfo

import requests
import yfinance as yf

from screener.screener import get_exchange_symbols, load_config


LOG_DIR = Path("logs")
LOG_PATH = LOG_DIR / "momentum_log.csv"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Thresholds for move criteria
THRESHOLDS = {"d1": 3.0, "d7": 5.0, "d30": 15.0, "d90": 30.0}

# Volume thresholds (dollar volume = price * volume)
MIN_DOLLAR_VOLUME = 250_000_000  # >= $250M
BIG_STOCK_DOLLAR_VOLUME = 1_000_000_000  # >= $1B


def _pct_change(series, days: int) -> Optional[float]:
    if len(series) < days + 1:
        return None
    recent = float(series.iloc[-1])
    past = float(series.iloc[-(days + 1)])
    if past == 0:
        return None
    return (recent - past) / past * 100.0


def _evaluate_symbol(symbol: str, as_of: Optional[datetime] = None) -> Optional[Dict]:
    """
    Evaluate a symbol as of a given datetime (ET). Uses daily bars from yfinance.
    Returns a dict with all logged fields if it passes filters, else None.
    """
    try:
        t = yf.Ticker(symbol)
        hist = t.history(period="max", auto_adjust=False, interval="1d")
        if hist is None or hist.empty or len(hist) < 252:
            return None

        close = hist["Close"]
        volume = hist["Volume"]

        # Restrict history up to as_of if provided
        if as_of is not None:
            idx = close.index
            tz = idx.tz if hasattr(idx, "tz") and idx.tz else ZoneInfo("America/New_York")
            as_of_aware = as_of if as_of.tzinfo is not None else as_of.replace(tzinfo=tz)
            close = close[close.index <= as_of_aware]
            volume = volume[volume.index <= as_of_aware]

        if len(close) < 252:
            return None

        d1 = _pct_change(close, 1)
        d7 = _pct_change(close, 7)
        d30 = _pct_change(close, 30)
        d90 = _pct_change(close, 90)
        d1y = _pct_change(close, 252)
        d3y = _pct_change(close, 756)

        if any(x is None for x in (d1, d7, d30, d90)):
            return None

        # 3-of-4 in same direction (all positive or all negative)
        pos = [
            d1 >= THRESHOLDS["d1"],
            d7 >= THRESHOLDS["d7"],
            d30 >= THRESHOLDS["d30"],
            d90 >= THRESHOLDS["d90"],
        ]
        neg = [
            d1 <= -THRESHOLDS["d1"],
            d7 <= -THRESHOLDS["d7"],
            d30 <= -THRESHOLDS["d30"],
            d90 <= -THRESHOLDS["d90"],
        ]
        if sum(pos) < 3 and sum(neg) < 3:
            return None

        last_price = float(close.iloc[-1])
        last_volume = float(volume.iloc[-1])
        dollar_volume = last_price * last_volume
        if dollar_volume < MIN_DOLLAR_VOLUME:
            return None

        info = t.info or {}
        name = info.get("longName") or info.get("shortName") or symbol
        sector = info.get("sector") or "Other"
        target = info.get("targetMeanPrice")
        target = round(float(target), 2) if isinstance(target, (int, float)) else None

        ts = as_of or datetime.now(ZoneInfo("America/New_York"))
        return {
            "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": symbol,
            "name": name,
            "sector": sector,
            "price": f"{last_price:.2f}",
            "target_1y": f"{target:.2f}" if target is not None else "",
            "dollar_volume": f"{dollar_volume:.2f}",
            "pct_1d": f"{d1:.2f}",
            "pct_7d": f"{d7:.2f}",
            "pct_30d": f"{d30:.2f}",
            "pct_90d": f"{d90:.2f}",
            "pct_1y": f"{d1y:.2f}" if d1y is not None else "",
            "pct_3y": f"{d3y:.2f}" if d3y is not None else "",
        }
    except Exception:
        return None


def _load_universe(config: Dict, quick_sample: bool = False) -> List[str]:
    exchanges = config.get("exchanges", ["NYSE", "NASDAQ"])
    symbols: List[str] = []
    for ex in exchanges:
        symbols.extend(get_exchange_symbols(ex))
    symbols = list(dict.fromkeys(symbols))

    if quick_sample:
        # Small fixed sample for manual testing
        sample = [
            "AAPL",
            "MSFT",
            "NVDA",
            "AMZN",
            "META",
            "TSLA",
            "ABNB",
            "COP",
            "DVN",
            "DOCN",
        ]
        print(f"MOMENTUM_QUICK_SAMPLE=1: using {len(sample)} symbols")
        return sample

    print(f"Universe size: {len(symbols)} symbols")
    return symbols


def _read_rows() -> List[Dict]:
    if not LOG_PATH.exists():
        return []
    with open(LOG_PATH, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _write_rows(rows: List[Dict]) -> None:
    fieldnames = [
        "timestamp",
        "symbol",
        "name",
        "sector",
        "price",
        "target_1y",
        "dollar_volume",
        "pct_1d",
        "pct_7d",
        "pct_30d",
        "pct_90d",
        "pct_1y",
        "pct_3y",
    ]
    with open(LOG_PATH, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        if rows:
            w.writerows(rows)


def _prune_six_months(rows: List[Dict]) -> List[Dict]:
    cutoff = datetime.now(ZoneInfo("America/New_York")) - timedelta(days=180)
    kept: List[Dict] = []
    for r in rows:
        try:
            ts = datetime.strptime(r["timestamp"], "%Y-%m-%d %H:%M:%S")
        except Exception:
            continue
        if ts >= cutoff.replace(tzinfo=None):
            kept.append(r)
    return kept


def _classify_row_direction(row: Dict) -> str:
    """
    Return "pos", "neg", or "mixed" based on 3-of-4 same-direction rule for that row.
    """
    try:
        d1 = float(row.get("pct_1d") or 0.0)
        d7 = float(row.get("pct_7d") or 0.0)
        d30 = float(row.get("pct_30d") or 0.0)
        d90 = float(row.get("pct_90d") or 0.0)
    except ValueError:
        return "mixed"

    pos = [
        d1 >= THRESHOLDS["d1"],
        d7 >= THRESHOLDS["d7"],
        d30 >= THRESHOLDS["d30"],
        d90 >= THRESHOLDS["d90"],
    ]
    neg = [
        d1 <= -THRESHOLDS["d1"],
        d7 <= -THRESHOLDS["d7"],
        d30 <= -THRESHOLDS["d30"],
        d90 <= -THRESHOLDS["d90"],
    ]
    if sum(pos) >= 3:
        return "pos"
    if sum(neg) >= 3:
        return "neg"
    return "mixed"


def _build_group_html(
    title: str,
    rows: List[Dict],
    today_dollar_volume_by_symbol: Dict[str, float],
) -> str:
    """
    Build HTML report:
    - Line 1: blank (simulated as leading <br>)
    - Per stock (sorted by name):
        Line 2-like: Name, (Ticker), Sector, price_now, (target_now), daily dollar volume today
        Subsequent lines: timestamp, price, (target_1y), daily_dollar_volume_at_ts,
                          %1D, %7D, %30D, %90D, %1Y
                          row text green if >=3 positive criteria, red if >=3 negative.
    """
    style_body = "background-color: white; color: black; font-family: Arial, sans-serif; padding: 1em;"

    if not rows:
        return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>{title}</title></head>
<body style="{style_body}">
<br>
<h2>{title}</h2>
<p>No matches in the last 6 months.</p>
</body></html>
"""

    # Group by symbol
    by_symbol: Dict[str, List[Dict]] = {}
    for r in rows:
        sym = r.get("symbol", "")
        by_symbol.setdefault(sym, []).append(r)

    def sort_key(sym: str) -> str:
        first = by_symbol[sym][0]
        return (first.get("name") or sym).upper()

    parts: List[str] = [
        "<!DOCTYPE html>",
        '<html><head><meta charset="utf-8">',
        f"<title>{title}</title>",
        "</head>",
        f'<body style="{style_body}">',
        "<br>",
        f"<h2>{title}</h2>",
    ]

    for symbol in sorted(by_symbol.keys(), key=sort_key):
        entries = by_symbol[symbol]
        first = entries[0]
        name = first.get("name", symbol)
        sector = first.get("sector", "")

        # Current price/target and today's dollar volume (if available)
        price_now = ""
        target_now = ""
        dv_today = today_dollar_volume_by_symbol.get(symbol)

        try:
            t = yf.Ticker(symbol)
            info = t.info or {}
            p = info.get("regularMarketPrice") or info.get("previousClose")
            if isinstance(p, (int, float)):
                price_now = f"{float(p):.2f}"
            target = info.get("targetMeanPrice")
            if isinstance(target, (int, float)):
                target_now = f"{float(target):.2f}"
        except Exception:
            # fall back to last logged values
            last = entries[-1]
            price_now = last.get("price", "")
            target_now = last.get("target_1y", "")

        dv_today_str = f"{dv_today:,.0f}" if isinstance(dv_today, (int, float, float)) else ""

        parts.append(
            f"<h3>{name}, ({symbol}), {sector}, {price_now}, ({target_now}), {dv_today_str}</h3>"
        )

        # Table of logged rows
        parts.append(
            '<table border="1" cellpadding="4" cellspacing="0" '
            'style="border-collapse: collapse; border-color: #ccc; font-size: 13px;">'
        )
        parts.append(
            "<tr>"
            "<th>Timestamp</th>"
            "<th>Price</th>"
            "<th>Target 1Y</th>"
            "<th>Daily $ Volume</th>"
            "<th>%1D</th>"
            "<th>%7D</th>"
            "<th>%30D</th>"
            "<th>%90D</th>"
            "<th>%1Y</th>"
            "</tr>"
        )

        for e in entries:
            direction = _classify_row_direction(e)
            color = "green" if direction == "pos" else "red" if direction == "neg" else "black"
            ts = e.get("timestamp", "")
            price = e.get("price", "")
            target_1y = e.get("target_1y", "")
            dv = e.get("dollar_volume", "")
            p1 = e.get("pct_1d", "")
            p7 = e.get("pct_7d", "")
            p30 = e.get("pct_30d", "")
            p90 = e.get("pct_90d", "")
            p1y = e.get("pct_1y", "")
            parts.append(
                f'<tr style="color: {color};">'
                f"<td>{ts}</td>"
                f"<td>{price}</td>"
                f"<td>{target_1y}</td>"
                f"<td>{dv}</td>"
                f"<td>{p1}</td>"
                f"<td>{p7}</td>"
                f"<td>{p30}</td>"
                f"<td>{p90}</td>"
                f"<td>{p1y}</td>"
                "</tr>"
            )
        parts.append("</table>")
        parts.append("<br>")

    parts.append("</body></html>")
    return "\n".join(parts)


def _send_telegram_document(content: str, filename: str, mime_type: str, caption: str) -> bool:
    token = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    chat_id = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()
    if not token or not chat_id:
        print("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be set to send Telegram reports.")
        return False

    suffix = ".html" if "html" in mime_type else ".csv"
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=suffix, delete=False, encoding="utf-8"
        ) as f:
            f.write(content)
            tmp_path = f.name
        try:
            url = f"https://api.telegram.org/bot{token}/sendDocument"
            with open(tmp_path, "rb") as doc:
                files = {"document": (filename, doc, mime_type)}
                data = {"chat_id": chat_id, "caption": caption}
                r = requests.post(url, data=data, files=files, timeout=60)
            if r.status_code != 200:
                print(f"Telegram error {r.status_code}: {r.text[:300]}")
                return False
            print(f"Telegram document sent: {filename}")
            return True
        finally:
            os.unlink(tmp_path)
    except Exception as e:
        print(f"Failed to send Telegram document: {e}")
        return False


def _build_and_send_reports(today_matches: List[Dict]) -> None:
    """
    Build Rising Stars / Big Ones HTML reports from the full log, filtered by today's dollar volume.
    """
    if not today_matches:
        print("No matches today; sending empty reports.")

    # Map symbol -> today's dollar volume
    today_dollar_volumes: Dict[str, float] = {}
    for m in today_matches:
        sym = m.get("symbol", "")
        try:
            dv = float(m.get("dollar_volume") or 0.0)
        except ValueError:
            dv = 0.0
        today_dollar_volumes[sym] = dv

    all_rows = _read_rows()
    all_rows = _prune_six_months(all_rows)

    # Partition symbols into Rising Stars and Big Ones based on today's volume
    rising_syms = {
        s
        for s, dv in today_dollar_volumes.items()
        if MIN_DOLLAR_VOLUME <= dv < BIG_STOCK_DOLLAR_VOLUME
    }
    big_syms = {s for s, dv in today_dollar_volumes.items() if dv >= BIG_STOCK_DOLLAR_VOLUME}

    rising_rows = [r for r in all_rows if r.get("symbol") in rising_syms]
    big_rows = [r for r in all_rows if r.get("symbol") in big_syms]

    # Build HTML
    rising_html = _build_group_html("Rising Stars", rising_rows, today_dollar_volumes)
    big_html = _build_group_html("Big Ones", big_rows, today_dollar_volumes)

    today_str = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    _send_telegram_document(
        rising_html,
        f"rising_stars_{today_str}.html",
        "text/html",
        caption=f"Rising Stars — {today_str}",
    )
    _send_telegram_document(
        big_html,
        f"big_ones_{today_str}.html",
        "text/html",
        caption=f"Big Ones — {today_str}",
    )


def run_daily_scan(as_of: Optional[datetime] = None) -> None:
    """
    Scan universe as of as_of (defaults to now ET), log matches, prune to 6 months,
    then send Rising Stars and Big Ones reports.
    """
    config = load_config("config.yaml")
    quick = (os.getenv("MOMENTUM_QUICK_SAMPLE") or "").strip().lower() in ("1", "true", "yes")
    universe = _load_universe(config, quick_sample=quick)

    now_et = as_of or datetime.now(ZoneInfo("America/New_York"))
    print(
        f"Scanning {len(universe)} symbols for 3-of-4 criteria (all positive or all negative) "
        f"as of {now_et.strftime('%Y-%m-%d %H:%M:%S %Z')}..."
    )

    existing = _read_rows()
    today_matches: List[Dict] = []

    for i, sym in enumerate(universe):
        rec = _evaluate_symbol(sym, as_of=now_et)
        if rec:
            today_matches.append(rec)
            print(
                f"  [MATCH] {sym}: 1D {rec['pct_1d']}%, 7D {rec['pct_7d']}%, "
                f"30D {rec['pct_30d']}%, 90D {rec['pct_90d']}%, "
                f"DV ${float(rec['dollar_volume']):,.0f}"
            )
        if (i + 1) % 100 == 0:
            print(f"  Progress: {i + 1}/{len(universe)}", flush=True)
        time.sleep(0.05)

    all_rows = existing + today_matches
    all_rows = _prune_six_months(all_rows)
    _write_rows(all_rows)
    print(
        f"Appended {len(today_matches)} rows. Total (<=6 months): {len(all_rows)}. "
        f"Log: {LOG_PATH.resolve()}"
    )

    _build_and_send_reports(today_matches)


def run_backtest(start_date: str = "2026-01-01") -> None:
    """
    Backtest: simulate daily scans from start_date to today (ET), writing to the same CSV.
    This does NOT send reports; it only populates the log.
    """
    config = load_config("config.yaml")
    quick = (os.getenv("MOMENTUM_QUICK_SAMPLE") or "").strip().lower() in ("1", "true", "yes")
    universe = _load_universe(config, quick_sample=quick)

    start_d = datetime.strptime(start_date, "%Y-%m-%d").date()
    today_d = datetime.now(ZoneInfo("America/New_York")).date()

    existing = _read_rows()
    all_new: List[Dict] = []

    d = start_d
    day_count = 0
    while d <= today_d:
        day_count += 1
        as_of = datetime(d.year, d.month, d.day, 16, 0, 0, tzinfo=ZoneInfo("America/New_York"))
        print(f"\nBacktest day {day_count}: {d} (as_of {as_of})", flush=True)
        for sym in universe:
            rec = _evaluate_symbol(sym, as_of=as_of)
            if rec:
                all_new.append(rec)
            time.sleep(0.02)
        d += timedelta(days=1)

    all_rows = existing + all_new
    all_rows = _prune_six_months(all_rows)
    _write_rows(all_rows)
    print(
        f"\nBacktest complete from {start_date} to {today_d}. "
        f"New rows: {len(all_new)}. Total: {len(all_rows)}. Log: {LOG_PATH.resolve()}"
    )


if __name__ == "__main__":
    import argparse
    from dotenv import load_dotenv

    load_dotenv()

    parser = argparse.ArgumentParser(
        description=(
            "Momentum logger: NYSE/NASDAQ, 3-of-4 criteria (all same sign), "
            "volume >= $250M, 6-month log, HTML Telegram reports."
        )
    )
    parser.add_argument("--mode", choices=["daily", "backtest"], default="daily")
    parser.add_argument(
        "--start", default="2026-01-01", help="Backtest start date (YYYY-MM-DD)"
    )
    args = parser.parse_args()

    if args.mode == "daily":
        run_daily_scan()
    else:
        run_backtest(args.start)

