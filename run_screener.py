"""
MarketScout: Weekdays 4pm EST — scan stocks with daily $ vol > $250M,
3-of-4 criteria (1D 3%, 7D 5%, 30D 15%, 90D 30%) same sign.
Log 6 months. Send Rising Stars ($250M–$1B) and Big Ones (>= $1B) HTML reports to Telegram.
"""
import csv
import os
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional, Tuple

import requests
import yfinance as yf

from screener.screener import get_exchange_symbols, load_config

LOG_DIR = Path("logs")
LOG_PATH = LOG_DIR / "screener_log.csv"
LOG_DIR.mkdir(parents=True, exist_ok=True)


def _pct_change(series, days: int) -> Optional[float]:
    if len(series) < days + 1:
        return None
    recent = float(series.iloc[-1])
    past = float(series.iloc[-(days + 1)])
    if past == 0:
        return None
    return (recent - past) / past * 100.0


# Liquid symbols for quick backtest (set SCREENER_QUICK_SAMPLE=1)
QUICK_SAMPLE_SYMBOLS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "BRK-B", "UNH", "JNJ",
    "JPM", "V", "PG", "MA", "HD", "DIS", "BAC", "XOM", "CVX", "KO", "PEP", "COST",
    "WMT", "MCD", "ABT", "NEE", "TMO", "AVGO", "ACN", "DHR", "NKE", "ORCL", "CRM",
    "AMD", "INTC", "IBM", "GE", "CAT", "GS", "MS", "BA", "DOW", "HON", "UPS", "LOW",
]


def _load_universe(config: Dict, quick_sample: bool = False) -> List[str]:
    if quick_sample:
        print(f"  SCREENER_QUICK_SAMPLE: using {len(QUICK_SAMPLE_SYMBOLS)} symbols")
        return list(QUICK_SAMPLE_SYMBOLS)
    symbols = []
    for ex in config.get("exchanges", ["NYSE", "NASDAQ"]):
        symbols.extend(get_exchange_symbols(ex))
    return list(dict.fromkeys(symbols))


def _evaluate_symbol(
    symbol: str,
    config: Dict,
    as_of: Optional[datetime] = None,
) -> Optional[Dict]:
    """Return log row if stock has $ vol > min and 3-of-4 same-sign criteria."""
    try:
        t = yf.Ticker(symbol)
        hist = t.history(period="max", auto_adjust=False, interval="1d")
        if hist is None or hist.empty or len(hist) < 252:
            return None

        close = hist["Close"]
        volume = hist["Volume"]
        if as_of is not None:
            tz = close.index.tz if getattr(close.index, "tz", None) else ZoneInfo("America/New_York")
            as_of_aware = as_of if as_of.tzinfo else as_of.replace(tzinfo=tz)
            close = close[close.index <= as_of_aware]
            volume = volume.reindex(close.index).fillna(0)
        if len(close) < 252:
            return None

        # Daily dollar volume (most recent day)
        last_price = float(close.iloc[-1])
        last_vol = float(volume.iloc[-1]) if len(volume) else 0
        dollar_vol = last_price * last_vol
        min_dv = config.get("min_dollar_volume", 250_000_000)
        if dollar_vol < min_dv:
            return None

        d1 = _pct_change(close, 1)
        d7 = _pct_change(close, 7)
        d30 = _pct_change(close, 30)
        d90 = _pct_change(close, 90)
        d1y = _pct_change(close, 252)
        d3y = _pct_change(close, 756)
        if any(x is None for x in (d1, d7, d30, d90)):
            return None

        th = config.get("thresholds", {})
        t1, t7, t30, t90 = th.get("d1", 3), th.get("d7", 5), th.get("d30", 15), th.get("d90", 30)
        pos = [d1 >= t1, d7 >= t7, d30 >= t30, d90 >= t90]
        neg = [d1 <= -t1, d7 <= -t7, d30 <= -t30, d90 <= -t90]
        if sum(pos) >= 3:
            direction = "positive"
        elif sum(neg) >= 3:
            direction = "negative"
        else:
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
            "dollar_volume": f"{dollar_vol:,.0f}",
            "pct_1d": f"{d1:.2f}",
            "pct_7d": f"{d7:.2f}",
            "pct_30d": f"{d30:.2f}",
            "pct_90d": f"{d90:.2f}",
            "pct_1y": f"{d1y:.2f}" if d1y is not None else "",
            "pct_3y": f"{d3y:.2f}" if d3y is not None else "",
            "direction": direction,
        }
    except Exception:
        return None


def _read_rows() -> List[Dict]:
    if not LOG_PATH.exists():
        return []
    with open(LOG_PATH, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _write_rows(rows: List[Dict]) -> None:
    fieldnames = [
        "timestamp", "symbol", "name", "sector", "price", "target_1y", "dollar_volume",
        "pct_1d", "pct_7d", "pct_30d", "pct_90d", "pct_1y", "pct_3y", "direction",
    ]
    with open(LOG_PATH, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        if rows:
            w.writerows(rows)


def _prune_six_months(rows: List[Dict]) -> List[Dict]:
    cutoff = datetime.now() - timedelta(days=180)
    kept = []
    for r in rows:
        try:
            ts = datetime.strptime(r["timestamp"], "%Y-%m-%d %H:%M:%S")
        except Exception:
            continue
        if ts >= cutoff:
            kept.append(r)
    return kept


def _parse_dollar_volume(s: str) -> float:
    """Parse dollar_volume string (e.g. '1,234,567') to float."""
    try:
        return float(str(s).replace(",", ""))
    except (ValueError, TypeError):
        return 0.0


def _build_report_html(
    rows: List[Dict],
    title: str,
    config: Dict,
) -> str:
    """Build one HTML report from log data only (6 months). Rising Stars: $250M-$1B, Big Ones: >= $1B.

    Per stock (sorted by name):
    - Blank line
    - Header line: Name, (Ticker), Sector, price_now, (target_now), latest dollar volume
    - Then an HTML table with rows = log entries and columns = timestamp, price, target, dollar volume, %1D, %7D, %30D, %90D, %1Y.
      Each data row is green when 3+ criteria are positive, red when 3+ are negative.
    """
    min_dv = config.get("min_dollar_volume", 250_000_000)
    rising_max = config.get("rising_stars_max_dollar_volume", 1_000_000_000)
    big_min = config.get("big_ones_min_dollar_volume", 1_000_000_000)

    by_symbol: Dict[str, List[Dict]] = {}
    for r in rows:
        by_symbol.setdefault(r["symbol"], []).append(r)

    # Sort entries by timestamp within each symbol (oldest first for display)
    for sym in by_symbol:
        entries = by_symbol[sym]
        entries.sort(key=lambda e: e.get("timestamp", ""))

    # Filter by dollar_volume from MOST RECENT log entry (no live fetch)
    symbols_in_scope = []
    for sym, entries in by_symbol.items():
        latest = entries[-1]
        dv = _parse_dollar_volume(latest.get("dollar_volume", 0))
        if title == "Rising Stars" and min_dv <= dv < rising_max:
            symbols_in_scope.append(sym)
        elif title == "Big Ones" and dv >= big_min:
            symbols_in_scope.append(sym)

    symbols_in_scope.sort(key=lambda s: (by_symbol[s][0].get("name") or s).upper())
    style = "background-color: white; color: black; font-family: sans-serif; padding: 1em;"
    lines = [
        "<!DOCTYPE html><html><head><meta charset='utf-8'><title>" + title + "</title></head>",
        f"<body style='{style}'>",
        "",
        f"<h2 style='color: black;'>{title}</h2>",
        f"<p style='color: black;'>Generated: {datetime.now(ZoneInfo('America/New_York')).strftime('%Y-%m-%d %H:%M %Z')} — Data from log (6 months)</p>",
    ]

    # For each symbol, show a header then a table of its logged appearances.
    for symbol in symbols_in_scope:
        entries = by_symbol[symbol]
        name = entries[0].get("name", symbol)
        sector = entries[0].get("sector", "")
        latest = entries[-1]
        price_now = latest.get("price", "")
        target_now = latest.get("target_1y", "")
        vol_str = latest.get("dollar_volume", "")
        if vol_str:
            try:
                v = float(str(vol_str).replace(",", ""))
                vol_str = f"${v/1e6:.2f}M" if v >= 1e6 else f"${v:,.0f}"
            except (ValueError, TypeError):
                pass

        # Blank line (spacing) + header line
        lines.append("")
        lines.append(
            f"<p style='color: black; margin-top: 1.2em;'><strong>{name}, ({symbol}), {sector}, {price_now}, ({target_now}), {vol_str}</strong></p>"
        )

        # Table header
        lines.append(
            "<table border='1' cellspacing='0' cellpadding='4' "
            "style='border-collapse: collapse; margin-top: 0.4em; color: black; background-color: white;'>"
        )
        lines.append(
            "<tr style='font-weight: bold; background-color: #f0f0f0;'>"
            "<th>Timestamp</th><th>Price</th><th>Target (1Y)</th><th>Dollar Vol</th>"
            "<th>%1D</th><th>%7D</th><th>%30D</th><th>%90D</th><th>%1Y</th>"
            "</tr>"
        )

        # Table rows
        for e in entries:
            color = "green" if e.get("direction") == "positive" else "red"
            ts = e.get("timestamp", "")
            price = e.get("price", "")
            target = e.get("target_1y", "")
            dv = e.get("dollar_volume", "")
            p1 = e.get("pct_1d", "")
            p7 = e.get("pct_7d", "")
            p30 = e.get("pct_30d", "")
            p90 = e.get("pct_90d", "")
            p1y = e.get("pct_1y", "")
            lines.append(
                f"<tr style='color: {color};'>"
                f"<td>{ts}</td><td>{price}</td><td>{target}</td><td>{dv}</td>"
                f"<td>{p1}</td><td>{p7}</td><td>{p30}</td><td>{p90}</td><td>{p1y}</td>"
                "</tr>"
            )

        lines.append("</table>")

    lines.append("</body></html>")
    return "\n".join(lines)


def _send_telegram_html(html: str, filename: str, caption: str) -> bool:
    token = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    chat_id = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()
    if not token or not chat_id:
        print("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID required.")
        return False
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".html", delete=False, encoding="utf-8") as f:
            f.write(html)
            tmp = f.name
        try:
            url = f"https://api.telegram.org/bot{token}/sendDocument"
            with open(tmp, "rb") as doc:
                r = requests.post(url, data={"chat_id": chat_id, "caption": caption}, files={"document": (filename, doc, "text/html")}, timeout=60)
            if r.status_code != 200:
                print(f"Telegram error {r.status_code}: {r.text[:300]}")
                return False
            print(f"Sent: {filename}")
            return True
        finally:
            os.unlink(tmp)
    except Exception as e:
        print(f"Send failed: {e}")
        return False


def run_scan(as_of: Optional[datetime] = None, send_reports: bool = True) -> None:
    config = load_config("config.yaml")
    quick = (os.getenv("SCREENER_QUICK_SAMPLE") or "").strip().lower() in ("1", "true", "yes")
    universe = _load_universe(config, quick_sample=quick)
    now = as_of or datetime.now(ZoneInfo("America/New_York"))
    print(f"Scanning {len(universe)} symbols (3-of-4 same sign, $ vol > $250M)...")

    existing = _read_rows()
    new_rows = []
    for i, sym in enumerate(universe):
        rec = _evaluate_symbol(sym, config, as_of=now)
        if rec:
            new_rows.append(rec)
            print(f"  [MATCH] {sym}: {rec['direction']} — 1D {rec['pct_1d']}%, 7D {rec['pct_7d']}%, 30D {rec['pct_30d']}%, 90D {rec['pct_90d']}%")
        if (i + 1) % 200 == 0:
            print(f"  Progress: {i + 1}/{len(universe)}", flush=True)
        time.sleep(0.08)

    all_rows = existing + new_rows
    all_rows = _prune_six_months(all_rows)
    _write_rows(all_rows)
    print(f"Appended {len(new_rows)}. Total (<=6mo): {len(all_rows)}. Log: {LOG_PATH}")

    if send_reports and all_rows:
        rising_html = _build_report_html(all_rows, "Rising Stars", config)
        big_html = _build_report_html(all_rows, "Big Ones", config)
        (LOG_DIR / "MarketScout_Rising_Stars.html").write_text(rising_html, encoding="utf-8")
        (LOG_DIR / "MarketScout_Big_Ones.html").write_text(big_html, encoding="utf-8")
        _send_telegram_html(rising_html, "MarketScout_Rising_Stars.html", "Rising Stars ($250M–$1B)")
        _send_telegram_html(big_html, "MarketScout_Big_Ones.html", "Big Ones (>= $1B)")


def run_report_only(start_date: Optional[str] = None) -> None:
    """Read log, build and send both HTML reports to Telegram (no scan).

    If start_date is provided (YYYY-MM-DD), only include rows with
    timestamp >= start_date and <= now.
    """
    config = load_config("config.yaml")
    rows = _read_rows()
    rows = _prune_six_months(rows)
    if start_date:
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d")
        except ValueError:
            print(f"Invalid start_date '{start_date}', expected YYYY-MM-DD. Using full 6-month log instead.")
        else:
            end = datetime.now(ZoneInfo("America/New_York")).replace(tzinfo=None)
            filtered: List[Dict] = []
            for r in rows:
                try:
                    ts = datetime.strptime(r["timestamp"], "%Y-%m-%d %H:%M:%S")
                except Exception:
                    continue
                if start <= ts <= end:
                    filtered.append(r)
            rows = filtered

    if not rows:
        print("No data in log. Run a scan or backtest first.")
        return
    rising_html = _build_report_html(rows, "Rising Stars", config)
    big_html = _build_report_html(rows, "Big Ones", config)
    # Save locally
    (LOG_DIR / "MarketScout_Rising_Stars.html").write_text(rising_html, encoding="utf-8")
    (LOG_DIR / "MarketScout_Big_Ones.html").write_text(big_html, encoding="utf-8")
    print(f"Reports saved to {LOG_DIR}/")
    _send_telegram_html(rising_html, "MarketScout_Rising_Stars.html", "Rising Stars ($250M–$1B)")
    _send_telegram_html(big_html, "MarketScout_Big_Ones.html", "Big Ones (>= $1B)")
    print("Reports sent to Telegram.")


def run_backtest(start_date: str = "2026-01-01") -> None:
    config = load_config("config.yaml")
    quick = (os.getenv("SCREENER_QUICK_SAMPLE") or "").strip().lower() in ("1", "true", "yes")
    universe = _load_universe(config, quick_sample=quick)
    start_d = datetime.strptime(start_date, "%Y-%m-%d").date()
    today = datetime.now(ZoneInfo("America/New_York")).date()
    existing = _read_rows()
    all_new = []
    d = start_d
    day_count = 0

    while d <= today:
        # Weekdays only
        if d.weekday() >= 5:
            d += timedelta(days=1)
            continue
        day_count += 1
        as_of = datetime(d.year, d.month, d.day, 16, 0, 0, tzinfo=ZoneInfo("America/New_York"))
        print(f"\nBacktest day {day_count}: {d}...", flush=True)
        for sym in universe:
            rec = _evaluate_symbol(sym, config, as_of=as_of)
            if rec:
                all_new.append(rec)
            time.sleep(0.05)
        d += timedelta(days=1)

    all_rows = existing + all_new
    all_rows = _prune_six_months(all_rows)
    _write_rows(all_rows)
    print(f"\nBacktest done. New: {len(all_new)}. Total: {len(all_rows)}. Log: {LOG_PATH}")


if __name__ == "__main__":
    import argparse
    from dotenv import load_dotenv
    load_dotenv()

    parser = argparse.ArgumentParser(description="MarketScout screener")
    parser.add_argument("--mode", choices=["scan", "backtest", "report"], default="scan")
    parser.add_argument("--start", default="2026-01-01", help="Backtest start YYYY-MM-DD")
    parser.add_argument("--no-send", action="store_true", help="Do not send reports to Telegram after scan")
    args = parser.parse_args()

    if args.mode == "scan":
        run_scan(send_reports=not args.no_send)
    elif args.mode == "report":
        run_report_only(start_date=args.start)
    else:
        run_backtest(args.start)
