"""
Momentum logger: scan stocks with 3-of-4 criteria (1D>=3%, 7D>=5%, 30D>=15%, 90D>=30%),
all same sign (all positive or all negative). Log to CSV, keep 6 months. Daily at 4pm: send via Telegram.
"""
import csv
import os
import tempfile
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Dict, List, Optional

import requests
import yfinance as yf

from screener.screener import get_exchange_symbols, load_config

LOG_DIR = Path("logs")
LOG_PATH = LOG_DIR / "momentum_log.csv"
LOG_DIR.mkdir(parents=True, exist_ok=True)

THRESHOLDS = {"d1": 3.0, "d7": 5.0, "d30": 15.0, "d90": 30.0}


def _pct_change(series, days: int) -> Optional[float]:
    if len(series) < days + 1:
        return None
    recent = float(series.iloc[-1])
    past = float(series.iloc[-(days + 1)])
    if past == 0:
        return None
    return (recent - past) / past * 100.0


def _evaluate_symbol(symbol: str, as_of: Optional[datetime] = None) -> Optional[Dict]:
    try:
        t = yf.Ticker(symbol)
        hist = t.history(period="max", auto_adjust=False, interval="1d")
        if hist is None or hist.empty or len(hist) < 252:
            return None

        close = hist["Close"]
        if as_of is not None:
            # yfinance index is tz-aware; make as_of match
            tz = close.index.tz if hasattr(close.index, "tz") and close.index.tz else ZoneInfo("America/New_York")
            if as_of.tzinfo is None:
                as_of_aware = as_of.replace(tzinfo=tz)
            else:
                as_of_aware = as_of
            close = close[close.index <= as_of_aware]
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

        pos = [d1 >= THRESHOLDS["d1"], d7 >= THRESHOLDS["d7"], d30 >= THRESHOLDS["d30"], d90 >= THRESHOLDS["d90"]]
        neg = [d1 <= -THRESHOLDS["d1"], d7 <= -THRESHOLDS["d7"], d30 <= -THRESHOLDS["d30"], d90 <= -THRESHOLDS["d90"]]
        if sum(pos) < 3 and sum(neg) < 3:
            return None

        info = t.info or {}
        last_price = float(close.iloc[-1])
        name = info.get("longName") or info.get("shortName") or symbol
        sector = info.get("sector") or "Other"
        target = info.get("targetMeanPrice")
        target = round(float(target), 2) if isinstance(target, (int, float)) else None

        ts = as_of or datetime.now()
        return {
            "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": symbol,
            "name": name,
            "sector": sector,
            "price": f"{last_price:.2f}",
            "target_1y": f"{target:.2f}" if target is not None else "",
            "pct_1d": f"{d1:.2f}",
            "pct_7d": f"{d7:.2f}",
            "pct_30d": f"{d30:.2f}",
            "pct_90d": f"{d90:.2f}",
            "pct_1y": f"{d1y:.2f}" if d1y is not None else "",
            "pct_3y": f"{d3y:.2f}" if d3y is not None else "",
        }
    except Exception:
        return None


# Curated liquid symbols for quick sample (avoid warrants/delisted at start of exchange lists)
QUICK_SAMPLE_SYMBOLS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "BRK-B", "UNH", "JNJ",
    "JPM", "V", "PG", "MA", "HD", "DIS", "BAC", "ADBE", "CRM", "XOM", "CVX", "KO",
    "PEP", "COST", "WMT", "MCD", "ABT", "NEE", "TMO", "AVGO", "ACN", "DHR", "NKE",
    "PM", "BMY", "TXN", "HON", "ORCL", "UPS", "LOW", "AMD", "INTC", "IBM", "GE",
    "CAT", "DE", "GS", "MS", "AXP", "BA", "UNP", "RTX", "LMT", "SBUX", "MDT",
    "GILD", "AMGN", "VZ", "T", "CMCSA", "PYPL", "ISRG", "REGN", "PLD", "LRCX",
    "BKNG", "ADI", "SNPS", "CDNS", "KLAC", "AMAT", "PANW", "ABNB", "MRVL",
]

def _load_universe(config: Dict, quick_sample: bool = False) -> List[str]:
    if quick_sample:
        print(f"  MOMENTUM_QUICK_SAMPLE: using {len(QUICK_SAMPLE_SYMBOLS)} liquid symbols")
        return list(QUICK_SAMPLE_SYMBOLS)
    exchanges = config.get("exchanges", ["NYSE", "NASDAQ"])
    symbols = []
    for ex in exchanges:
        symbols.extend(get_exchange_symbols(ex))
    return list(dict.fromkeys(symbols))


def _read_rows() -> List[Dict]:
    if not LOG_PATH.exists():
        return []
    with open(LOG_PATH, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _write_rows(rows: List[Dict]) -> None:
    fieldnames = [
        "timestamp", "symbol", "name", "sector", "price", "target_1y",
        "pct_1d", "pct_7d", "pct_30d", "pct_90d", "pct_1y", "pct_3y",
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


def _build_noon_csv(rows: List[Dict]) -> str:
    """Build CSV: Line 1 blank. Per stock (sorted by name): header line, then log lines."""
    if not rows:
        return "\n"

    by_symbol: Dict[str, List[Dict]] = {}
    for r in rows:
        sym = r.get("symbol", "")
        by_symbol.setdefault(sym, []).append(r)

    def sort_key(sym: str) -> str:
        return (by_symbol[sym][0].get("name") or sym).upper()

    lines = [""]
    for symbol in sorted(by_symbol.keys(), key=sort_key):
        entries = by_symbol[symbol]
        name = entries[0].get("name", symbol)
        sector = entries[0].get("sector", "")

        try:
            t = yf.Ticker(symbol)
            info = t.info or {}
            price_now = info.get("regularMarketPrice") or info.get("previousClose") or entries[-1].get("price", "")
            target_now = info.get("targetMeanPrice")
            price_now = f"{float(price_now):.2f}" if isinstance(price_now, (int, float)) else str(price_now or "")
            target_now = f"{float(target_now):.2f}" if isinstance(target_now, (int, float)) else ""
        except Exception:
            price_now = entries[-1].get("price", "")
            target_now = entries[-1].get("target_1y", "")

        lines.append(f"{name}, ({symbol}), {sector}, {price_now}, {target_now}")
        for e in entries:
            ts = e.get("timestamp", "")
            price = e.get("price", "")
            target = e.get("target_1y", "")
            p1, p7, p30, p90, p1y = e.get("pct_1d", ""), e.get("pct_7d", ""), e.get("pct_30d", ""), e.get("pct_90d", ""), e.get("pct_1y", "")
            lines.append(f"{ts}, {price}, ({target}), {p1}, {p7}, {p30}, {p90}, {p1y}")
        time.sleep(0.1)

    return "\n".join(lines) + "\n"


def _build_noon_html(rows: List[Dict]) -> str:
    """Build HTML report: white background, black font. Works when empty too."""
    style = "background-color: white; color: black; font-family: sans-serif; padding: 1em;"
    if not rows:
        return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>Momentum Log</title></head>
<body style="{style}">
<h2 style="color: black;">MarketScout Momentum Log</h2>
<p style="color: black;">No data in the last 6 months. Run a daily scan or backtest to populate the log.</p>
<p style="color: black;">Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
</body>
</html>"""

    by_symbol: Dict[str, List[Dict]] = {}
    for r in rows:
        sym = r.get("symbol", "")
        by_symbol.setdefault(sym, []).append(r)

    def sort_key(sym: str) -> str:
        return (by_symbol[sym][0].get("name") or sym).upper()

    html_parts = [f'<html><head><meta charset="utf-8"><title>Momentum Log</title></head><body style="{style}">']
    html_parts.append(f'<h2 style="color: black;">MarketScout Momentum Log</h2>')
    html_parts.append(f'<p style="color: black;">Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")}</p>')

    for symbol in sorted(by_symbol.keys(), key=sort_key):
        entries = by_symbol[symbol]
        name = entries[0].get("name", symbol)
        sector = entries[0].get("sector", "")

        try:
            t = yf.Ticker(symbol)
            info = t.info or {}
            price_now = info.get("regularMarketPrice") or info.get("previousClose") or entries[-1].get("price", "")
            target_now = info.get("targetMeanPrice")
            price_now = f"{float(price_now):.2f}" if isinstance(price_now, (int, float)) else str(price_now or "")
            target_now = f"{float(target_now):.2f}" if isinstance(target_now, (int, float)) else ""
        except Exception:
            price_now = entries[-1].get("price", "")
            target_now = entries[-1].get("target_1y", "")

        html_parts.append(f'<h3 style="color: black; margin-top: 1.5em;">{name}, ({symbol}), {sector}, {price_now}, {target_now}</h3>')
        html_parts.append('<table border="1" cellpadding="6" cellspacing="0" style="border-color: black; color: black; background: white;">')
        html_parts.append("<tr><th>Timestamp</th><th>Price</th><th>Target</th><th>%1D</th><th>%7D</th><th>%30D</th><th>%90D</th><th>%1Y</th></tr>")
        for e in entries:
            ts = e.get("timestamp", "")
            price = e.get("price", "")
            target = e.get("target_1y", "")
            p1, p7, p30, p90, p1y = e.get("pct_1d", ""), e.get("pct_7d", ""), e.get("pct_30d", ""), e.get("pct_90d", ""), e.get("pct_1y", "")
            html_parts.append(f"<tr><td>{ts}</td><td>{price}</td><td>{target}</td><td>{p1}</td><td>{p7}</td><td>{p30}</td><td>{p90}</td><td>{p1y}</td></tr>")
        html_parts.append("</table>")
        time.sleep(0.1)

    html_parts.append("</body></html>")
    return "\n".join(html_parts)


def _send_telegram_document(content: str, filename: str, mime_type: str, caption: str = "") -> bool:
    """Send file via Telegram sendDocument."""
    token = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    chat_id = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()
    if not token or not chat_id:
        print("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID required to send.")
        return False

    ext = ".html" if "html" in mime_type else ".csv"
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=ext, delete=False, encoding="utf-8") as f:
            f.write(content)
            tmp_path = f.name
        try:
            url = f"https://api.telegram.org/bot{token}/sendDocument"
            with open(tmp_path, "rb") as doc:
                files = {"document": (filename, doc, mime_type)}
                data = {"chat_id": chat_id, "caption": caption or f"MarketScout — {datetime.now().strftime('%Y-%m-%d %H:%M')}"}
                r = requests.post(url, data=data, files=files, timeout=30)
            if r.status_code != 200:
                print(f"Telegram error {r.status_code}: {r.text[:300]}")
                return False
            print("Report sent via Telegram.")
            return True
        finally:
            os.unlink(tmp_path)
    except Exception as e:
        print(f"Failed to send: {e}")
        return False


def run_daily_scan(as_of: Optional[datetime] = None) -> None:
    """Scan universe, append matches, prune to 6 months."""
    config = load_config("config.yaml")
    quick = (os.getenv("MOMENTUM_QUICK_SAMPLE") or "").strip().lower() in ("1", "true", "yes")
    universe = _load_universe(config, quick_sample=quick)
    print(f"Scanning {len(universe)} symbols for 3-of-4 criteria (all positive or all negative)...")

    existing = _read_rows()
    new_rows = []
    now = as_of or datetime.now()
    for i, sym in enumerate(universe):
        rec = _evaluate_symbol(sym, as_of=now)
        if rec:
            new_rows.append(rec)
            print(f"  [MATCH] {sym}: 1D {rec['pct_1d']}%, 7D {rec['pct_7d']}%, 30D {rec['pct_30d']}%, 90D {rec['pct_90d']}%")
        if (i + 1) % 100 == 0:
            print(f"  Progress: {i + 1}/{len(universe)}", flush=True)
        time.sleep(0.1)

    all_rows = existing + new_rows
    all_rows = _prune_six_months(all_rows)
    _write_rows(all_rows)
    print(f"Appended {len(new_rows)}. Total (<=6mo): {len(all_rows)}. Log: {LOG_PATH}")


def run_noon_report() -> None:
    """Build report (HTML with white bg, black font) and send via Telegram."""
    rows = _read_rows()
    rows = _prune_six_months(rows)

    html_content = _build_noon_html(rows)
    out = LOG_DIR / "momentum_noon_report.html"
    with open(out, "w", encoding="utf-8") as f:
        f.write(html_content)
    print(f"Noon report saved to {out}")

    if not rows:
        print("No data in log. Sending empty-state report.")

    _send_telegram_document(html_content, "momentum_log.html", "text/html")


def run_backtest(start_date: str = "2026-01-01", send_csv: bool = False) -> None:
    """Simulate daily scans from start_date to today; optionally send CSV via Telegram."""
    start_d = datetime.strptime(start_date, "%Y-%m-%d").date()
    today = datetime.now().date()
    config = load_config("config.yaml")
    quick = (os.getenv("MOMENTUM_QUICK_SAMPLE") or "").strip().lower() in ("1", "true", "yes")
    universe = _load_universe(config, quick_sample=quick)

    existing = _read_rows()
    all_new = []
    d = start_d
    day_count = 0

    while d <= today:
        day_count += 1
        as_of = datetime(d.year, d.month, d.day, 12, 0, 0, tzinfo=ZoneInfo("America/New_York"))
        print(f"\nBacktest day {day_count}: {d}...", flush=True)
        for sym in universe:
            rec = _evaluate_symbol(sym, as_of=as_of)
            if rec:
                all_new.append(rec)
            time.sleep(0.05)
        d += timedelta(days=1)

    all_rows = existing + all_new
    all_rows = _prune_six_months(all_rows)
    _write_rows(all_rows)
    print(f"\nBacktest done. New rows: {len(all_new)}. Total: {len(all_rows)}. Log: {LOG_PATH}")

    if send_csv:
        html_content = _build_noon_html(all_rows)
        _send_telegram_document(
            html_content,
            f"momentum_backtest_{start_date}_to_{today.strftime('%Y-%m-%d')}.html",
            "text/html",
            caption=f"Momentum backtest {start_date} to {today}",
        )


if __name__ == "__main__":
    import argparse
    from dotenv import load_dotenv
    load_dotenv()

    p = argparse.ArgumentParser(description="Momentum logger: 3-of-4 criteria (all same sign), log to CSV")
    p.add_argument("--mode", choices=["daily", "noon", "backtest"], default="daily")
    p.add_argument("--start", default="2026-01-01", help="Backtest start (YYYY-MM-DD)")
    p.add_argument("--send", action="store_true", help="Send CSV via Telegram after backtest")
    args = p.parse_args()

    if args.mode == "daily":
        run_daily_scan()
    elif args.mode == "noon":
        run_noon_report()
    else:
        run_backtest(args.start, send_csv=args.send)
