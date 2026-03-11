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


def _load_universe(config: Dict) -> List[str]:
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


def _get_today_dollar_volume(symbol: str) -> Optional[float]:
    try:
        t = yf.Ticker(symbol)
        hist = t.history(period="5d")
        if hist is None or hist.empty:
            return None
        close = hist["Close"].iloc[-1]
        vol = hist["Volume"].iloc[-1]
        return float(close) * float(vol)
    except Exception:
        return None


def _build_report_html(
    rows: List[Dict],
    title: str,
    config: Dict,
) -> str:
    """Build one HTML report (Rising Stars or Big Ones). Filter by today's $ vol."""
    min_dv = config.get("min_dollar_volume", 250_000_000)
    rising_max = config.get("rising_stars_max_dollar_volume", 1_000_000_000)
    big_min = config.get("big_ones_min_dollar_volume", 1_000_000_000)

    by_symbol: Dict[str, List[Dict]] = {}
    for r in rows:
        by_symbol.setdefault(r["symbol"], []).append(r)

    # Filter symbols by today's dollar volume
    symbols_in_scope = []
    for sym in by_symbol:
        dv = _get_today_dollar_volume(sym)
        if dv is None:
            continue
        if title == "Rising Stars" and min_dv <= dv < rising_max:
            symbols_in_scope.append((sym, dv))
        elif title == "Big Ones" and dv >= big_min:
            symbols_in_scope.append((sym, dv))
        time.sleep(0.05)

    def sort_key(item: Tuple[str, float]) -> str:
        return (by_symbol[item[0]][0].get("name") or item[0]).upper()

    symbols_in_scope.sort(key=sort_key)
    style = "background-color: white; color: black; font-family: sans-serif; padding: 1em;"
    lines = [
        "<!DOCTYPE html><html><head><meta charset='utf-8'><title>" + title + "</title></head>",
        f"<body style='{style}'>",
        "",
        f"<h2 style='color: black;'>{title}</h2>",
        f"<p style='color: black;'>Generated: {datetime.now(ZoneInfo('America/New_York')).strftime('%Y-%m-%d %H:%M %Z')}</p>",
    ]

    for symbol, vol_today in symbols_in_scope:
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
        vol_str = f"${vol_today/1e6:.2f}M" if vol_today >= 1e6 else f"${vol_today:,.0f}"

        lines.append(f"<p style='color: black; margin-top: 1.2em;'><strong>{name}, ({symbol}), {sector}, {price_now}, ({target_now}), {vol_str}</strong></p>")
        for e in entries:
            color = "green" if e.get("direction") == "positive" else "red"
            ts = e.get("timestamp", "")
            price = e.get("price", "")
            target = e.get("target_1y", "")
            dv = e.get("dollar_volume", "")
            p1, p7, p30, p90, p1y = e.get("pct_1d", ""), e.get("pct_7d", ""), e.get("pct_30d", ""), e.get("pct_90d", ""), e.get("pct_1y", "")
            lines.append(f"<p style='color: {color}; margin: 0.2em 0;'>{ts}, {price}, ({target}), {dv}, %1D: {p1}, %7D: {p7}, %30D: {p30}, %90D: {p90}, %1Y: {p1y}</p>")
        time.sleep(0.05)

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
    universe = _load_universe(config)
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
        _send_telegram_html(rising_html, "MarketScout_Rising_Stars.html", "Rising Stars ($250M–$1B)")
        _send_telegram_html(big_html, "MarketScout_Big_Ones.html", "Big Ones (>= $1B)")


def run_backtest(start_date: str = "2026-01-01") -> None:
    config = load_config("config.yaml")
    universe = _load_universe(config)
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
    parser.add_argument("--mode", choices=["scan", "backtest"], default="scan")
    parser.add_argument("--start", default="2026-01-01", help="Backtest start YYYY-MM-DD")
    parser.add_argument("--no-send", action="store_true", help="Do not send reports to Telegram after scan")
    args = parser.parse_args()

    if args.mode == "scan":
        run_scan(send_reports=not args.no_send)
    else:
        run_backtest(args.start)
