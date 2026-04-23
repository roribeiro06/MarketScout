"""
MarketScout: Weekdays 4pm EST — scan stocks with daily $ vol > $250M,
min_matches of 1D / 7D / 30D same sign vs thresholds (config, default 2-of-3); %90D in log/report only.
Log 6 months. Send Rising Stars ($250M–$1B) and Big Ones (>= $1B) HTML reports to Telegram.
"""
import csv
import json
import os
import tempfile
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional, Tuple

import requests
import yfinance as yf

from screener.screener import get_exchange_symbols, load_config

LOG_DIR = Path("logs")
LOG_PATH = LOG_DIR / "screener_log.csv"
BACKTEST_CHECKPOINT_PATH = LOG_DIR / "backtest_checkpoint.json"
LOG_DIR.mkdir(parents=True, exist_ok=True)


def _next_weekday_after(last: date) -> date:
    d = last + timedelta(days=1)
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d


def _weekday_number_in_range(range_start: date, current: date) -> int:
    """1-based index: current is the Nth weekday on/after range_start."""
    n = 0
    d = range_start
    while d <= current:
        if d.weekday() < 5:
            n += 1
        if d == current:
            break
        d += timedelta(days=1)
    return n


def _read_backtest_checkpoint() -> Optional[Dict]:
    if not BACKTEST_CHECKPOINT_PATH.exists():
        return None
    try:
        return json.loads(BACKTEST_CHECKPOINT_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None


def _write_backtest_checkpoint(last_completed: date, start_date: str) -> None:
    BACKTEST_CHECKPOINT_PATH.write_text(
        json.dumps(
            {"last_completed": last_completed.isoformat(), "start_date": start_date},
            indent=2,
        ),
        encoding="utf-8",
    )


def _clear_backtest_checkpoint() -> None:
    try:
        if BACKTEST_CHECKPOINT_PATH.exists():
            BACKTEST_CHECKPOINT_PATH.unlink()
    except Exception:
        pass


def _pct_change(series, days: int) -> Optional[float]:
    if len(series) < days + 1:
        return None
    recent = float(series.iloc[-1])
    past = float(series.iloc[-(days + 1)])
    if past == 0:
        return None
    return (recent - past) / past * 100.0


def _load_universe(config: Dict) -> List[str]:
    """Always use full NYSE + NASDAQ universe."""
    symbols = []
    for ex in config.get("exchanges", ["NYSE", "NASDAQ"]):
        symbols.extend(get_exchange_symbols(ex))
    return list(dict.fromkeys(symbols))


def _evaluate_symbol(
    symbol: str,
    config: Dict,
    as_of: Optional[datetime] = None,
) -> Optional[Dict]:
    """Return log row if $ vol > min and min_matches of 1D/7D/30D meet same-sign thresholds."""
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
        if any(x is None for x in (d1, d7, d30)):
            return None

        th = config.get("thresholds", {})
        t1, t7, t30 = th.get("d1", 5), th.get("d7", 10), th.get("d30", 20)
        min_matches = int(th.get("min_matches", 2))
        min_matches = max(1, min(3, min_matches))
        pos = [d1 >= t1, d7 >= t7, d30 >= t30]
        neg = [d1 <= -t1, d7 <= -t7, d30 <= -t30]
        sp, sn = sum(pos), sum(neg)
        if sp >= min_matches and sn >= min_matches:
            return None
        if sp >= min_matches:
            direction = "positive"
        elif sn >= min_matches:
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
            "pct_90d": f"{d90:.2f}" if d90 is not None else "",
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


def _date_from_ts(ts: str) -> str:
    """Extract YYYY-MM-DD from timestamp string."""
    return (ts or "")[:10]


def _dedupe_rows(rows: List[Dict]) -> List[Dict]:
    """Keep one row per (date, symbol), preferring backtest close (16:00) then latest."""
    seen: Dict[Tuple[str, str], Dict] = {}
    for r in rows:
        ts = r.get("timestamp", "")
        date_key = _date_from_ts(ts)
        sym = r.get("symbol", "")
        key = (date_key, sym)
        existing = seen.get(key)
        if not existing:
            seen[key] = r
        else:
            # Prefer 16:00:00 (backtest EOD); else keep later timestamp
            ex_ts = existing.get("timestamp", "")
            if "16:00:00" in ts and "16:00:00" not in ex_ts:
                seen[key] = r
            elif "16:00:00" not in ts and "16:00:00" in ex_ts:
                pass  # keep existing
            elif ts > ex_ts:
                seen[key] = r
    out = list(seen.values())
    out.sort(key=lambda r: (r.get("timestamp", ""), r.get("symbol", "")))
    return out


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
      Each data row is green when direction is positive, red when negative (1D/7D/30D criteria).
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
    universe = _load_universe(config)
    now = as_of or datetime.now(ZoneInfo("America/New_York"))
    mm = int(config.get("thresholds", {}).get("min_matches", 2))
    print(f"Scanning {len(universe)} symbols ({mm}-of-3 1D/7D/30D vs thresholds, $ vol > $250M)...")

    existing = _read_rows()
    new_rows = []
    for i, sym in enumerate(universe):
        rec = _evaluate_symbol(sym, config, as_of=now)
        if rec:
            new_rows.append(rec)
            print(f"  [MATCH] {sym}: {rec['direction']} — 1D {rec['pct_1d']}%, 7D {rec['pct_7d']}%, 30D {rec['pct_30d']}%, 90D {rec.get('pct_90d','')}%")
        if (i + 1) % 200 == 0:
            print(f"  Progress: {i + 1}/{len(universe)}", flush=True)
        time.sleep(0.08)

    all_rows = _dedupe_rows(existing + new_rows)
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

    rows = _dedupe_rows(rows)
    if not rows:
        print("No data in log. Run a scan or backtest first.")
        return
    # Clean log file on disk (dedupe by date, prune) to prevent future duplicate display
    full = _dedupe_rows(_prune_six_months(_read_rows()))
    _write_rows(full)
    rising_html = _build_report_html(rows, "Rising Stars", config)
    big_html = _build_report_html(rows, "Big Ones", config)
    # Save locally
    (LOG_DIR / "MarketScout_Rising_Stars.html").write_text(rising_html, encoding="utf-8")
    (LOG_DIR / "MarketScout_Big_Ones.html").write_text(big_html, encoding="utf-8")
    print(f"Reports saved to {LOG_DIR}/")
    _send_telegram_html(rising_html, "MarketScout_Rising_Stars.html", "Rising Stars ($250M–$1B)")
    _send_telegram_html(big_html, "MarketScout_Big_Ones.html", "Big Ones (>= $1B)")
    print("Reports sent to Telegram.")


def run_backtest(start_date: str = "2026-01-01", reset_log: bool = False, resume: bool = False) -> None:
    """Backtest weekdays from start through today. Writes log + checkpoint after each completed day.

    --reset-log: start with empty log and new checkpoint.
    --resume: continue from last_completed in backtest_checkpoint.json (log rows preserved).
    """
    config = load_config("config.yaml")
    universe = _load_universe(config)
    start_d = datetime.strptime(start_date, "%Y-%m-%d").date()
    today = datetime.now(ZoneInfo("America/New_York")).date()
    existing: List[Dict] = []
    range_start = start_d
    all_new: List[Dict] = []

    if reset_log:
        _clear_backtest_checkpoint()
        existing = []
        d = start_d
        print("Reset log: ignoring existing screener_log.csv rows for this backtest.", flush=True)
    elif resume:
        existing = _read_rows()
        cp = _read_backtest_checkpoint()
        if cp and cp.get("last_completed"):
            last_done = datetime.strptime(cp["last_completed"], "%Y-%m-%d").date()
            if cp.get("start_date"):
                range_start = datetime.strptime(cp["start_date"], "%Y-%m-%d").date()
            d = _next_weekday_after(last_done)
            print(
                f"Resume: last completed {last_done}; continuing from {d}. Log rows loaded: {len(existing)}.",
                flush=True,
            )
        else:
            d = start_d
            print("Resume: no checkpoint; starting from --start date.", flush=True)
    else:
        existing = _read_rows()
        d = start_d

    if d > today:
        print("Nothing to backtest (start is after today).")
        return

    total_weekdays = 0
    sd = range_start
    while sd <= today:
        if sd.weekday() < 5:
            total_weekdays += 1
        sd += timedelta(days=1)

    all_rows: List[Dict] = []
    while d <= today:
        if d.weekday() >= 5:
            d += timedelta(days=1)
            continue
        wd_i = _weekday_number_in_range(range_start, d)
        as_of = datetime(d.year, d.month, d.day, 16, 0, 0, tzinfo=ZoneInfo("America/New_York"))
        print(f"\nBacktest weekday {wd_i}/{total_weekdays}: {d}...", flush=True)
        day_rows: List[Dict] = []
        for sym in universe:
            rec = _evaluate_symbol(sym, config, as_of=as_of)
            if rec:
                day_rows.append(rec)
            time.sleep(0.05)
        all_new.extend(day_rows)

        all_rows = _dedupe_rows(existing + all_new)
        all_rows = _prune_six_months(all_rows)
        _write_rows(all_rows)
        _write_backtest_checkpoint(d, range_start.isoformat())
        print(
            f"  Checkpoint saved ({len(day_rows)} matches this day; {len(all_rows)} rows in log).",
            flush=True,
        )
        d += timedelta(days=1)

    _clear_backtest_checkpoint()
    print(f"\nBacktest done. New rows this run: {len(all_new)}. Total in log: {len(all_rows)}. Log: {LOG_PATH}")


if __name__ == "__main__":
    import argparse
    from dotenv import load_dotenv
    load_dotenv()

    parser = argparse.ArgumentParser(description="MarketScout screener")
    parser.add_argument("--mode", choices=["scan", "backtest", "report"], default="scan")
    parser.add_argument("--start", default="2026-01-01", help="Backtest start YYYY-MM-DD")
    parser.add_argument("--no-send", action="store_true", help="Do not send reports to Telegram after scan")
    parser.add_argument(
        "--reset-log",
        action="store_true",
        help="Backtest only: do not merge existing log; write fresh results from backtest",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Backtest only: continue from logs/backtest_checkpoint.json after a stop/crash",
    )
    args = parser.parse_args()

    if args.mode == "scan":
        run_scan(send_reports=not args.no_send)
    elif args.mode == "report":
        run_report_only(start_date=args.start)
    else:
        run_backtest(args.start, reset_log=args.reset_log, resume=args.resume)
