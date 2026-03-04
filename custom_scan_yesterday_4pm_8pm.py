"""
One-off report: yesterday's 4pm close → yesterday's post-market close (8pm),
±3% move, ≥$250M dollar volume. Uses hourly data with prepost to get extended session.
"""
import os

import requests
import yfinance as yf
from dotenv import load_dotenv

from screener.screener import get_exchange_symbols, load_config


def get_yesterday_4pm_8pm(ticker: yf.Ticker):
    """Return (date, 4pm_close, 8pm_close, day_volume) or None. 4pm = last regular-session close; 8pm = last bar of day."""
    hist = ticker.history(period="5d", interval="1h", prepost=True, auto_adjust=False)
    if hist is None or len(hist) < 2:
        return None
    if hist.index.tz is None:
        return None
    dates = sorted(hist.index.normalize().unique())
    if len(dates) < 2:
        return None
    # "yesterday" = second-to-last date (last full day before today)
    yesterday = dates[-2]
    day = hist[hist.index.normalize() == yesterday]
    if day.empty:
        return None
    # 4pm close = close of last bar with volume (regular session)
    reg = day[day["Volume"] > 0]
    if reg.empty:
        return None
    close_4pm = float(reg["Close"].iloc[-1])
    vol_day = float(reg["Volume"].sum())  # regular-session volume for dollar vol
    # post-market close = last bar of the day (extended session)
    close_8pm = float(day["Close"].iloc[-1])
    return (yesterday, close_4pm, close_8pm, vol_day)


def main() -> None:
    load_dotenv()
    config = load_config()
    exchanges = config.get("exchanges", ["NYSE", "NASDAQ"])
    symbols = []
    for ex in exchanges:
        symbols.extend(get_exchange_symbols(ex))
    symbols = sorted(set(symbols))
    print(f"Scanning {len(symbols)} symbols for yesterday 4pm->8pm (+/-3%, >=$250M)...", flush=True)
    results = []

    for sym in symbols:
        try:
            t = yf.Ticker(sym)
            out = get_yesterday_4pm_8pm(t)
            if out is None:
                continue
            yesterday_d, close_4pm, close_8pm, vol_day = out
            if close_4pm <= 0:
                continue
            pct = (close_8pm - close_4pm) / close_4pm * 100.0
            dollar_vol = close_4pm * vol_day
            if abs(pct) >= 3.0 and dollar_vol >= 250_000_000:
                info = t.info or {}
                name = info.get("shortName") or info.get("longName") or sym
                results.append(
                    {
                        "symbol": sym,
                        "name": name,
                        "date": yesterday_d,
                        "close_4pm": close_4pm,
                        "close_8pm": close_8pm,
                        "pct": pct,
                        "vol": vol_day,
                        "dollar_vol": dollar_vol,
                    }
                )
        except Exception as e:  # noqa: BLE001
            print(f"Error for {sym}: {e}")
            continue

    results.sort(key=lambda r: -abs(r["pct"]))

    date_str = results[0]["date"].strftime("%Y-%m-%d") if results else "N/A"
    lines = [
        f"📊 Yesterday 4pm → 8pm post-market (±3%, ≥$250M) — {date_str}\n",
        f"Matches: {len(results)}\n",
    ]
    for r in results:
        lines.append(f"{r['symbol']} {r['name']}")
        lines.append(f"  4pm: ${r['close_4pm']:.2f} → 8pm: ${r['close_8pm']:.2f} ({r['pct']:+.2f}%)")
        lines.append(f"  Vol: {r['vol']:.0f} (dollar vol ${r['dollar_vol'] / 1_000_000:.1f}M)\n")

    msg = "\n".join(lines)

    token = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    chat_id = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()
    if not token or not chat_id:
        raise SystemExit("Missing TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID")

    resp = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id": chat_id, "text": msg},
        timeout=60,
    )
    print("Telegram status:", resp.status_code, resp.text[:200])


if __name__ == "__main__":
    main()
