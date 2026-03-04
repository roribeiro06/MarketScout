import os

import requests
import yfinance as yf
from dotenv import load_dotenv

from screener.screener import _fallback_exchange_symbols


def main() -> None:
    load_dotenv()

    symbols = sorted(set(_fallback_exchange_symbols("NYSE") + _fallback_exchange_symbols("NASDAQ")))
    results = []

    for sym in symbols:
        try:
            t = yf.Ticker(sym)
            hist = t.history(period="5d")
            if len(hist) < 2:
                continue
            prev = hist.iloc[-2]
            today = hist.iloc[-1]
            prev_close = float(prev["Close"])
            open_today = float(today["Open"])
            vol_today = float(today["Volume"])
            if prev_close <= 0 or vol_today <= 0:
                continue
            pct = (open_today - prev_close) / prev_close * 100.0
            dollar_vol = prev_close * vol_today
            if abs(pct) >= 5.0 and dollar_vol >= 250_000_000:
                info = t.info or {}
                name = info.get("shortName") or info.get("longName") or sym
                results.append(
                    {
                        "symbol": sym,
                        "name": name,
                        "prev_close": prev_close,
                        "open": open_today,
                        "pct": pct,
                        "vol": vol_today,
                        "dollar_vol": dollar_vol,
                    }
                )
        except Exception as e:  # noqa: BLE001
            print(f"Error for {sym}: {e}")
            continue

    results.sort(key=lambda r: -abs(r["pct"]))

    lines: list[str] = []
    lines.append("📊 Custom scan — prev close → today's open (±5%, ≥$250M dollar volume)\n")
    lines.append(f"Matches: {len(results)}\n")
    for r in results:
        lines.append(f"{r['symbol']} {r['name']}")
        lines.append(f"  {r['prev_close']:.2f} → {r['open']:.2f} ({r['pct']:+.2f}%)")
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

