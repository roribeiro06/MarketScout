import yfinance as yf


def main() -> None:
    t = yf.Ticker("MRNA")
    hist = t.history(period="5d")
    if len(hist) < 2:
        raise SystemExit("Not enough history for MRNA")
    prev = hist.iloc[-2]
    today = hist.iloc[-1]
    prev_close = float(prev["Close"])
    open_today = float(today["Open"])
    vol_today = float(today["Volume"])
    pct = (open_today - prev_close) / prev_close * 100.0 if prev_close > 0 else None
    dollar_vol = prev_close * vol_today if prev_close > 0 else None

    print("Yesterday close:", prev_close)
    print("Today open:", open_today)
    print("Today volume:", vol_today)
    if pct is not None:
        print("Pct change prev close -> open:", f"{pct:+.2f}%")
    if dollar_vol is not None:
        print("Dollar volume:", dollar_vol)
        qualifies = abs(pct) >= 5.0 and dollar_vol >= 250_000_000
        print("Qualifies (|pct|>=5% and dollar_vol>=250M):", qualifies)


if __name__ == "__main__":
    main()

