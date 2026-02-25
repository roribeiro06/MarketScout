"""Main entry point for MarketScout stock screener."""
import html
import os
import re
import time
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import yfinance as yf
from screener.screener import load_config, run_screener, run_screener_and_rising_stars, run_crypto_screener, run_forex_screener, run_commodity_screener, run_etf_screener
from screener.charts import generate_charts_for_results

# Indices to show at top (symbol, display name). VIX gets 1D only; others get 1D/1W/1M.
INDICES = [
    ("^GSPC", "S&P 500"),
    ("^IXIC", "Nasdaq"),
    ("^DJI", "Dow 30"),
    ("^RUT", "Russell 2000"),
    ("^VIX", "VIX"),
]

# Contract size (units per contract) for commodity futures dollar volume
COMMODITY_CONTRACT_SIZE = {
    "CL=F": 1000,   # Crude Oil: 1000 barrels
    "BZ=F": 1000,   # Brent: 1000 barrels
    "NG=F": 10000,  # Natural Gas: 10,000 mmBtu
    "RB=F": 1000,   # RBOB Gasoline: 1000 barrels
    "HO=F": 1000,   # Heating Oil: 1000 barrels
    "GC=F": 100,    # Gold: 100 oz
    "SI=F": 5000,   # Silver: 5000 oz
    "HG=F": 25000,  # Copper: 25,000 lb
    "PL=F": 100,    # Platinum: 100 oz
    "PA=F": 100,    # Palladium: 100 oz
    "ZC=F": 5000,   # Corn: 5000 bu
    "ZS=F": 5000,   # Soybeans: 5000 bu
    "ZW=F": 5000,   # Wheat: 5000 bu
    "KE=F": 5000,   # KC Wheat: 5000 bu
    "CT=F": 50000,  # Cotton: 50,000 lb
    "CC=F": 10,     # Cocoa: 10 metric tons
    "KC=F": 37500,  # Coffee: 37,500 lb
    "SB=F": 112000, # Sugar: 112,000 lb
    "OJ=F": 15000,  # Orange Juice: 15,000 lb
    "ZO=F": 5000,   # Oats: 5000 bu
    "LE=F": 40000,  # Live Cattle: 40,000 lb
    "HE=F": 40000,  # Lean Hogs: 40,000 lb
    "LBS=F": 110000,# Lumber: 110,000 board feet
}


def get_indices_snapshot() -> List[Dict]:
    """Fetch current level and 1D/1W/1M/6M/1Y/3Y % changes for indices. VIX has 1D only."""
    out = []
    for symbol, name in INDICES:
        try:
            ticker = yf.Ticker(symbol)
            data = ticker.history(period="5y")
            if data is None or len(data) < 2:
                continue
            price = round(float(data["Close"].iloc[-1]), 2)
            one_d = None
            one_w = None
            one_m = None
            one_6m = None
            one_1y = None
            one_3y = None
            if len(data) >= 2:
                p0 = data["Close"].iloc[-2]
                one_d = ((data["Close"].iloc[-1] - p0) / p0) * 100
            if len(data) >= 6:
                p5 = data["Close"].iloc[-6]
                one_w = ((data["Close"].iloc[-1] - p5) / p5) * 100
            if len(data) >= 21:
                p20 = data["Close"].iloc[-21]
                one_m = ((data["Close"].iloc[-1] - p20) / p20) * 100
            if len(data) >= 2:
                n_6m = min(126, len(data) - 1)
                p_6m = data["Close"].iloc[-(n_6m + 1)]
                one_6m = ((data["Close"].iloc[-1] - p_6m) / p_6m) * 100
            if len(data) >= 253:
                n_1y = min(252, len(data) - 1)
                p_1y = data["Close"].iloc[-(n_1y + 1)]
                one_1y = ((data["Close"].iloc[-1] - p_1y) / p_1y) * 100
            if len(data) >= 757:
                n_3y = min(756, len(data) - 1)
                p_3y = data["Close"].iloc[-(n_3y + 1)]
                one_3y = ((data["Close"].iloc[-1] - p_3y) / p_3y) * 100
            out.append({
                "symbol": symbol,
                "name": name,
                "price": price,
                "one_day_pct": round(one_d, 2) if one_d is not None else None,
                "one_week_pct": round(one_w, 2) if one_w is not None else None,
                "one_month_pct": round(one_m, 2) if one_m is not None else None,
                "one_6m_pct": round(one_6m, 2) if one_6m is not None else None,
                "one_year_pct": round(one_1y, 2) if one_1y is not None else None,
                "three_year_pct": round(one_3y, 2) if one_3y is not None else None,
                "is_vix": symbol == "^VIX",
            })
        except Exception as e:
            print(f"  [INDICES] Error fetching {symbol}: {e}")
    return out


TELEGRAM_MAX_MESSAGE_LENGTH = 4096


def _telegram_400_hint(response_text: str) -> str:
    """Return a short hint from Telegram 400 response for logging."""
    try:
        import json
        data = json.loads(response_text or "{}")
        desc = (data.get("description") or "").lower()
        if "chat" in desc and ("not found" in desc or "invalid" in desc):
            return "Likely invalid chat_id: check TELEGRAM_CHAT_ID (e.g. group ID should start with -100)."
        if "unauthorized" in desc or "token" in desc:
            return "Likely invalid or expired bot token: check TELEGRAM_BOT_TOKEN."
        if "parse" in desc or "entities" in desc:
            return "Invalid HTML in message: try plain-text fallback or fix special characters."
        if "too long" in desc or "message" in desc:
            return "Message too long: chunk size may still exceed Telegram limit."
        return f"Telegram says: {data.get('description', response_text[:200])}"
    except Exception:
        return response_text[:300] if response_text else "Unknown error"


def _strip_html_to_plain(s: str) -> str:
    """Convert our simple HTML to plain text for Telegram fallback (e.g. when <span> is rejected)."""
    s = re.sub(r"<span[^>]*>", "", s)
    s = s.replace("</span>", "")
    s = s.replace("</b>", "").replace("<b>", "")
    s = s.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    return s


def _telegram_send_message(url: str, payload: dict) -> requests.Response:
    """POST to Telegram sendMessage; on error print API response and raise."""
    try:
        response = requests.post(url, json=payload, timeout=10)
        if not response.ok:
            print(f"Telegram API error: {response.status_code} - {response.text}", flush=True)
            if response.status_code == 400:
                print(f"Root cause hint: {_telegram_400_hint(response.text)}", flush=True)
        response.raise_for_status()
        return response
    except requests.exceptions.HTTPError as e:
        print(f"Telegram API error: {e.response.status_code} - {e.response.text}", flush=True)
        raise


def send_telegram_message(text: str, token: str, chat_id: str) -> None:
    """Send a plain-text message to the configured Telegram chat. Splits at newlines if over limit."""
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    # Ensure chat_id is an integer (Telegram API accepts int or str; int can avoid malformed-id issues)
    try:
        chat_id_param = int(chat_id)
    except (ValueError, TypeError):
        chat_id_param = chat_id
    # Telegram hard limit 4096; we chunk so each piece stays under it
    max_len = TELEGRAM_MAX_MESSAGE_LENGTH - 100  # 3996 to be safe
    chunk_count = 0
    while text:
        if len(text) <= max_len:
            chunk = text
            text = ""
        else:
            chunk = text[:max_len]
            last_newline = chunk.rfind("\n")
            if last_newline > max_len // 2:
                chunk = chunk[: last_newline + 1]
                text = text[last_newline + 1 :].lstrip("\n")
            else:
                last_amp = chunk.rfind("&")
                if last_amp > max_len - 20 and last_amp > 0:
                    chunk = chunk[:last_amp]
                    text = text[last_amp:]
                else:
                    text = text[max_len:]
        if len(chunk) > 4096:
            chunk = chunk[:4096]
        chunk_count += 1
        if chunk_count > 1:
            print(f"Sending message part {chunk_count} ({len(chunk)} chars).", flush=True)
        payload = {"chat_id": chat_id_param, "text": chunk, "parse_mode": "HTML"}
        try:
            _telegram_send_message(url, payload)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 400:
                plain_chunk = _strip_html_to_plain(chunk)
                print("Retrying as plain text (HTML rejected by Telegram).", flush=True)
                _telegram_send_message(url, {"chat_id": chat_id_param, "text": plain_chunk})
            else:
                raise


def send_telegram_media_group(photo_paths: list, token: str, chat_id: str) -> None:
    """Send multiple photos as a media group (single notification)."""
    if not photo_paths:
        return
    try:
        chat_id_param = int(chat_id)
    except (ValueError, TypeError):
        chat_id_param = chat_id
    import json
    for batch_start in range(0, len(photo_paths), 10):
        batch_paths = photo_paths[batch_start:batch_start + 10]
        url = f"https://api.telegram.org/bot{token}/sendMediaGroup"
        media = []
        files = {}
        for i, photo_path in enumerate(batch_paths):
            media.append({
                'type': 'photo',
                'media': f'attach://photo_{i}'
            })
            files[f'photo_{i}'] = open(photo_path, 'rb')
        data = {
            'chat_id': chat_id_param,
            'media': json.dumps(media)
        }
        
        try:
            response = requests.post(url, files=files, data=data, timeout=60)
            if not response.ok:
                print(f"Telegram API error (sendMediaGroup {response.status_code}): {response.text}", flush=True)
            response.raise_for_status()
        finally:
            # Close all file handles
            for f in files.values():
                f.close()


def _pct_sort_key(item: dict) -> tuple:
    """Sort key: largest move in any period that fit the criteria first (by magnitude)."""
    d = item.get("one_day_pct")
    w = item.get("one_week_pct")
    m = item.get("one_month_pct")
    vals = []
    if item.get("passes_day") and d is not None:
        vals.append(abs(d))
    if item.get("passes_week") and w is not None:
        vals.append(abs(w))
    if item.get("passes_month") and m is not None:
        vals.append(abs(m))
    max_abs = max(vals) if vals else 0.0
    return (-max_abs,)  # descending: largest qualifying move first


def _pct_str_no_pct(label: str, pct: Optional[float], passes: bool) -> str:
    """Format percentage without '%' for Telegram. When passes: 🟢 if positive, 🔴 if negative."""
    if pct is None:
        return f"{label}: —"
    s = f"{label}: {pct:+.2f}"
    if passes:
        return ("🟢 " if pct >= 0 else "🔴 ") + s
    return s


def _all_three_pass(item: dict) -> bool:
    """True if asset passes 1D, 1W, and 1M criteria."""
    return bool(
        item.get("passes_day")
        and item.get("passes_week")
        and item.get("passes_month")
    )


def _all_three_positive(item: dict) -> bool:
    """True if asset passes 1D, 1W, and 1M criteria and all moved up (positive).
    Uses >= -0.01 for rounding; if a pct is None we still include (e.g. edge cases)."""
    if not _all_three_pass(item):
        return False
    d = item.get("one_day_pct")
    w = item.get("one_week_pct")
    m = item.get("one_month_pct")
    # When present, must be >= -0.01; when None (edge case), don't disqualify
    if d is not None and d < -0.01:
        return False
    if w is not None and w < -0.01:
        return False
    if m is not None and m < -0.01:
        return False
    return True


def _format_big_num(x: Optional[float]) -> str:
    """Format large numbers as $X.XB or $X.XM or $X.XK."""
    if x is None or (isinstance(x, float) and (x != x or x == 0)):
        return "—"
    try:
        x = float(x)
    except (TypeError, ValueError):
        return "—"
    if abs(x) >= 1e12:
        return f"${x/1e12:.2f}T"
    if abs(x) >= 1e9:
        return f"${x/1e9:.2f}B"
    if abs(x) >= 1e6:
        return f"${x/1e6:.2f}M"
    if abs(x) >= 1e3:
        return f"${x/1e3:.2f}K"
    return f"${x:.2f}"


def _fetch_stock_financial_stats(symbol: str, delay_seconds: float = 2.0) -> Optional[Dict]:
    """Fetch financial stats for a stock from yfinance. Returns dict or None on total failure.
    Fills stats step-by-step so one failing step doesn't leave us with empty dict."""
    time.sleep(delay_seconds)

    def _get(info_dict: dict, k: str, default=None):
        if not isinstance(info_dict, dict):
            return default
        v = info_dict.get(k, default)
        if v is None:
            return default
        try:
            return float(v) if isinstance(v, (int, float)) else default
        except (TypeError, ValueError):
            return default

    empty_stats = {
        "market_cap": None, "profit_margin": None, "total_revenue": None,
        "revenue_per_share": None, "gross_profit": None, "total_cash": None,
        "cash_per_share": None, "total_debt": None, "operating_cashflow": None,
        "forward_dividend": None, "dividend_yield": None,
        "current_assets": None, "current_liabilities": None, "operating_income": None,
    }

    for attempt in range(3):
        stats = dict(empty_stats)
        try:
            t = yf.Ticker(symbol)
            info = None
            try:
                info = t.info
            except Exception:
                pass
            if isinstance(info, dict) and info:
                shares = _get(info, "sharesOutstanding") or _get(info, "impliedSharesOutstanding")
                stats["market_cap"] = _get(info, "marketCap")
                stats["profit_margin"] = _get(info, "profitMargins")
                stats["total_revenue"] = _get(info, "totalRevenue")
                stats["gross_profit"] = _get(info, "grossProfits")
                stats["total_cash"] = _get(info, "totalCash") or _get(info, "cash")
                stats["total_debt"] = _get(info, "totalDebt")
                stats["operating_cashflow"] = _get(info, "operatingCashflow")
                stats["forward_dividend"] = _get(info, "forwardDividendRate") or _get(info, "dividendRate")
                stats["dividend_yield"] = _get(info, "dividendYield")
                stats["revenue_per_share"] = _get(info, "revenuePerShare")
                if stats["revenue_per_share"] is None and stats["total_revenue"] and shares:
                    stats["revenue_per_share"] = stats["total_revenue"] / shares
                stats["cash_per_share"] = _get(info, "totalCashPerShare")
                if stats["cash_per_share"] is None and stats["total_cash"] and shares:
                    stats["cash_per_share"] = stats["total_cash"] / shares
            if stats["market_cap"] is None:
                try:
                    fast = getattr(t, "fast_info", None)
                    if fast is not None:
                        mc = getattr(fast, "market_cap", None)
                        if mc is not None:
                            stats["market_cap"] = float(mc)
                except Exception:
                    pass
            try:
                bs = getattr(t, "balance_sheet", None)
                if bs is not None and not getattr(bs, "empty", True) and hasattr(bs, "index"):
                    for label in ["Total Current Assets", "Current Assets", "Total Current Liabilities", "Current Liabilities"]:
                        if label in bs.index:
                            row = bs.loc[label]
                            val = row.iloc[0] if hasattr(row, "iloc") and len(row) else (row[0] if len(row) else None)
                            if val is not None and not (isinstance(val, float) and (val != val)):
                                try:
                                    fval = float(val)
                                    if "Asset" in label:
                                        stats["current_assets"] = stats["current_assets"] or fval
                                    else:
                                        stats["current_liabilities"] = stats["current_liabilities"] or fval
                                except (TypeError, ValueError):
                                    pass
            except Exception:
                pass
            try:
                inc = getattr(t, "income_stmt", None)
                if inc is not None and not getattr(inc, "empty", True) and hasattr(inc, "index"):
                    for label in ["Operating Income", "Operating Income Loss", "Total Operating Income As Reported"]:
                        if label in inc.index:
                            row = inc.loc[label]
                            val = row.iloc[0] if hasattr(row, "iloc") and len(row) else (row[0] if len(row) else None)
                            if val is not None and not (isinstance(val, float) and (val != val)):
                                try:
                                    stats["operating_income"] = float(val)
                                    break
                                except (TypeError, ValueError):
                                    pass
            except Exception:
                pass
            if any(v is not None for v in stats.values()):
                return stats
        except Exception as e:
            print(f"  [DEEP] Attempt {attempt + 1} for {symbol}: {e}")
            if attempt < 2:
                time.sleep(3.0 + attempt)

    return None


def _append_section_block(
    message: str,
    by_sector: dict,
    sector: str,
    emoji: str,
    is_forex: bool = False,
    is_commodity: bool = False,
) -> str:
    """Append one section (Crypto, Commodities, or Forex) to message. Returns updated message."""
    stocks = by_sector.get(sector, [])
    if not stocks:
        return message
    stocks = sorted(stocks, key=_pct_sort_key)
    message += f"<b>{emoji} {sector}</b>\n"
    for stock in stocks:
        symbol = stock["symbol"]
        company_name = stock.get("company_name", symbol)
        price = stock["price"]
        vol = stock.get("volume") or 0
        vol_shares = vol / 1_000_000
        dollar_vol = (vol * price) / 1_000_000
        lead = "🟡 " if _all_three_pass(stock) else ""

        d_str = _pct_str_no_pct("1D", stock["one_day_pct"], stock["passes_day"])
        w_str = _pct_str_no_pct("1W", stock["one_week_pct"], stock["passes_week"])
        m_val = stock.get("one_month_pct")
        m_pass = stock.get("passes_month", False)
        m_str = _pct_str_no_pct("1M", m_val, m_pass)
        six_val = stock.get("one_6m_pct")
        one_yr_val = stock.get("one_year_pct")
        three_yr_val = stock.get("three_year_pct")
        six_str = f"6M: {six_val:+.2f}" if six_val is not None else "6M: —"
        one_yr_str = f"1Y: {one_yr_val:+.2f}" if one_yr_val is not None else "1Y: —"
        three_yr_str = f"3Y: {three_yr_val:+.2f}" if three_yr_val is not None else "3Y: —"
        change_str = f"{d_str} | {w_str} | {m_str} | {six_str} | {one_yr_str} | {three_yr_str}"
        sector_label = stock.get("sector", sector)
        if is_forex:
            message += f"{lead}<b>{html.escape(company_name)} ({symbol})</b> {price:.4f}\n"
        else:
            message += f"{lead}<b>{html.escape(company_name)} ({symbol})</b> ${price:.2f}\n"
        message += f"  <i>{sector_label}</i>\n"
        message += f"  {change_str}\n"
        if is_forex and vol > 0:
            message += f"  Vol: {vol_shares:.2f}M\n\n"
        elif is_commodity and vol > 0:
            vol_k = vol / 1_000
            contract_size = COMMODITY_CONTRACT_SIZE.get(symbol, 1)
            commodity_dollar_vol = (vol * price * contract_size) / 1_000_000
            message += f"  Vol: {vol_k:.1f}K contracts (${commodity_dollar_vol:.1f}M)\n\n"
        elif not is_forex and not is_commodity and vol > 0:
            message += f"  Vol: {vol_shares:.2f}M (${dollar_vol:.1f}M)\n\n"
        else:
            message += "\n"
    message += "\n"
    return message


def _format_one_stock_block(stocks: list) -> str:
    """Format a list of stocks (big or rising stars) into a message block."""
    if not stocks:
        return ""
    lines = []
    for stock in sorted(stocks, key=_pct_sort_key):
        symbol = stock["symbol"]
        company_name = stock.get("company_name", symbol)
        sector_name = stock.get("display_sector") or stock.get("sector", "Other")
        price = stock["price"]
        vol = stock.get("volume") or 0
        vol_shares = vol / 1_000_000
        dollar_vol = (vol * price) / 1_000_000
        lead = "🟡 " if _all_three_pass(stock) else ""
        d_str = _pct_str_no_pct("1D", stock["one_day_pct"], stock["passes_day"])
        w_str = _pct_str_no_pct("1W", stock["one_week_pct"], stock["passes_week"])
        m_val = stock.get("one_month_pct")
        m_pass = stock.get("passes_month", False)
        m_str = _pct_str_no_pct("1M", m_val, m_pass)
        six_val = stock.get("one_6m_pct")
        one_yr_val = stock.get("one_year_pct")
        three_yr_val = stock.get("three_year_pct")
        six_str = f"6M: {six_val:+.2f}" if six_val is not None else "6M: —"
        one_yr_str = f"1Y: {one_yr_val:+.2f}" if one_yr_val is not None else "1Y: —"
        three_yr_str = f"3Y: {three_yr_val:+.2f}" if three_yr_val is not None else "3Y: —"
        change_str = f"{d_str} | {w_str} | {m_str} | {six_str} | {one_yr_str} | {three_yr_str}"
        tp = stock.get("target_price")
        price_str = f"${price:.2f}" + (f" (1Y: ${tp:.2f})" if tp is not None else "")
        lines.append(f"{lead}<b>{html.escape(company_name)} ({symbol})</b> {price_str}")
        lines.append(f"  <i>{sector_name}</i>")
        lines.append(f"  {change_str}")
        if vol > 0:
            lines.append(f"  Vol: {vol_shares:.2f}M (${dollar_vol:.1f}M)")
        lines.append("")
    return "\n".join(lines).strip()


def format_deep_dive_message(
    all_results: list,
    collection_time: Optional[str] = None,
) -> str:
    """
    Format a separate message for stocks that pass all 3 criteria positively (1D, 1W, 1M all up).
    Includes market cap, profit margin, revenue, gross profit, cash, debt, operating CF, forward div,
    plus balance sheet and income statement notes.
    """
    non_stock = {"Crypto", "Forex", "Commodities", "ETFs"}
    qualifying = [
        r for r in all_results
        if r.get("sector") not in non_stock
        and _all_three_positive(r)
    ]
    if not qualifying:
        return ""

    SECTION_END = "\n\n🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵"
    time_header = ("🕐 Yahoo data as of " + collection_time + "\n\n" if collection_time else "")
    msg = time_header + "📊 <b>MarketScout — Deep Dive (all 3 criteria positive, 1D/1W/1M up)</b>\n\n"
    msg += "Stocks below passed 1D, 1W, and 1M thresholds with all positive moves:\n\n"

    # Brief pause before fetching financials (helps avoid Yahoo rate limit after stock scan)
    time.sleep(3.0)
    for stock in sorted(qualifying, key=_pct_sort_key):
        symbol = stock["symbol"]
        company_name = stock.get("company_name", symbol)
        sector_name = stock.get("display_sector") or stock.get("sector", "Other")
        price = stock["price"]
        vol = stock.get("volume") or 0
        vol_shares = vol / 1_000_000
        dollar_vol = (vol * price) / 1_000_000
        tp = stock.get("target_price")
        price_str = f"${price:.2f}" + (f" (1Y: ${tp:.2f})" if tp is not None else "")

        # Same header block as regular stock report: name, ticker, price, target, sector, changes, vol
        msg += f"🟡 <b>{html.escape(company_name)} ({symbol})</b> {price_str}\n"
        msg += f"  <i>{sector_name}</i>\n"
        d_str = _pct_str_no_pct("1D", stock["one_day_pct"], stock["passes_day"])
        w_str = _pct_str_no_pct("1W", stock["one_week_pct"], stock["passes_week"])
        m_str = _pct_str_no_pct("1M", stock.get("one_month_pct"), stock.get("passes_month"))
        six_val = stock.get("one_6m_pct")
        one_yr_val = stock.get("one_year_pct")
        three_yr_val = stock.get("three_year_pct")
        six_str = f"6M: {six_val:+.2f}" if six_val is not None else "6M: —"
        one_yr_str = f"1Y: {one_yr_val:+.2f}" if one_yr_val is not None else "1Y: —"
        three_yr_str = f"3Y: {three_yr_val:+.2f}" if three_yr_val is not None else "3Y: —"
        msg += f"  {d_str} | {w_str} | {m_str} | {six_str} | {one_yr_str} | {three_yr_str}\n"
        if vol > 0:
            msg += f"  Vol: {vol_shares:.2f}M (${dollar_vol:.1f}M)\n"
        msg += "\n"

        stats = _fetch_stock_financial_stats(symbol, delay_seconds=2.5)
        if not stats:
            stats = _fetch_stock_financial_stats(symbol, delay_seconds=5.0)
        if not stats:
            msg += "  (Financial data unavailable)\n\n"
            continue

        # Core stats
        msg += f"  Market cap: {_format_big_num(stats['market_cap'])}\n"
        pm = stats.get("profit_margin")
        msg += f"  Profit margin: {pm*100:.1f}%\n" if pm is not None else "  Profit margin: —\n"
        rev = stats.get("total_revenue")
        rps = stats.get("revenue_per_share")
        msg += f"  Revenue: {_format_big_num(rev)}" + (f" (${rps:.2f}/share)" if rps is not None else "") + "\n"
        msg += f"  Gross profit: {_format_big_num(stats.get('gross_profit'))}\n"
        tc = stats.get("total_cash")
        cps = stats.get("cash_per_share")
        msg += f"  Total cash: {_format_big_num(tc)}" + (f" (${cps:.2f}/share)" if cps is not None else "") + "\n"
        msg += f"  Total debt: {_format_big_num(stats.get('total_debt'))}\n"
        msg += f"  Operating cash flow: {_format_big_num(stats.get('operating_cashflow'))}\n"
        fd = stats.get("forward_dividend")
        dy = stats.get("dividend_yield")
        msg += f"  Forward dividend: " + (f"${fd:.2f}" if fd is not None else "—")
        if dy is not None and isinstance(dy, (int, float)):
            # Yahoo may return decimal (0.0096) or percentage (0.96); show as %
            pct = (float(dy) * 100) if float(dy) < 0.1 else float(dy)
            msg += f" ({pct:.2f}% yield)"
        msg += "\n\n"

        # Notes
        ca, cl = stats.get("current_assets"), stats.get("current_liabilities")
        if ca is not None and cl is not None and cl != 0:
            ratio = ca / cl
            msg += f"  Balance sheet: Current assets/liabilities = {ratio:.2f} (ideally above 1)\n"
        else:
            msg += "  Balance sheet: Current assets/liabilities = — (ideally above 1)\n"

        oi, tr = stats.get("operating_income"), stats.get("total_revenue")
        if oi is not None and tr is not None and tr != 0:
            pct = (oi / tr) * 100
            msg += f"  Income statement: Operating income/Revenue = {pct:.1f}% (ideally above 15%)\n"
        else:
            msg += "  Income statement: Operating income/Revenue = — (ideally above 15%)\n"

        msg += "\n"
    return (msg.strip() + SECTION_END).strip()


def _criteria_count(r: dict) -> int:
    """Count how many of 1D, 1W, 1M criteria the asset passes."""
    return sum(1 for k in ("passes_day", "passes_week", "passes_month") if r.get(k))


def _get_next_earnings_date(symbol: str) -> Optional[str]:
    """Fetch next earnings date for a symbol. Returns YYYY-MM-DD string or None."""
    try:
        t = yf.Ticker(symbol)
        ed = t.earnings_dates
        if ed is not None and not ed.empty:
            first_date = ed.index[0]
            if hasattr(first_date, "strftime"):
                return first_date.strftime("%Y-%m-%d")
            return str(first_date)
        cal = getattr(t, "calendar", None)
        if isinstance(cal, dict):
            for k in ("earningsDate", "earnings", "nextEarnings"):
                v = cal.get(k)
                if v is not None and hasattr(v, "strftime"):
                    return v.strftime("%Y-%m-%d")
    except Exception:
        pass
    return None


def format_earnings_message(
    all_results: list,
    collection_time: Optional[str] = None,
) -> str:
    """
    Format a 6th message for stocks that pass 2 of 3 criteria (1D, 1W, 1M), regardless of direction.
    Shows next earnings date for each.
    """
    non_stock = {"Crypto", "Forex", "Commodities", "ETFs"}
    qualifying = [
        r for r in all_results
        if r.get("sector") not in non_stock
        and _criteria_count(r) >= 2
    ]
    if not qualifying:
        return ""

    SECTION_END = "\n\n🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵"
    time_header = ("🕐 Yahoo data as of " + collection_time + "\n\n" if collection_time else "")
    msg = time_header + "📅 <b>MarketScout — Earnings Dates (2 of 3 criteria met)</b>\n\n"
    msg += "Stocks with upcoming earnings that passed at least 2 of 1D/1W/1M (ordered by nearest date):\n\n"

    time.sleep(1.5)
    today = datetime.now(ZoneInfo("America/New_York")).date()

    def _earnings_sort_key(item):
        stock, earnings_str = item
        if not earnings_str:
            return (1, 999999)  # no date last
        try:
            parts = earnings_str.split("-")
            if len(parts) == 3:
                ed = today.__class__(int(parts[0]), int(parts[1]), int(parts[2]))
                days = (ed - today).days
                return (0, days if days >= 0 else 999998)  # past dates before no-date
        except (ValueError, IndexError):
            pass
        return (1, 999999)

    # Fetch earnings date once per stock; keep only upcoming (future) earnings
    stock_dates = []
    for stock in qualifying:
        symbol = stock["symbol"]
        earnings_date = _get_next_earnings_date(symbol)
        if not earnings_date:
            continue
        try:
            parts = earnings_date.split("-")
            if len(parts) == 3:
                ed = today.__class__(int(parts[0]), int(parts[1]), int(parts[2]))
                if (ed - today).days >= 0:
                    stock_dates.append((stock, earnings_date))
        except (ValueError, IndexError):
            continue
        time.sleep(1.0)
    if not stock_dates:
        return ""
    stock_dates.sort(key=_earnings_sort_key)

    for stock, earnings_date in stock_dates:
        symbol = stock["symbol"]
        company_name = stock.get("company_name", symbol)
        sector_name = stock.get("display_sector") or stock.get("sector", "Other")
        d_str = _pct_str_no_pct("1D", stock["one_day_pct"], stock["passes_day"])
        w_str = _pct_str_no_pct("1W", stock["one_week_pct"], stock["passes_week"])
        m_str = _pct_str_no_pct("1M", stock.get("one_month_pct"), stock.get("passes_month"))

        msg += f"<b>{html.escape(company_name)} ({symbol})</b>\n"
        msg += f"  <i>{sector_name}</i>\n"
        msg += f"  {d_str} | {w_str} | {m_str}\n"
        msg += f"  📅 Next earnings: {earnings_date or '—'}\n\n"

    return (msg.strip() + SECTION_END).strip()


def format_stock_message(
    results: list,
    crypto_count: int = 0,
    forex_count: int = 0,
    commodity_count: int = 0,
    etf_count: int = 0,
    indices_data: Optional[List[Dict]] = None,
    etf_asset_class_order: Optional[List[str]] = None,
    collection_time: Optional[str] = None,
) -> Tuple[str, str, str, str]:
    """Format into 4 Telegram messages: (1) Indices, (2) Big stocks, (3) Rising stars, (4) Crypto+Commodities+Forex+ETFs."""
    # Visual section end: white circles so each section is easy to spot
    SECTION_END = "\n\n🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵"
    time_header = ("🕐 Yahoo data as of " + collection_time + "\n\n" if collection_time else "")
    non_stock = {"Crypto", "Forex", "Commodities", "ETFs"}
    by_sector = {}
    for stock in results:
        sector = stock.get("sector", "Other")
        by_sector.setdefault(sector, []).append(stock)
    stock_sectors_regular = sorted(
        [s for s in by_sector if s not in non_stock and s != "Rising Stars"],
        key=lambda s: (s == "Other", s.upper()),
    )
    SECTION_EMOJI = {"Crypto": "🪙", "Commodities": "🌾", "Forex": "💵"}

    # ---------- Message 1: Indices only ----------
    msg_indices = ""
    if indices_data:
        msg_indices = time_header + "📊 <b>MarketScout (1/4) — Indices</b>\n\n<b>🌍 Indices</b>\n"
        for idx in indices_data:
            name = idx["name"]
            symbol = idx["symbol"]
            price = idx["price"]
            if idx.get("is_vix"):
                d = idx.get("one_day_pct")
                change = f"  1D: {d:+.2f}" if d is not None else ""
                msg_indices += f"<b>{html.escape(name)} ({symbol})</b> {price:.2f}{change}\n\n"
            else:
                d = idx.get("one_day_pct")
                w = idx.get("one_week_pct")
                m = idx.get("one_month_pct")
                six = idx.get("one_6m_pct")
                one_yr = idx.get("one_year_pct")
                three_yr = idx.get("three_year_pct")
                d_str = f"1D: {d:+.2f}" if d is not None else "1D: —"
                w_str = f"1W: {w:+.2f}" if w is not None else "1W: —"
                m_str = f"1M: {m:+.2f}" if m is not None else "1M: —"
                six_str = f"6M: {six:+.2f}" if six is not None else "6M: —"
                one_yr_str = f"1Y: {one_yr:+.2f}" if one_yr is not None else "1Y: —"
                three_yr_str = f"3Y: {three_yr:+.2f}" if three_yr is not None else "3Y: —"
                msg_indices += f"<b>{html.escape(name)} ({symbol})</b> {price:.2f}\n"
                msg_indices += f"  {d_str} | {w_str} | {m_str} | {six_str} | {one_yr_str} | {three_yr_str}\n\n"
        msg_indices = (msg_indices.strip() + SECTION_END) if msg_indices.strip() else ""

    # ---------- Message 2: Big stocks (≥$2B vol) ----------
    big_stocks = []
    for s in stock_sectors_regular:
        big_stocks.extend(by_sector[s])
    msg_big = ""
    if big_stocks:
        msg_big = time_header + "📊 <b>MarketScout (2/4) — Stocks ≥$1B vol</b>\n\n"
        msg_big += f"Found {len(big_stocks)} stock(s) matching criteria:\n\n"
        msg_big += "<b>📈 Stocks (≥$1B vol)</b>\n"
        msg_big += _format_one_stock_block(big_stocks) + SECTION_END

    # ---------- Message 3: Rising stars (250M–$1B vol) ----------
    rising_stocks = by_sector.get("Rising Stars", [])
    msg_rising = ""
    if rising_stocks:
        msg_rising = time_header + "📊 <b>MarketScout (3/4) — Rising Stars</b>\n\n"
        msg_rising += f"Found {len(rising_stocks)} rising star(s) matching criteria:\n\n"
        msg_rising += "<b>⭐ Rising Stars (250M–$1B vol)</b>\n"
        msg_rising += _format_one_stock_block(rising_stocks) + SECTION_END

    # ---------- Message 4: Crypto, Commodities, Forex, ETFs ----------
    msg_rest = ""
    msg_rest = _append_section_block(msg_rest, by_sector, "Crypto", SECTION_EMOJI["Crypto"], is_forex=False, is_commodity=False)
    msg_rest = _append_section_block(msg_rest, by_sector, "Commodities", SECTION_EMOJI["Commodities"], is_forex=False, is_commodity=True)
    msg_rest = _append_section_block(msg_rest, by_sector, "Forex", SECTION_EMOJI["Forex"], is_forex=True, is_commodity=False)
    etf_order = etf_asset_class_order or ["Equity", "Fixed Income", "Commodities", "Currency", "Asset Location", "Alternatives"]
    etf_results = by_sector.get("ETFs", [])
    if etf_results:
        by_asset_class = {}
        for r in etf_results:
            ac = r.get("asset_class", "Other")
            by_asset_class.setdefault(ac, []).append(r)
        msg_rest += "<b>⚖️ ETFs</b>\n"
        for ac in etf_order:
            if ac not in by_asset_class:
                continue
            items = sorted(by_asset_class[ac], key=_pct_sort_key)
            msg_rest += f"  <b>▸ {ac}</b>\n"
            for stock in items:
                symbol = stock["symbol"]
                company_name = stock.get("company_name", symbol)
                ac_label = stock.get("asset_class", ac)
                price = stock["price"]
                vol = stock.get("volume") or 0
                vol_shares = vol / 1_000_000
                dollar_vol = (vol * price) / 1_000_000
                lead = "🟡 " if _all_three_pass(stock) else ""
                d_str = _pct_str_no_pct("1D", stock["one_day_pct"], stock["passes_day"])
                w_str = _pct_str_no_pct("1W", stock["one_week_pct"], stock["passes_week"])
                m_val = stock.get("one_month_pct")
                m_pass = stock.get("passes_month", False)
                m_str = _pct_str_no_pct("1M", m_val, m_pass)
                six_val = stock.get("one_6m_pct")
                one_yr_val = stock.get("one_year_pct")
                three_yr_val = stock.get("three_year_pct")
                six_str = f"6M: {six_val:+.2f}" if six_val is not None else "6M: —"
                one_yr_str = f"1Y: {one_yr_val:+.2f}" if one_yr_val is not None else "1Y: —"
                three_yr_str = f"3Y: {three_yr_val:+.2f}" if three_yr_val is not None else "3Y: —"
                change_str = f"{d_str} | {w_str} | {m_str} | {six_str} | {one_yr_str} | {three_yr_str}"
                tp = stock.get("target_price")
                price_str = f"${price:.2f}" + (f" (1Y: ${tp:.2f})" if tp is not None else "")
                msg_rest += f"{lead}<b>{html.escape(company_name)} ({symbol})</b> {price_str}\n"
                msg_rest += f"  <i>{ac_label}</i>\n"
                msg_rest += f"  {change_str}\n"
                msg_rest += f"  Vol: {vol_shares:.2f}M (${dollar_vol:.1f}M)\n"
                msg_rest += "\n"
            msg_rest += "\n"
        for ac in sorted(by_asset_class.keys()):
            if ac in etf_order:
                continue
            items = sorted(by_asset_class[ac], key=_pct_sort_key)
            msg_rest += f"  <b>▸ {ac}</b>\n"
            for stock in items:
                symbol = stock["symbol"]
                company_name = stock.get("company_name", symbol)
                ac_label = stock.get("asset_class", ac)
                price = stock["price"]
                vol = stock.get("volume") or 0
                vol_shares = vol / 1_000_000
                dollar_vol = (vol * price) / 1_000_000
                lead = "🟡 " if _all_three_pass(stock) else ""
                d_str = _pct_str_no_pct("1D", stock["one_day_pct"], stock["passes_day"])
                w_str = _pct_str_no_pct("1W", stock["one_week_pct"], stock["passes_week"])
                m_val = stock.get("one_month_pct")
                m_pass = stock.get("passes_month", False)
                m_str = _pct_str_no_pct("1M", m_val, m_pass)
                six_val = stock.get("one_6m_pct")
                one_yr_val = stock.get("one_year_pct")
                three_yr_val = stock.get("three_year_pct")
                six_str = f"6M: {six_val:+.2f}" if six_val is not None else "6M: —"
                one_yr_str = f"1Y: {one_yr_val:+.2f}" if one_yr_val is not None else "1Y: —"
                three_yr_str = f"3Y: {three_yr_val:+.2f}" if three_yr_val is not None else "3Y: —"
                change_str = f"{d_str} | {w_str} | {m_str} | {six_str} | {one_yr_str} | {three_yr_str}"
                tp = stock.get("target_price")
                price_str = f"${price:.2f}" + (f" (1Y: ${tp:.2f})" if tp is not None else "")
                msg_rest += f"{lead}<b>{html.escape(company_name)} ({symbol})</b> {price_str}\n"
                msg_rest += f"  <i>{ac_label}</i>\n"
                msg_rest += f"  {change_str}\n"
                msg_rest += f"  Vol: {vol_shares:.2f}M (${dollar_vol:.1f}M)\n"
                msg_rest += "\n"
            msg_rest += "\n"
    if msg_rest.strip():
        msg_rest = time_header + "📊 <b>MarketScout (4/4) — Crypto, Commodities, Forex, ETFs</b>\n\n" + msg_rest.strip() + SECTION_END

    return (msg_indices.strip(), msg_big.strip(), msg_rising.strip(), msg_rest.strip())


def main() -> None:
    """Main screener execution."""
    # Load environment variables
    load_dotenv()
    
    # Load configuration
    config = load_config("config.yaml")
    
    # Check if dry-run mode
    dry_run = config.get("dry_run", False)

    # Quick sample: scan a short symbol list so a real report can be sent in ~1–2 min (for preview)
    quick_sample = (os.getenv("MARKETSCOUT_QUICK_SAMPLE") or "").strip().lower() in ("1", "true", "yes")
    sample_symbols = None
    if quick_sample:
        # Include big names + mid-caps that can qualify as rising stars (250M–1B vol): ULTA, EW, AAL, etc.
        sample_symbols = list(dict.fromkeys([
            "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "JPM", "V", "WMT", "NFLX", "PLTR",
            "BAC", "XOM", "KO", "COST", "PEP", "CMCSA", "NKE", "RIVN", "ADBE", "INTC", "AMD", "AVGO",
            "DIS", "HD", "MCD", "CRM", "ORCL", "ABT", "ACN", "DHR", "GE", "CAT", "UNP", "HON",
            "ULTA", "EW", "AAL", "DAL", "LUV", "CCL", "NCLH", "RCL", "MGM", "WYNN", "LVS", "HAS",
            "BBY", "DKS", "GPS", "ANF", "ROST", "DLTR", "POOL", "FIVE", "WSM", "RH",
        ]))
        print(f"MARKETSCOUT_QUICK_SAMPLE: scanning {len(sample_symbols)} symbols only (real data, quick send).")

    # Capture time when we start pulling from Yahoo (not when we send)
    collection_time = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M %Z")
    print("Fetching indices...")
    indices_data = get_indices_snapshot()

    # Run crypto, forex, commodities, ETFs first (small symbol lists). Then run the long stock scan.
    # This way message 4 is always populated even if the job times out or gets rate-limited during stocks.
    print("Scanning Crypto...")
    crypto_results = run_crypto_screener(config)
    print("Scanning Forex...")
    forex_results = run_forex_screener(config)
    print("Scanning Commodities...")
    commodity_results = run_commodity_screener(config)
    print("Scanning ETFs...")
    etf_results = run_etf_screener(config)

    print("Starting MarketScout stock screener (big + rising stars)...")
    if config.get("rising_stars_thresholds"):
        # Single pass: get both big stocks and rising stars (avoids timeout/rate limits on scheduled runs)
        results, rising_stars_results = run_screener_and_rising_stars(config, symbols_override=sample_symbols)
    else:
        results = run_screener(config, symbols_override=sample_symbols)
        rising_stars_results = []

    all_results = results + rising_stars_results + crypto_results + forex_results + commodity_results + etf_results
    crypto_count = len(crypto_results)
    forex_count = len(forex_results)
    commodity_count = len(commodity_results)
    etf_count = len(etf_results)
    
    # Format into 4 messages: (1) Indices, (2) Big stocks, (3) Rising stars, (4) Crypto+Commodities+Forex+ETFs
    msg_indices, msg_big, msg_rising, msg_rest = format_stock_message(
        all_results,
        crypto_count=crypto_count,
        forex_count=forex_count,
        commodity_count=commodity_count,
        etf_count=etf_count,
        indices_data=indices_data,
        etf_asset_class_order=config.get("etf_asset_class_order"),
        collection_time=collection_time,
    )
    messages = [msg_indices, msg_big, msg_rising, msg_rest]

    # Deep dive: stocks that passed all 3 criteria positively (1D, 1W, 1M all up)
    msg_deep = format_deep_dive_message(all_results, collection_time=collection_time)
    if msg_deep:
        messages.append(msg_deep)

    # Earnings dates: stocks that pass 2 of 3 criteria (regardless of direction)
    msg_earnings = format_earnings_message(all_results, collection_time=collection_time)
    if msg_earnings:
        messages.append(msg_earnings)

    messages = [m for m in messages if m]

    if dry_run:
        print("\n=== DRY RUN MODE ===")
        for i, msg in enumerate(messages, 1):
            path = f"sample_report_{i}.txt"
            Path(path).write_text(msg, encoding="utf-8")
            print(f"Message {i} written to {path} ({len(msg)} chars)")
        print("(Telegram notification skipped)")
    else:
        # Send Telegram notification (4 separate messages). Only this path sends to Telegram; data is live (not mock).
        token = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
        chat_id = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()

        if not token or not chat_id:
            raise RuntimeError(
                "Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID in environment/.env"
            )
        if not chat_id.lstrip("-").isdigit():
            print("Warning: TELEGRAM_CHAT_ID should be numeric (e.g. 123456789 or -1001234567890 for groups).", flush=True)

        for msg in messages:
            send_telegram_message(msg, token, chat_id)
        print(f"\nTelegram notification sent ({len(messages)} messages): {len(results)} stock(s), {len(rising_stars_results)} rising star(s), {etf_count} ETF(s), {crypto_count} crypto, {forex_count} forex, {commodity_count} commodities")
        
        # Generate and send charts only for assets that pass at least 2 of 3 criteria (1D, 1W, 1M)
        results_for_charts = [r for r in all_results if _criteria_count(r) >= 2]
        print(f"  Assets with 2+ criteria (for charts): {len(results_for_charts)}")
        if results_for_charts:
            print("Generating charts...")
            charts = generate_charts_for_results(results_for_charts, config)
            
            if charts:
                chart_paths = [chart_info["chart_path"] for chart_info in charts]
                try:
                    send_telegram_media_group(chart_paths, token, chat_id)
                    print(f"  Charts sent as media group ({len(charts)} charts)")
                except Exception as e:
                    print(f"  Error sending charts: {e}")
                
                # Clean up chart files
                for chart_info in charts:
                    try:
                        if os.path.exists(chart_info["chart_path"]):
                            os.remove(chart_info["chart_path"])
                    except Exception as e:
                        print(f"  Error cleaning up chart {chart_info['symbol']}: {e}")


if __name__ == "__main__":
    import sys
    import traceback
    try:
        main()
    except Exception as e:
        traceback.print_exc()
        print(f"\nFatal error: {e}", file=sys.stderr)
        sys.exit(1)
