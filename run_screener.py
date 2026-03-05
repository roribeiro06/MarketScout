"""Main entry point for MarketScout stock screener."""
import csv
import html
import os
import re
import time
import requests
from datetime import datetime, timedelta
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


def get_indices_snapshot(use_postmarket: bool = False) -> List[Dict]:
    """Fetch current level and 1D/1W/1M/6M/1Y/3Y % changes for indices. Uses live post-market or pre-market price when available."""
    out = []
    for symbol, name in INDICES:
        try:
            ticker = yf.Ticker(symbol)
            data = ticker.history(period="5y")
            if data is None or len(data) < 2:
                continue
            price = round(float(data["Close"].iloc[-1]), 2)
            prev_close = float(data["Close"].iloc[-2])
            one_d = None
            one_w = None
            one_m = None
            one_6m = None
            one_1y = None
            one_3y = None
            if len(data) >= 2:
                one_d = ((data["Close"].iloc[-1] - prev_close) / prev_close) * 100
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
            # Overlay live post-market or pre-market price when available
            try:
                info = ticker.info
                if use_postmarket:
                    live = info.get("postMarketPrice") or info.get("regularMarketPrice") or info.get("currentPrice")
                else:
                    live = info.get("preMarketPrice") or info.get("regularMarketPrice") or info.get("currentPrice")
                if live is not None and isinstance(live, (int, float)) and prev_close and prev_close > 0:
                    p = float(live)
                    if p > 0:
                        price = round(p, 2)
                        one_d = ((p - prev_close) / prev_close) * 100
            except Exception:
                pass
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


def _get_next_delivery_target(config: dict):
    """Return (next_target_datetime_et, index_in_list) or (None, None). Index 0 = first slot = premarket run.
    Uses a 5-min window so when the job runs at 8pm ET we get the 8pm slot and send when done."""
    delivery_times = config.get("delivery_times_et") or []
    if isinstance(delivery_times, str):
        delivery_times = [delivery_times]
    if not delivery_times:
        return None, None
    now_et = datetime.now(ZoneInfo("America/New_York"))
    today = now_et.date()
    cutoff = now_et - timedelta(minutes=5)  # so 8pm run gets 8pm slot
    next_target = None
    next_index = None
    for i, s in enumerate(delivery_times):
        if not s or not isinstance(s, str):
            continue
        parts = s.strip().split(":")
        if len(parts) != 2:
            continue
        try:
            h, m = int(parts[0]), int(parts[1])
            if 0 <= h <= 23 and 0 <= m <= 59:
                target = datetime(today.year, today.month, today.day, h, m, 0, tzinfo=ZoneInfo("America/New_York"))
                if target >= cutoff and (next_target is None or target < next_target):
                    next_target = target
                    next_index = i
        except (ValueError, TypeError):
            continue
    return next_target, next_index


def _pct_sort_key(item: dict) -> tuple:
    """Sort key: largest move in any period that fit the criteria first (by magnitude)."""
    d = item.get("pct_4pm_to_8pm") if item.get("pct_4pm_to_8pm") is not None else item.get("one_day_pct")
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


def _all_three_pass_and_positive(item: dict) -> bool:
    """True if asset passes all 3 criteria AND all three moves (1D, 1W, 1M) are positive."""
    if not _all_three_pass(item):
        return False
    d = item.get("one_day_pct")
    w = item.get("one_week_pct")
    m = item.get("one_month_pct")
    if d is None or w is None or m is None:
        return False
    return d > 0 and w > 0 and m > 0


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


def _first_value_from_df_row(df, *row_labels):
    """Get first numeric value from a DataFrame row (row = line item, columns = dates). Returns None if not found."""
    if df is None or getattr(df, "empty", True) or not hasattr(df, "index"):
        return None
    for label in row_labels:
        if label not in df.index:
            continue
        try:
            row = df.loc[label]
            if hasattr(row, "iloc"):
                for i in range(len(row)):
                    val = row.iloc[i]
                    if val is not None and isinstance(val, (int, float)) and val == val:  # not nan
                        return float(val)
            elif len(row):
                val = row[0] if hasattr(row, "__getitem__") else None
                if val is not None and isinstance(val, (int, float)) and val == val:
                    return float(val)
        except Exception:
            pass
    return None


def _fetch_stock_financial_stats(symbol: str, delay_seconds: float = 2.0) -> Optional[Dict]:
    """Fetch financial stats for a stock from yfinance. Returns dict or None on total failure.
    Tries info, fast_info, balance_sheet, income_stmt, cashflow and quarterly variants with retries."""
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

    max_attempts = 6
    for attempt in range(max_attempts):
        stats = dict(empty_stats)
        try:
            t = yf.Ticker(symbol)

            # 1) fast_info first (lightweight, often works when info is throttled)
            try:
                fast = getattr(t, "fast_info", None)
                if fast is not None:
                    mc = getattr(fast, "market_cap", None)
                    if mc is not None:
                        stats["market_cap"] = float(mc)
            except Exception:
                pass

            # 2) info dict (can be empty or partial under rate limit)
            info = None
            try:
                info = t.info
            except Exception:
                pass
            if isinstance(info, dict) and len(info) > 2:
                shares = _get(info, "sharesOutstanding") or _get(info, "impliedSharesOutstanding")
                if stats["market_cap"] is None:
                    stats["market_cap"] = _get(info, "marketCap") or _get(info, "enterpriseValue")
                stats["profit_margin"] = _get(info, "profitMargins")
                stats["total_revenue"] = _get(info, "totalRevenue")
                stats["gross_profit"] = _get(info, "grossProfits")
                stats["total_cash"] = _get(info, "totalCash") or _get(info, "cash")
                stats["total_debt"] = _get(info, "totalDebt")
                stats["operating_cashflow"] = _get(info, "operatingCashflow") or _get(info, "freeCashflow")
                stats["forward_dividend"] = _get(info, "forwardDividendRate") or _get(info, "dividendRate")
                stats["dividend_yield"] = _get(info, "dividendYield")
                stats["revenue_per_share"] = _get(info, "revenuePerShare")
                if stats["revenue_per_share"] is None and stats["total_revenue"] and shares:
                    stats["revenue_per_share"] = stats["total_revenue"] / shares
                stats["cash_per_share"] = _get(info, "totalCashPerShare")
                if stats["cash_per_share"] is None and stats["total_cash"] and shares:
                    stats["cash_per_share"] = stats["total_cash"] / shares

            # 3) Balance sheet (annual then quarterly; try get_* with freq for quarterly)
            for attr in ("balance_sheet", "quarterly_balance_sheet"):
                try:
                    bs = getattr(t, attr, None)
                    if bs is None and attr == "balance_sheet":
                        bs = getattr(t, "get_balance_sheet", lambda: None)()
                    if bs is None and attr == "quarterly_balance_sheet":
                        get_bs = getattr(t, "get_balance_sheet", None)
                        if callable(get_bs):
                            try:
                                bs = get_bs(freq="quarterly")
                            except TypeError:
                                bs = get_bs()
                    if bs is None or getattr(bs, "empty", True):
                        continue
                    v = _first_value_from_df_row(
                        bs, "Total Current Assets", "Current Assets", "Current Assets And Other"
                    )
                    if v is not None:
                        stats["current_assets"] = stats["current_assets"] or v
                    v = _first_value_from_df_row(
                        bs, "Total Current Liabilities", "Current Liabilities", "Current Liabilities And Other"
                    )
                    if v is not None:
                        stats["current_liabilities"] = stats["current_liabilities"] or v
                    if stats["current_assets"] is not None and stats["current_liabilities"] is not None:
                        break
                except Exception:
                    pass

            # 4) Income statement (annual then quarterly)
            for attr in ("income_stmt", "quarterly_income_stmt"):
                try:
                    inc = getattr(t, attr, None)
                    if inc is None and attr == "income_stmt":
                        inc = getattr(t, "get_income_stmt", lambda: None)()
                    if inc is None or getattr(inc, "empty", True):
                        continue
                    v = _first_value_from_df_row(
                        inc,
                        "Operating Income",
                        "Operating Income Loss",
                        "Total Operating Income As Reported",
                        "EBIT",
                    )
                    if v is not None:
                        stats["operating_income"] = stats["operating_income"] or v
                        break
                except Exception:
                    pass

            # 5) Cash flow for operating cash flow if still missing
            if stats["operating_cashflow"] is None:
                for attr in ("cashflow", "quarterly_cashflow"):
                    try:
                        cf = getattr(t, attr, None)
                        if cf is None and attr == "cashflow":
                            cf = getattr(t, "get_cashflow", lambda: None)()
                        if cf is None or getattr(cf, "empty", True):
                            continue
                        v = _first_value_from_df_row(
                            cf,
                            "Operating Cash Flow",
                            "Cash Flow From Continuing Operating Activities",
                            "Operating Cash Flow",
                        )
                        if v is not None:
                            stats["operating_cashflow"] = v
                            break
                    except Exception:
                        pass

            if any(v is not None for v in stats.values()):
                return stats
        except Exception as e:
            print(f"  [DEEP] Attempt {attempt + 1}/{max_attempts} for {symbol}: {e}", flush=True)
        if attempt < max_attempts - 1:
            time.sleep(5.0 + attempt * 3)

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


def _format_one_stock_block(stocks: list, day_label: Optional[str] = None) -> str:
    """Format a list of stocks (big or rising stars) into a message block. day_label: optional override (e.g. post-market shows 1D + 4pm→8pm)."""
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
        d_str = _pct_str_no_pct("1D", stock["one_day_pct"], stock["passes_day"] if stock.get("pct_4pm_to_8pm") is None else False)
        pct_4pm = stock.get("pct_4pm_to_8pm")
        if pct_4pm is not None:
            pm_str = _pct_str_no_pct("4pm→8pm", pct_4pm, stock["passes_day"])
            d_str = f"{d_str} | {pm_str}"
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
        p4 = stock.get("regular_session_close")
        if pct_4pm is not None and p4 is not None:
            price_str = f"4pm: ${p4:.2f} → 8pm: ${price:.2f}" + (f" (1Y: ${tp:.2f})" if tp is not None else "")
        else:
            price_str = f"${price:.2f}" + (f" (1Y: ${tp:.2f})" if tp is not None else "")
        lines.append(f"{lead}<b>{html.escape(company_name)} ({symbol})</b> {price_str}")
        lines.append(f"  <i>{sector_name}</i>")
        lines.append(f"  {change_str}")
        if vol > 0:
            lines.append(f"  Vol: {vol_shares:.2f}M (${dollar_vol:.1f}M)")
        lines.append("")
    return "\n".join(lines).strip()


def format_deep_dive_message(
    qualifying: list,
    collection_time: Optional[str] = None,
    title: str = "Deep Dive (1D, 1W, 1M all three criteria met)",
    intro: str = "Stocks below pass all three criteria AND have 1D, 1W, and 1M all positive with key financials:",
) -> str:
    """
    Format a deep dive message for a list of stocks. Caller passes the qualifying list.
    """
    if not qualifying:
        return ""

    SECTION_END = "\n\n🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵"
    time_header = ("🕐 Yahoo data as of " + collection_time + "\n\n" if collection_time else "")
    msg = time_header + f"📊 <b>MarketScout — {title}</b>\n\n"
    msg += f"Stocks in this report: <b>{len(qualifying)}</b>\n\n"
    msg += intro + "\n\n"

    # Brief pause before fetching financials (helps avoid Yahoo rate limit after stock scan)
    time.sleep(6.0)
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

        stats = _fetch_stock_financial_stats(symbol, delay_seconds=4.0)
        if not stats:
            time.sleep(3.0)
            stats = _fetch_stock_financial_stats(symbol, delay_seconds=12.0)
        if not stats:
            time.sleep(4.0)
            stats = _fetch_stock_financial_stats(symbol, delay_seconds=20.0)
        if not stats:
            msg += "  (Financial data unavailable)\n\n"
            continue

        # Core stats
        msg += f"  Market cap: {_format_big_num(stats['market_cap'])}\n"
        pm = stats.get("profit_margin")
        if pm is not None and isinstance(pm, (int, float)) and pm == pm:
            pm_pct = (pm * 100) if abs(pm) < 1.1 else pm  # Yahoo: decimal (0.15) or already % (15)
            msg += f"  Profit margin: {pm_pct:.1f}%\n"
        else:
            msg += "  Profit margin: —\n"
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


def format_premarket_messages(
    indices_data: Optional[List[Dict]],
    all_results: list,
    collection_time: Optional[str] = None,
) -> List[str]:
    """
    For the first delivery slot (8 PM): indices + only stocks that pass 1-day criteria.
    Uses live post-market/after-hours prices (already in all_results from screener). Same volume criteria (high vol + rising stars).
    No crypto/forex/commodities/ETFs.
    """
    non_stock = {"Crypto", "Forex", "Commodities", "ETFs"}
    stocks_1d = [
        r for r in all_results
        if r.get("sector") not in non_stock and r.get("passes_day")
    ]
    SECTION_END = "\n\n🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵"
    time_header = ("🕐 Yahoo data as of " + collection_time + "\n\n" if collection_time else "")
    messages = []

    if indices_data:
        msg_indices = time_header + "📊 <b>MarketScout — Post-market (1/2) Indices</b>\n\n<b>🌍 Indices</b>\n"
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
        messages.append((msg_indices.strip() + SECTION_END).strip())

    msg_stocks = time_header + "📊 <b>MarketScout — Post-market (2/2) Stocks (4pm → 8pm)</b>\n\n"
    msg_stocks += f"Stocks with ±3% move from 4pm close to 8pm post-market (same volume criteria as 4pm): {len(stocks_1d)}\n\n"
    msg_stocks += _format_one_stock_block(stocks_1d) + SECTION_END
    messages.append(msg_stocks.strip())

    return [m for m in messages if m]


def _log_price_tracking_archive(
    results_to_log: list,
    report_type: str,
    config: dict,
) -> Tuple[Optional[List[Dict]], Optional[Dict]]:
    """
    Append each stock appearance to archive. Returns (stocks_with_pct_next, yesterday_8pm_by_symbol)
    for building the 4pm tracking Telegram message.
    """
    import csv
    from datetime import timedelta
    non_stock = {"Crypto", "Forex", "Commodities", "ETFs"}
    stocks_only = [r for r in results_to_log if r.get("sector") not in non_stock]
    if not stocks_only:
        return None, None
    try:
        paths = config.get("paths") or {}
        logs_dir = paths.get("logs_dir", "logs")
        Path(logs_dir).mkdir(parents=True, exist_ok=True)
        now = datetime.now(ZoneInfo("America/New_York"))
        date_str = now.strftime("%Y-%m-%d")
        time_str = now.strftime("%H:%M")
        ts = now.strftime("%Y-%m-%d %H:%M %Z")
        yesterday_str = (now - timedelta(days=1)).strftime("%Y-%m-%d")
        filename = os.path.join(logs_dir, "price_tracking_archive.csv")
        file_exists = os.path.exists(filename)
        yesterday_8pm = {}
        if file_exists:
            with open(filename, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get("date") == yesterday_str and row.get("report") == "8pm":
                        s = row.get("symbol", "")
                        p8 = row.get("price_8pm", "")
                        if s and p8:
                            yesterday_8pm[s] = p8
        fields = ["timestamp", "date", "time", "symbol", "name", "report", "price_4pm", "price_8pm", "pct_change_4pm_to_8pm", "pct_change_8pm_to_next_4pm"]
        to_append = []
        stocks_with_pct = []
        for r in stocks_only:
            symbol = r.get("symbol", "")
            name = (r.get("company_name") or symbol).replace(",", ";")
            if report_type == "4pm":
                price_4pm = r.get("regular_session_close") or r.get("price")
                p8_prev = yesterday_8pm.get(symbol, "")
                pct_next = ""
                if price_4pm and p8_prev:
                    try:
                        p4, p8 = float(price_4pm), float(p8_prev)
                        if p8 > 0:
                            pct_next = f"{((p4 - p8) / p8) * 100:.2f}"
                            stocks_with_pct.append({
                                "symbol": symbol, "name": name,
                                "price_8pm": p8_prev, "price_4pm": price_4pm,
                                "pct_change_8pm_to_next_4pm": pct_next,
                            })
                    except (ValueError, TypeError):
                        pass
                to_append.append({"timestamp": ts, "date": date_str, "time": time_str, "symbol": symbol, "name": name, "report": "4pm",
                    "price_4pm": price_4pm or "", "price_8pm": "", "pct_change_4pm_to_8pm": "", "pct_change_8pm_to_next_4pm": pct_next})
            else:
                price_4pm = r.get("regular_session_close")
                price_8pm = r.get("price")
                pct = ""
                if price_4pm and price_8pm and float(price_4pm) > 0:
                    pct = f"{((float(price_8pm) - float(price_4pm)) / float(price_4pm)) * 100:.2f}"
                to_append.append({"timestamp": ts, "date": date_str, "time": time_str, "symbol": symbol, "name": name, "report": "8pm",
                    "price_4pm": price_4pm or "", "price_8pm": price_8pm or "", "pct_change_4pm_to_8pm": pct, "pct_change_8pm_to_next_4pm": ""})
        with open(filename, "a" if file_exists else "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            if not file_exists:
                writer.writeheader()
            for row in to_append:
                writer.writerow(row)
        print(f"  Archived {len(stocks_only)} stock appearances to {filename}", flush=True)
        return (stocks_with_pct, yesterday_8pm) if report_type == "4pm" else (None, None)
    except Exception as e:
        print(f"  [LOG] Could not write price archive: {e}", flush=True)
        return None, None


def _format_tracking_summary_4pm(
    stocks_with_pct: list,
    collection_time: Optional[str] = None,
) -> str:
    """Format 4pm tracking message: today 4pm vs yesterday 8pm."""
    if not stocks_with_pct:
        return ""
    SECTION_END = "\n\n🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵"
    time_header = ("🕐 " + (collection_time or "") + "\n\n" if collection_time else "")
    msg = time_header + "📈 <b>MarketScout — Price Tracking (4pm vs yesterday 8pm)</b>\n\n"
    msg += "Stocks in today's 4pm report that also appeared in yesterday's 8pm report:\n\n"
    for s in sorted(stocks_with_pct, key=lambda x: -abs(float(x.get("pct_change_8pm_to_next_4pm", 0) or 0))):
        sym = s.get("symbol", "")
        name = (s.get("name") or sym).replace(",", ";")
        p8 = s.get("price_8pm", "")
        p4 = s.get("price_4pm", "")
        pct = s.get("pct_change_8pm_to_next_4pm", "")
        emoji = "🟢" if pct and float(pct) >= 0 else "🔴"
        msg += f"{emoji} <b>{html.escape(name)} ({sym})</b>\n"
        msg += f"  Yesterday 8pm: ${p8} → Today 4pm: ${p4} ({pct}%)\n\n"
    return (msg.strip() + SECTION_END).strip()


def _format_tracking_no_data_4pm(collection_time: Optional[str] = None) -> str:
    """Format 4pm tracking message when there is no yesterday 8pm data to compare."""
    SECTION_END = "\n\n🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵"
    time_header = ("🕐 " + (collection_time or "") + "\n\n" if collection_time else "")
    msg = time_header + "📈 <b>MarketScout — Price Tracking (4pm vs yesterday 8pm)</b>\n\n"
    msg += "No yesterday 8pm data in the archive (e.g. the 8pm report did not run). "
    msg += "The comparison will appear after the next 8pm report runs."
    return (msg.strip() + SECTION_END).strip()


def _append_appearance_log(
    results_to_log: list,
    report_slot: str,
    config: dict,
) -> None:
    """Append each appearance to logs/appearance_archive.csv (date, symbol, name, report, price, pct) for 12pm report."""
    if report_slot not in ("8am", "4pm", "5pm", "8pm") or not results_to_log:
        return
    non_stock = {"Crypto", "Forex", "Commodities", "ETFs"}
    rows = []
    for r in results_to_log:
        if r.get("sector") in non_stock and report_slot != "4pm":
            continue
        symbol = r.get("symbol", "")
        name = (r.get("company_name") or symbol).replace(",", ";")
        price = r.get("price") or r.get("regular_session_close")
        if report_slot == "4pm":
            pct = r.get("one_day_pct")
        elif report_slot in ("5pm", "8pm"):
            pct = r.get("pct_4pm_to_8pm")
        else:
            pct = r.get("one_day_pct")
        if symbol and price is not None:
            pct_val = pct if pct is not None else ""
            rows.append({"date": "", "symbol": symbol, "name": name, "report": report_slot, "price": price, "pct": pct_val})
    if not rows:
        return
    try:
        paths = config.get("paths") or {}
        logs_dir = paths.get("logs_dir", "logs")
        Path(logs_dir).mkdir(parents=True, exist_ok=True)
        now = datetime.now(ZoneInfo("America/New_York"))
        date_str = now.strftime("%Y-%m-%d")
        filename = os.path.join(logs_dir, "appearance_archive.csv")
        file_exists = os.path.exists(filename)
        fields = ["date", "symbol", "name", "report", "price", "pct"]
        with open(filename, "a" if file_exists else "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            if not file_exists:
                writer.writeheader()
            for row in rows:
                row["date"] = date_str
                writer.writerow(row)
        print(f"  Appearance log: {len(rows)} rows for {report_slot} -> {filename}", flush=True)
    except Exception as e:
        print(f"  [LOG] Could not write appearance archive: {e}", flush=True)


def _run_12pm_tracking_report(config: dict) -> None:
    """Build and send 12pm tracking report: Part 1 = 4pm log (6 months), Part 2 = 8am/5pm/8pm log (3 days)."""
    load_dotenv()
    paths = config.get("paths") or {}
    logs_dir = paths.get("logs_dir", "logs")
    filename = os.path.join(logs_dir, "appearance_archive.csv")
    if not os.path.exists(filename):
        print("No appearance_archive.csv; skipping 12pm report.", flush=True)
        return
    now_et = datetime.now(ZoneInfo("America/New_York"))
    today_str = now_et.strftime("%Y-%m-%d")
    six_months_ago = (now_et - timedelta(days=180)).strftime("%Y-%m-%d")
    three_days_ago = (now_et - timedelta(days=3)).strftime("%Y-%m-%d")

    # Read archive
    rows_4pm = []  # (date, symbol, name, pct)
    rows_short = []  # (date, symbol, name, report, pct)
    with open(filename, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            d = row.get("date", "")
            s = row.get("symbol", "")
            rpt = row.get("report", "")
            if rpt == "4pm" and d >= six_months_ago:
                rows_4pm.append((d, s, row.get("name", s), row.get("pct", "")))
            elif rpt in ("8am", "5pm", "8pm") and d >= three_days_ago:
                rows_short.append((d, s, row.get("name", s), rpt, row.get("pct", "")))

    # Part 1: 4pm symbols from last 6 months (unique symbols, show each with history of dates + pct)
    from collections import defaultdict
    by_sym_4pm = defaultdict(list)
    for d, sym, name, pct in rows_4pm:
        by_sym_4pm[sym].append((d, pct))
    # Sort by symbol, each list of (date, pct) sorted by date
    symbols_4pm = sorted(by_sym_4pm.keys())
    for sym in symbols_4pm:
        by_sym_4pm[sym].sort(key=lambda x: x[0])

    # Part 2: 8am/5pm/8pm symbols that have first appearance in last 3 days
    first_seen = {}
    for d, sym, _n, rpt, _p in rows_short:
        key = (sym, rpt)
        if key not in first_seen or d < first_seen[key]:
            first_seen[key] = d
    # Include symbol if any of its (sym, 8am), (sym, 5pm), (sym, 8pm) has first_seen >= three_days_ago
    short_term_symbols = set()
    for (sym, rpt), first_d in first_seen.items():
        if first_d >= three_days_ago:
            short_term_symbols.add(sym)
    by_sym_short = defaultdict(list)
    for d, sym, name, rpt, pct in rows_short:
        if sym in short_term_symbols:
            by_sym_short[sym].append((d, rpt, pct))
    for sym in by_sym_short:
        by_sym_short[sym].sort(key=lambda x: x[0])

    # Fetch current prices for all symbols
    all_syms = list(set(symbols_4pm) | short_term_symbols)
    prices = {}
    for sym in all_syms:
        try:
            t = yf.Ticker(sym)
            info = t.info or {}
            p = info.get("regularMarketPrice") or info.get("currentPrice") or info.get("previousClose")
            if p is not None:
                prices[sym] = float(p)
        except Exception:
            pass

    SECTION_END = "\n\n🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵"
    collection_time = now_et.strftime("%Y-%m-%d %H:%M %Z")
    time_header = "🕐 " + collection_time + "\n\n"
    msg_part1 = time_header + "📋 <b>MarketScout — 12pm Tracking (Part 1: 4pm log, 6 months)</b>\n\n"
    msg_part1 += "Assets that appeared in 4pm reports (today's price + logged 4pm % moves):\n\n"
    for sym in symbols_4pm:
        name = next((r[2] for r in rows_4pm if r[1] == sym), sym)
        cur = prices.get(sym)
        cur_str = f"${cur:.2f}" if cur is not None else "—"
        log_str = "; ".join(f"{d} ({pct}%)" for d, pct in by_sym_4pm[sym])
        msg_part1 += f"<b>{html.escape(name)} ({sym})</b> now {cur_str}\n  Log: {log_str}\n\n"
    msg_part1 = (msg_part1.strip() + SECTION_END).strip()

    msg_part2 = time_header + "📋 <b>MarketScout — 12pm Tracking (Part 2: 8am/5pm/8pm, 3 days)</b>\n\n"
    msg_part2 += "Assets that appeared in 8am/5pm/8pm in last 3 days (today's price + logged moves):\n\n"
    for sym in sorted(by_sym_short.keys()):
        entries = by_sym_short[sym]
        name = next((r[2] for r in rows_short if r[1] == sym), sym)
        cur = prices.get(sym)
        cur_str = f"${cur:.2f}" if cur is not None else "—"
        log_str = "; ".join(f"{d} {rpt} ({pct}%)" for d, rpt, pct in entries)
        msg_part2 += f"<b>{html.escape(name)} ({sym})</b> now {cur_str}\n  Log: {log_str}\n\n"
    msg_part2 = (msg_part2.strip() + SECTION_END).strip()

    token = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    chat_id = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()
    if not token or not chat_id:
        print("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID; 12pm report not sent.", flush=True)
        return
    send_telegram_message(msg_part1, token, chat_id)
    send_telegram_message(msg_part2, token, chat_id)
    print("12pm tracking report sent (Part 1 + Part 2).", flush=True)


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
    Stocks that appear on the report (big or rising stars) with an earnings date within the next 30 days.
    """
    non_stock = {"Crypto", "Forex", "Commodities", "ETFs"}
    # All stocks on the report (rising stars or bigger stocks)
    qualifying = [
        r for r in all_results
        if r.get("sector") not in non_stock
    ]
    if not qualifying:
        return ""

    SECTION_END = "\n\n🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵"
    time_header = ("🕐 Yahoo data as of " + collection_time + "\n\n" if collection_time else "")
    msg = time_header + "📅 <b>MarketScout — Earnings in the next 30 days</b>\n\n"
    msg += "Stocks on the report (big or rising stars) with earnings in the next 30 days (ordered by nearest date):\n\n"

    time.sleep(1.5)
    today = datetime.now(ZoneInfo("America/New_York")).date()
    max_days_ahead = 30

    def _earnings_sort_key(item):
        stock, earnings_str = item
        if not earnings_str:
            return (1, 999999)  # no date last
        try:
            parts = earnings_str.split("-")
            if len(parts) == 3:
                ed = today.__class__(int(parts[0]), int(parts[1]), int(parts[2]))
                days = (ed - today).days
                return (0, days if 0 <= days <= max_days_ahead else 999998)
        except (ValueError, IndexError):
            pass
        return (1, 999999)

    # Fetch earnings date once per stock; keep only upcoming within next 30 days
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
                days = (ed - today).days
                if 0 <= days <= max_days_ahead:
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
    
    # Determine which delivery slot we're in (0=8am, 1=12pm, 2=4pm, 3=5pm, 4=8pm)
    next_delivery_target, delivery_slot_index = _get_next_delivery_target(config)
    REPORT_SLOTS = ["8am", "12pm", "4pm", "5pm", "8pm"]
    report_slot = REPORT_SLOTS[delivery_slot_index] if delivery_slot_index is not None else None

    # Manual trigger: send slot 0 report (8am pre-market) right now
    force_report_1 = (os.getenv("MARKETSCOUT_REPORT_1") or "").strip().lower() in ("1", "true", "yes")
    if force_report_1:
        delivery_slot_index = 0
        report_slot = "8am"
        next_delivery_target = None  # send immediately, no wait
        print("MARKETSCOUT_REPORT_1=1: building 8am report and sending immediately.", flush=True)

    # Manual trigger: send 8pm report right now (scan and send when done)
    force_report_8pm = (os.getenv("MARKETSCOUT_REPORT_8PM") or "").strip().lower() in ("1", "true", "yes")
    if force_report_8pm:
        delivery_slot_index = 4
        report_slot = "8pm"
        next_delivery_target = None  # send when scan is done, no wait
        print("MARKETSCOUT_REPORT_8PM=1: building 8pm report and sending when done.", flush=True)

    # 12pm = tracking log only (no screener run)
    if report_slot == "12pm":
        _run_12pm_tracking_report(config)
        return
    
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
    # Indices: pre-market for 8am, regular for 4pm, post-market for 5pm/8pm
    use_postmarket_indices = report_slot in ("5pm", "8pm")
    indices_data = get_indices_snapshot(use_postmarket=use_postmarket_indices)

    # Run crypto, forex, commodities, ETFs first (small symbol lists). Then run the long stock scan.
    # This way message 4 is always populated even if the job times out or gets rate-limited during stocks.
    print("Scanning Crypto...")
    crypto_results = run_crypto_screener(config)
    if report_slot == "4pm":
        crypto_results = [r for r in crypto_results if r.get("symbol") in ("BTC-USD", "ETH-USD")]
    print("Scanning Forex...")
    forex_results = run_forex_screener(config)
    print("Scanning Commodities...")
    commodity_results = run_commodity_screener(config)
    print("Scanning ETFs...")
    etf_results = run_etf_screener(config)

    print("Starting MarketScout stock screener (big + rising stars)...")
    use_postmarket_stocks = report_slot in ("5pm", "8pm")
    if config.get("rising_stars_thresholds"):
        # Single pass: get both big stocks and rising stars (avoids timeout/rate limits on scheduled runs)
        results, rising_stars_results = run_screener_and_rising_stars(
            config, symbols_override=sample_symbols, use_postmarket_prices=use_postmarket_stocks, report_slot=report_slot
        )
    else:
        results = run_screener(config, symbols_override=sample_symbols, use_postmarket_prices=use_postmarket_stocks, report_slot=report_slot)
        rising_stars_results = []

    all_results = results + rising_stars_results + crypto_results + forex_results + commodity_results + etf_results
    crypto_count = len(crypto_results)
    forex_count = len(forex_results)
    commodity_count = len(commodity_results)
    etf_count = len(etf_results)

    # 8am / 5pm / 8pm: only indices, stocks, ETFs (no crypto, forex, commodities in message)
    if report_slot in ("8am", "5pm", "8pm"):
        results_for_message = [r for r in all_results if r.get("sector") not in ("Crypto", "Forex", "Commodities")]
        _crypto_count = _forex_count = _commodity_count = 0
    else:
        results_for_message = all_results
        _crypto_count, _forex_count, _commodity_count = crypto_count, forex_count, commodity_count

    # All slots: full report (for 8am/5pm/8pm: indices + stocks + ETFs only)
    msg_indices, msg_big, msg_rising, msg_rest = format_stock_message(
        results_for_message,
        crypto_count=_crypto_count,
        forex_count=_forex_count,
        commodity_count=_commodity_count,
        etf_count=etf_count,
        indices_data=indices_data,
        etf_asset_class_order=config.get("etf_asset_class_order"),
        collection_time=collection_time,
    )
    messages = [msg_indices, msg_big, msg_rising, msg_rest]

    # Earnings dates (4pm slot only)
    if report_slot == "4pm":
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

        # If delivery_times_et is set, wait until the next target time today (Eastern) before sending.
        # 8pm report: start at 8pm and send when done (no wait). Set env MARKETSCOUT_SEND_NOW=1 to skip wait for any slot.
        send_now = (os.getenv("MARKETSCOUT_SEND_NOW") or "").strip().lower() in ("1", "true", "yes")
        if report_slot == "8pm":
            next_delivery_target = None  # 8pm: scan starts at 8pm, send whenever done
        if not send_now and next_delivery_target is not None:
            now_et = datetime.now(ZoneInfo("America/New_York"))
            wait_sec = (next_delivery_target - now_et).total_seconds()
            if wait_sec > 0:
                print(f"Waiting until {next_delivery_target.strftime('%H:%M')} ET to send ({wait_sec:.0f}s)...", flush=True)
                time.sleep(wait_sec)

        for msg in messages:
            send_telegram_message(msg, token, chat_id)
        print(f"\nTelegram notification sent ({len(messages)} messages): {len(results)} stock(s), {len(rising_stars_results)} rising star(s), {etf_count} ETF(s), {crypto_count} crypto, {forex_count} forex, {commodity_count} commodities")

        # Archive every stock appearance (append-only) and send 4pm tracking summary
        non_stock = {"Crypto", "Forex", "Commodities", "ETFs"}
        if report_slot == "8pm":
            stocks_8pm = [r for r in all_results if r.get("sector") not in non_stock and r.get("passes_day")]
            _log_price_tracking_archive(stocks_8pm, "8pm", config)
            _append_appearance_log(stocks_8pm, "8pm", config)
        elif report_slot == "4pm":
            stocks_full = [r for r in all_results if r.get("sector") not in non_stock]
            _log_price_tracking_archive(stocks_full, "4pm", config)
            _append_appearance_log(all_results, "4pm", config)
        elif report_slot == "5pm":
            stocks_5pm = [r for r in all_results if r.get("sector") not in non_stock and r.get("passes_day")]
            _append_appearance_log(stocks_5pm, "5pm", config)
        elif report_slot == "8am":
            stocks_8am = [r for r in all_results if r.get("sector") not in non_stock and r.get("passes_day")]
            _append_appearance_log(stocks_8am, "8am", config)

        # Charts only for 4pm report
        if report_slot == "4pm":
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
