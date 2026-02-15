"""Main entry point for MarketScout stock screener."""
import html
import os
import requests
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import yfinance as yf
from screener.screener import load_config, run_screener, run_crypto_screener, run_forex_screener, run_commodity_screener, run_etf_screener
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
    """Convert our simple HTML (only <b>/</b> and entities) to plain text for Telegram fallback."""
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
            if e.response.status_code == 400 and "entities" in (e.response.text or "").lower():
                plain_chunk = _strip_html_to_plain(chunk)
                print("Retrying as plain text (HTML rejected).", flush=True)
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
    stocks = sorted(stocks, key=lambda x: x.get("company_name", x["symbol"]).upper())
    message += f"<b>{emoji} {sector}</b>\n"
    for stock in stocks:
        symbol = stock["symbol"]
        company_name = stock.get("company_name", symbol)
        price = stock["price"]
        vol = stock.get("volume") or 0
        vol_shares = vol / 1_000_000
        dollar_vol = (vol * price) / 1_000_000

        def pct_str(label: str, pct: Optional[float], passes: bool) -> str:
            if pct is None:
                return f"{label}: —"
            s = f"{label}: {pct:+.2f}%"
            return f"<b>{s}</b>" if passes else s

        d_str = pct_str("1D", stock["one_day_pct"], stock["passes_day"])
        w_str = pct_str("1W", stock["one_week_pct"], stock["passes_week"])
        m_val = stock.get("one_month_pct")
        m_pass = stock.get("passes_month", False)
        m_str = pct_str("1M", m_val, m_pass)
        six_val = stock.get("one_6m_pct")
        one_yr_val = stock.get("one_year_pct")
        three_yr_val = stock.get("three_year_pct")
        six_str = f"6M: {six_val:+.2f}%" if six_val is not None else "6M: —"
        one_yr_str = f"1Y: {one_yr_val:+.2f}%" if one_yr_val is not None else "1Y: —"
        three_yr_str = f"3Y: {three_yr_val:+.2f}%" if three_yr_val is not None else "3Y: —"
        change_str = f"{d_str} | {w_str} | {m_str} | {six_str} | {one_yr_str} | {three_yr_str}"
        if is_forex:
            message += f"<b>{html.escape(company_name)} ({symbol})</b> {price:.4f}\n"
        else:
            message += f"<b>{html.escape(company_name)} ({symbol})</b> ${price:.2f}\n"
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


def format_stock_message(
    results: list,
    crypto_count: int = 0,
    forex_count: int = 0,
    commodity_count: int = 0,
    etf_count: int = 0,
    indices_data: Optional[List[Dict]] = None,
    etf_asset_class_order: Optional[List[str]] = None,
) -> Tuple[str, str]:
    """Format screening results into two Telegram messages: (1) Indices, Stocks; (2) Crypto, Commodities, Forex, ETFs."""
    stock_count = len(results) - crypto_count - forex_count - commodity_count - etf_count
    if not results and not indices_data:
        return (
            "📊 <b>MarketScout Scan</b>\n\nNo stocks, crypto, forex, commodities, or ETFs found matching criteria.",
            "",
        )

    by_sector = {}
    for stock in results:
        sector = stock.get("sector", "Other")
        by_sector.setdefault(sector, []).append(stock)
    non_stock = {"Crypto", "Forex", "Commodities", "ETFs"}
    stock_sectors = sorted([s for s in by_sector if s not in non_stock], key=lambda s: (s == "Other", s.upper()))
    SECTION_EMOJI = {"Crypto": "🪙", "Commodities": "🌾", "Forex": "💵"}

    # ---------- Message 1: Indices, Stocks, Crypto ----------
    msg1 = "📊 <b>MarketScout Scan</b>\n"
    parts = []
    if stock_count:
        parts.append(f"{stock_count} stock(s)")
    if etf_count:
        parts.append(f"{etf_count} ETF(s)")
    if crypto_count:
        parts.append(f"{crypto_count} crypto")
    if forex_count:
        parts.append(f"{forex_count} forex")
    if commodity_count:
        parts.append(f"{commodity_count} commodities")
    if parts:
        msg1 += f"Found {' + '.join(parts)} matching criteria:\n\n"
    elif indices_data:
        msg1 += "Indices snapshot:\n\n"
    else:
        msg1 += "\n"

    if indices_data:
        msg1 += "<b>🌍 Indices</b>\n"
        for idx in indices_data:
            name = idx["name"]
            symbol = idx["symbol"]
            price = idx["price"]
            if idx.get("is_vix"):
                d = idx.get("one_day_pct")
                change = f"  1D: {d:+.2f}%" if d is not None else ""
                msg1 += f"<b>{html.escape(name)} ({symbol})</b> {price:.2f}{change}\n\n"
            else:
                d = idx.get("one_day_pct")
                w = idx.get("one_week_pct")
                m = idx.get("one_month_pct")
                six = idx.get("one_6m_pct")
                one_yr = idx.get("one_year_pct")
                three_yr = idx.get("three_year_pct")
                d_str = f"1D: {d:+.2f}%" if d is not None else "1D: —"
                w_str = f"1W: {w:+.2f}%" if w is not None else "1W: —"
                m_str = f"1M: {m:+.2f}%" if m is not None else "1M: —"
                six_str = f"6M: {six:+.2f}%" if six is not None else "6M: —"
                one_yr_str = f"1Y: {one_yr:+.2f}%" if one_yr is not None else "1Y: —"
                three_yr_str = f"3Y: {three_yr:+.2f}%" if three_yr is not None else "3Y: —"
                msg1 += f"<b>{html.escape(name)} ({symbol})</b> {price:.2f}\n"
                msg1 += f"  {d_str} | {w_str} | {m_str} | {six_str} | {one_yr_str} | {three_yr_str}\n\n"
        msg1 += "\n"

    if stock_sectors:
        msg1 += "<b>📈 Stocks</b>\n"
        for sector in stock_sectors:
            stocks = by_sector[sector]
            stocks = sorted(stocks, key=lambda x: x.get("company_name", x["symbol"]).upper())
            msg1 += f"  <b>▸ {sector}</b>\n"
            for stock in stocks:
                symbol = stock["symbol"]
                company_name = stock.get("company_name", symbol)
                price = stock["price"]
                vol = stock.get("volume") or 0
                vol_shares = vol / 1_000_000
                dollar_vol = (vol * price) / 1_000_000
                def pct_str(label: str, pct: Optional[float], passes: bool) -> str:
                    if pct is None:
                        return f"{label}: —"
                    s = f"{label}: {pct:+.2f}%"
                    return f"<b>{s}</b>" if passes else s
                d_str = pct_str("1D", stock["one_day_pct"], stock["passes_day"])
                w_str = pct_str("1W", stock["one_week_pct"], stock["passes_week"])
                m_val = stock.get("one_month_pct")
                m_pass = stock.get("passes_month", False)
                m_str = pct_str("1M", m_val, m_pass)
                six_val = stock.get("one_6m_pct")
                one_yr_val = stock.get("one_year_pct")
                three_yr_val = stock.get("three_year_pct")
                six_str = f"6M: {six_val:+.2f}%" if six_val is not None else "6M: —"
                one_yr_str = f"1Y: {one_yr_val:+.2f}%" if one_yr_val is not None else "1Y: —"
                three_yr_str = f"3Y: {three_yr_val:+.2f}%" if three_yr_val is not None else "3Y: —"
                change_str = f"{d_str} | {w_str} | {m_str} | {six_str} | {one_yr_str} | {three_yr_str}"
                msg1 += f"<b>{html.escape(company_name)} ({symbol})</b> ${price:.2f}\n"
                msg1 += f"  {change_str}\n"
                if vol > 0:
                    msg1 += f"  Vol: {vol_shares:.2f}M (${dollar_vol:.1f}M)\n\n"
                else:
                    msg1 += "\n"
            msg1 += "\n"

    # ---------- Message 2: Crypto, Commodities, Forex, ETFs ----------
    msg2 = ""
    msg2 = _append_section_block(msg2, by_sector, "Crypto", SECTION_EMOJI["Crypto"], is_forex=False, is_commodity=False)
    msg2 = _append_section_block(msg2, by_sector, "Commodities", SECTION_EMOJI["Commodities"], is_forex=False, is_commodity=True)
    msg2 = _append_section_block(msg2, by_sector, "Forex", SECTION_EMOJI["Forex"], is_forex=True, is_commodity=False)

    etf_order = etf_asset_class_order or ["Equity", "Fixed Income", "Commodities", "Currency", "Asset Location", "Alternatives"]
    etf_results = by_sector.get("ETFs", [])
    if etf_results:
        by_asset_class = {}
        for r in etf_results:
            ac = r.get("asset_class", "Other")
            by_asset_class.setdefault(ac, []).append(r)
        msg2 += "<b>⚖️ ETFs</b>\n"
        for ac in etf_order:
            if ac not in by_asset_class:
                continue
            items = sorted(by_asset_class[ac], key=lambda x: x.get("company_name", x["symbol"]).upper())
            msg2 += f"  <b>▸ {ac}</b>\n"
            for stock in items:
                symbol = stock["symbol"]
                company_name = stock.get("company_name", symbol)
                price = stock["price"]
                vol = stock.get("volume") or 0
                vol_shares = vol / 1_000_000
                dollar_vol = (vol * price) / 1_000_000
                def pct_str(label: str, pct: Optional[float], passes: bool) -> str:
                    if pct is None:
                        return f"{label}: —"
                    s = f"{label}: {pct:+.2f}%"
                    return f"<b>{s}</b>" if passes else s
                d_str = pct_str("1D", stock["one_day_pct"], stock["passes_day"])
                w_str = pct_str("1W", stock["one_week_pct"], stock["passes_week"])
                m_val = stock.get("one_month_pct")
                m_pass = stock.get("passes_month", False)
                m_str = pct_str("1M", m_val, m_pass)
                six_val = stock.get("one_6m_pct")
                one_yr_val = stock.get("one_year_pct")
                three_yr_val = stock.get("three_year_pct")
                six_str = f"6M: {six_val:+.2f}%" if six_val is not None else "6M: —"
                one_yr_str = f"1Y: {one_yr_val:+.2f}%" if one_yr_val is not None else "1Y: —"
                three_yr_str = f"3Y: {three_yr_val:+.2f}%" if three_yr_val is not None else "3Y: —"
                change_str = f"{d_str} | {w_str} | {m_str} | {six_str} | {one_yr_str} | {three_yr_str}"
                msg2 += f"<b>{html.escape(company_name)} ({symbol})</b> ${price:.2f}\n"
                msg2 += f"  {change_str}\n"
                msg2 += f"  Vol: {vol_shares:.2f}M (${dollar_vol:.1f}M)\n\n"
            msg2 += "\n"
        for ac in sorted(by_asset_class.keys()):
            if ac in etf_order:
                continue
            items = sorted(by_asset_class[ac], key=lambda x: x.get("company_name", x["symbol"]).upper())
            msg2 += f"  <b>▸ {ac}</b>\n"
            for stock in items:
                symbol = stock["symbol"]
                company_name = stock.get("company_name", symbol)
                price = stock["price"]
                vol = stock.get("volume") or 0
                vol_shares = vol / 1_000_000
                dollar_vol = (vol * price) / 1_000_000
                def pct_str(label: str, pct: Optional[float], passes: bool) -> str:
                    if pct is None:
                        return f"{label}: —"
                    s = f"{label}: {pct:+.2f}%"
                    return f"<b>{s}</b>" if passes else s
                d_str = pct_str("1D", stock["one_day_pct"], stock["passes_day"])
                w_str = pct_str("1W", stock["one_week_pct"], stock["passes_week"])
                m_val = stock.get("one_month_pct")
                m_pass = stock.get("passes_month", False)
                m_str = pct_str("1M", m_val, m_pass)
                six_val = stock.get("one_6m_pct")
                one_yr_val = stock.get("one_year_pct")
                three_yr_val = stock.get("three_year_pct")
                six_str = f"6M: {six_val:+.2f}%" if six_val is not None else "6M: —"
                one_yr_str = f"1Y: {one_yr_val:+.2f}%" if one_yr_val is not None else "1Y: —"
                three_yr_str = f"3Y: {three_yr_val:+.2f}%" if three_yr_val is not None else "3Y: —"
                change_str = f"{d_str} | {w_str} | {m_str} | {six_str} | {one_yr_str} | {three_yr_str}"
                msg2 += f"<b>{html.escape(company_name)} ({symbol})</b> ${price:.2f}\n"
                msg2 += f"  {change_str}\n"
                msg2 += f"  Vol: {vol_shares:.2f}M (${dollar_vol:.1f}M)\n\n"
            msg2 += "\n"

    if msg2.strip():
        msg2 = "📊 <b>MarketScout Scan (2/2)</b>\n\n" + msg2
    return (msg1.strip(), msg2.strip())


def main() -> None:
    """Main screener execution."""
    # Load environment variables
    load_dotenv()
    
    # Load configuration
    config = load_config("config.yaml")
    
    # Check if dry-run mode
    dry_run = config.get("dry_run", False)
    
    # When set (e.g. by GitHub Actions), send only stocks — for testing scheduled delivery
    stocks_only = (os.getenv("MARKETSCOUT_STOCKS_ONLY") or "").strip().lower() in ("1", "true", "yes")
    if stocks_only:
        print("MARKETSCOUT_STOCKS_ONLY: sending stocks only (no indices, crypto, commodities, forex, ETFs).")
    
    if not stocks_only:
        print("Fetching indices...")
        indices_data = get_indices_snapshot()
    else:
        indices_data = []
    
    print("Starting MarketScout screener...")
    results = run_screener(config)
    
    if stocks_only:
        crypto_results = []
        forex_results = []
        commodity_results = []
        etf_results = []
    else:
        print("Scanning Crypto...")
        crypto_results = run_crypto_screener(config)
        print("Scanning Forex...")
        forex_results = run_forex_screener(config)
        print("Scanning Commodities...")
        commodity_results = run_commodity_screener(config)
        print("Scanning ETFs...")
        etf_results = run_etf_screener(config)
    
    all_results = results + crypto_results + forex_results + commodity_results + etf_results
    crypto_count = len(crypto_results)
    forex_count = len(forex_results)
    commodity_count = len(commodity_results)
    etf_count = len(etf_results)
    
    # Format into two messages: (1) Indices, Stocks, Crypto; (2) Commodities, Forex, ETFs
    message1, message2 = format_stock_message(
        all_results,
        crypto_count=crypto_count,
        forex_count=forex_count,
        commodity_count=commodity_count,
        etf_count=etf_count,
        indices_data=indices_data,
        etf_asset_class_order=config.get("etf_asset_class_order"),
    )
    
    if dry_run:
        print("\n=== DRY RUN MODE ===")
        Path("sample_report_1.txt").write_text(message1, encoding="utf-8")
        print(f"Message 1 written to sample_report_1.txt ({len(message1)} chars)")
        if message2:
            Path("sample_report_2.txt").write_text(message2, encoding="utf-8")
            print(f"Message 2 written to sample_report_2.txt ({len(message2)} chars)")
        print("(Telegram notification skipped)")
    else:
        # Send Telegram notification (two messages)
        token = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
        chat_id = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()
        
        if not token or not chat_id:
            raise RuntimeError(
                "Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID in environment/.env"
            )
        # Telegram chat_id: numeric, or - for groups (e.g. -1001234567890)
        if not chat_id.lstrip("-").isdigit():
            print("Warning: TELEGRAM_CHAT_ID should be numeric (e.g. 123456789 or -1001234567890 for groups).", flush=True)
        
        send_telegram_message(message1, token, chat_id)
        if message2:
            send_telegram_message(message2, token, chat_id)
        print(f"\nTelegram notification sent (2 messages): {len(results)} stock(s), {etf_count} ETF(s), {crypto_count} crypto, {forex_count} forex, {commodity_count} commodities")
        
        # Generate and send charts as media group (stocks + crypto that passed)
        if all_results:
            print("Generating charts...")
            charts = generate_charts_for_results(all_results, config)
            
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
