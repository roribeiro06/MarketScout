"""Main entry point for MarketScout stock screener."""
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import requests
import yaml
import yfinance as yf
from dotenv import load_dotenv

from screener.screener import load_config, run_screener, run_crypto_screener, run_forex_screener, run_commodity_screener
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
    """Fetch current level and 1D/1W/1M % changes for indices. VIX has 1D only."""
    out = []
    for symbol, name in INDICES:
        try:
            ticker = yf.Ticker(symbol)
            data = ticker.history(period="6mo")
            if data is None or len(data) < 2:
                continue
            price = round(float(data["Close"].iloc[-1]), 2)
            one_d = None
            one_w = None
            one_m = None
            one_6m = None
            if len(data) >= 2:
                p0 = data["Close"].iloc[-2]
                one_d = ((data["Close"].iloc[-1] - p0) / p0) * 100
            if len(data) >= 6:
                p5 = data["Close"].iloc[-6]
                one_w = ((data["Close"].iloc[-1] - p5) / p5) * 100
            if len(data) >= 21:
                p20 = data["Close"].iloc[-21]
                one_m = ((data["Close"].iloc[-1] - p20) / p20) * 100
            # 6-month: use ~126 trading days or first available
            if len(data) >= 2:
                n_6m = min(126, len(data) - 1)
                p_6m = data["Close"].iloc[-(n_6m + 1)]
                one_6m = ((data["Close"].iloc[-1] - p_6m) / p_6m) * 100
            out.append({
                "symbol": symbol,
                "name": name,
                "price": price,
                "one_day_pct": round(one_d, 2) if one_d is not None else None,
                "one_week_pct": round(one_w, 2) if one_w is not None else None,
                "one_month_pct": round(one_m, 2) if one_m is not None else None,
                "one_6m_pct": round(one_6m, 2) if one_6m is not None else None,
                "is_vix": symbol == "^VIX",
            })
        except Exception as e:
            print(f"  [INDICES] Error fetching {symbol}: {e}")
    return out


def send_telegram_message(text: str, token: str, chat_id: str) -> None:
    """Send a plain-text message to the configured Telegram chat."""
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
    }

    response = requests.post(url, json=payload, timeout=10)
    response.raise_for_status()


def send_telegram_media_group(photo_paths: list, token: str, chat_id: str) -> None:
    """Send multiple photos as a media group (single notification)."""
    if not photo_paths:
        return
    
    import json
    
    # Telegram allows max 10 items per media group
    # Process in batches of 10
    for batch_start in range(0, len(photo_paths), 10):
        batch_paths = photo_paths[batch_start:batch_start + 10]
        
        url = f"https://api.telegram.org/bot{token}/sendMediaGroup"
        
        # Prepare media array
        media = []
        files = {}
        
        for i, photo_path in enumerate(batch_paths):
            media.append({
                'type': 'photo',
                'media': f'attach://photo_{i}'
            })
            files[f'photo_{i}'] = open(photo_path, 'rb')
        
        data = {
            'chat_id': chat_id,
            'media': json.dumps(media)
        }
        
        try:
            response = requests.post(url, files=files, data=data, timeout=60)
            response.raise_for_status()
        finally:
            # Close all file handles
            for f in files.values():
                f.close()


def format_stock_message(
    results: list,
    crypto_count: int = 0,
    forex_count: int = 0,
    commodity_count: int = 0,
    indices_data: Optional[List[Dict]] = None,
) -> str:
    """Format screening results into a Telegram message, grouped by sector."""
    stock_count = len(results) - crypto_count - forex_count - commodity_count
    if not results and not indices_data:
        return "📊 <b>MarketScout Scan</b>\n\nNo stocks, crypto, forex, or commodities found matching criteria."
    
    # Group by sector
    by_sector = {}
    for stock in results:
        sector = stock.get("sector", "Other")
        by_sector.setdefault(sector, []).append(stock)
    
    # Separate stock sectors from Crypto/Forex/Commodities
    non_stock = {"Crypto", "Forex", "Commodities"}
    stock_sectors = sorted([s for s in by_sector if s not in non_stock], key=lambda s: (s == "Other", s.upper()))
    # Order for other sections: Crypto, Commodities, Forex
    other_section_order = ["Crypto", "Commodities", "Forex"]
    other_sectors = [s for s in other_section_order if s in by_sector]
    
    message = f"📊 <b>MarketScout Scan</b>\n"
    parts = []
    if stock_count:
        parts.append(f"{stock_count} stock(s)")
    if crypto_count:
        parts.append(f"{crypto_count} crypto")
    if forex_count:
        parts.append(f"{forex_count} forex")
    if commodity_count:
        parts.append(f"{commodity_count} commodities")
    if parts:
        message += f"Found {' + '.join(parts)} matching criteria:\n\n"
    elif indices_data:
        message += "Indices snapshot:\n\n"
    else:
        message += "\n"
    
    # Indices section first (earth emoji) – always before everything
    if indices_data:
        message += f"<b>🌍 Indices</b>\n"
        for idx in indices_data:
            name = idx["name"]
            symbol = idx["symbol"]
            price = idx["price"]
            if idx.get("is_vix"):
                # VIX: show level, 1D and 6M
                d = idx.get("one_day_pct")
                six = idx.get("one_6m_pct")
                change = f"  1D: {d:+.2f}%" if d is not None else ""
                if six is not None:
                    change += f" | 6M: {six:+.2f}%"
                message += f"<b>{name} ({symbol})</b> {price:.2f}{change}\n\n"
            else:
                d = idx.get("one_day_pct")
                w = idx.get("one_week_pct")
                m = idx.get("one_month_pct")
                six = idx.get("one_6m_pct")
                d_str = f"1D: {d:+.2f}%" if d is not None else "1D: —"
                w_str = f"1W: {w:+.2f}%" if w is not None else "1W: —"
                m_str = f"1M: {m:+.2f}%" if m is not None else "1M: —"
                six_str = f"6M: {six:+.2f}%" if six is not None else "6M: —"
                message += f"<b>{name} ({symbol})</b> {price:.2f}\n"
                message += f"  {d_str} | {w_str} | {m_str} | {six_str}\n\n"
        message += "\n"
    
    # Main section: Stocks (graph emoji) with subsections by sector
    if stock_sectors:
        message += f"<b>📈 Stocks</b>\n"
        for sector in stock_sectors:
            stocks = by_sector[sector]
            stocks = sorted(stocks, key=lambda x: x.get("company_name", x["symbol"]).upper())
            message += f"  <b>▸ {sector}</b>\n"
            for stock in stocks:
                symbol = stock["symbol"]
                company_name = stock.get("company_name", symbol)
                price = stock["price"]
                vol = stock.get("volume") or 0
                vol_shares = vol / 1_000_000
                dollar_vol = (vol * price) / 1_000_000
                def pct_str(label, pct, passes):
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
                six_str = f"6M: {six_val:+.2f}%" if six_val is not None else "6M: —"
                change_str = f"{d_str} | {w_str} | {m_str} | {six_str}"
                message += f"<b>{company_name} ({symbol})</b> ${price:.2f}\n"
                message += f"  {change_str}\n"
                if vol > 0:
                    message += f"  Vol: {vol_shares:.2f}M (${dollar_vol:.1f}M)\n\n"
                else:
                    message += "\n"
            message += "\n"
    
    # Single-section blocks: Crypto (coin), Commodities (wheat), Forex (dollar)
    SECTION_EMOJI = {"Crypto": "🪙", "Commodities": "🌾", "Forex": "💵"}  # 🌾 = wheat
    for sector in other_sectors:
        stocks = by_sector[sector]
        stocks = sorted(stocks, key=lambda x: x.get("company_name", x["symbol"]).upper())
        emoji = SECTION_EMOJI.get(sector, "")
        message += f"<b>{emoji} {sector}</b>\n"
        
        for stock in stocks:
            symbol = stock["symbol"]
            company_name = stock.get("company_name", symbol)
            price = stock["price"]
            vol = stock.get("volume") or 0
            vol_shares = vol / 1_000_000  # millions (shares or units)
            dollar_vol = (vol * price) / 1_000_000  # millions USD
            
            # 1D / 1W / 1M: show all, bold the one(s) that meet criteria; 6M for reference (no bold)
            def pct_str(label, pct, passes):
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
            six_str = f"6M: {six_val:+.2f}%" if six_val is not None else "6M: —"
            change_str = f"{d_str} | {w_str} | {m_str} | {six_str}"
            
            # Forex: show rate without $; stocks/crypto: show price with $
            if sector == "Forex":
                message += f"<b>{company_name} ({symbol})</b> {price:.4f}\n"
            else:
                message += f"<b>{company_name} ({symbol})</b> ${price:.2f}\n"
            message += f"  {change_str}\n"
            if sector == "Forex" and vol > 0:
                message += f"  Vol: {vol_shares:.2f}M\n\n"
            elif sector == "Commodities" and vol > 0:
                vol_k = vol / 1_000  # thousands of contracts
                contract_size = COMMODITY_CONTRACT_SIZE.get(symbol, 1)
                commodity_dollar_vol = (vol * price * contract_size) / 1_000_000  # millions USD
                message += f"  Vol: {vol_k:.1f}K contracts (${commodity_dollar_vol:.1f}M)\n\n"
            elif sector != "Forex" and sector != "Commodities" and vol > 0:
                message += f"  Vol: {vol_shares:.2f}M (${dollar_vol:.1f}M)\n\n"
            else:
                message += "\n"
        
        message += "\n"
    
    return message.strip()


def main() -> None:
    """Main screener execution."""
    # Load environment variables
    load_dotenv()
    
    # Load configuration
    config = load_config("config.yaml")
    
    # Check if dry-run mode
    dry_run = config.get("dry_run", False)
    
    # Indices snapshot (always first in message)
    print("Fetching indices...")
    indices_data = get_indices_snapshot()
    
    # Run stock screener
    print("Starting MarketScout screener...")
    results = run_screener(config)
    
    # Run crypto screener (Bitcoin, Ethereum)
    print("Scanning Crypto...")
    crypto_results = run_crypto_screener(config)
    
    # Run forex screener (1D +/-0.5%, 1W +/-1%, 1M +/-3%)
    print("Scanning Forex...")
    forex_results = run_forex_screener(config)
    
    # Run commodity futures screener (1D +/-2%, 1W +/-5%, 1M +/-10%, min 100K contracts)
    print("Scanning Commodities...")
    commodity_results = run_commodity_screener(config)
    
    all_results = results + crypto_results + forex_results + commodity_results
    crypto_count = len(crypto_results)
    forex_count = len(forex_results)
    commodity_count = len(commodity_results)
    
    # Format message (indices + stocks by sector + crypto + forex + commodities)
    message = format_stock_message(
        all_results,
        crypto_count=crypto_count,
        forex_count=forex_count,
        commodity_count=commodity_count,
        indices_data=indices_data,
    )
    
    if dry_run:
        print("\n=== DRY RUN MODE ===")
        print(message)
        print("\n(Telegram notification skipped)")
    else:
        # Send Telegram notification
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        
        if not token or not chat_id:
            raise RuntimeError(
                "Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID in environment/.env"
            )
        
        # Send text message first
        send_telegram_message(message, token, chat_id)
        print(f"\nTelegram notification sent: {len(results)} stock(s), {crypto_count} crypto, {forex_count} forex, {commodity_count} commodities")
        
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
    main()
