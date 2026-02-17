"""Stock screener implementation for MarketScout."""
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import yfinance as yf
import yaml


def load_config(config_path: str = "config.yaml") -> Dict:
    """Load configuration from YAML file."""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def _fetch_nasdaq_listed() -> List[str]:
    """Fetch all NASDAQ-listed common stocks from NASDAQ Trader (exclude ETFs, test issues)."""
    import requests
    url = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        lines = r.text.strip().split("\n")
        symbols = []
        for line in lines[1:]:  # skip header
            parts = line.split("|")
            if len(parts) < 8:
                continue
            symbol, _, _, test_issue, _, _, etf, next_shares = parts[0].strip(), parts[1], parts[2], parts[3].strip(), parts[4], parts[5], parts[6].strip(), parts[7].strip()
            if etf != "N" or test_issue != "N" or next_shares != "N":
                continue
            if not symbol or "$" in symbol:
                continue
            symbols.append(symbol)
        return list(dict.fromkeys(symbols))
    except Exception as e:
        print(f"Error fetching nasdaqlisted.txt: {e}")
        return []


def _fetch_other_listed_nyse() -> List[str]:
    """Fetch all NYSE common stocks from NASDAQ Trader otherlisted.txt (Exchange=N, exclude ETFs, test, preferred)."""
    import requests
    url = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        lines = r.text.strip().split("\n")
        symbols = []
        for line in lines[1:]:
            parts = line.split("|")
            if len(parts) < 8:
                continue
            act_symbol, _, exchange, _, etf, _, test_issue, nasdaq_symbol = parts[0].strip(), parts[1], parts[2].strip(), parts[3], parts[4].strip(), parts[5], parts[6].strip(), (parts[7] or "").strip()
            if exchange != "N":
                continue
            if etf != "N" or test_issue != "N":
                continue
            if "$" in act_symbol:
                continue
            symbol = nasdaq_symbol or act_symbol
            if not symbol:
                continue
            symbols.append(symbol)
        return list(dict.fromkeys(symbols))
    except Exception as e:
        print(f"Error fetching otherlisted.txt: {e}")
        return []


def _fallback_exchange_symbols(exchange: str) -> List[str]:
    """Fallback ticker list when NASDAQ Trader fetch fails."""
    if exchange == "NYSE":
        return [
            "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "JPM",
            "V", "JNJ", "WMT", "PG", "MA", "UNH", "HD", "DIS", "BAC", "XOM",
            "CVX", "ABBV", "PFE", "KO", "AVGO", "COST", "MRK", "PEP", "TMO",
            "CSCO", "ABT", "ACN", "NFLX", "ADBE", "CMCSA", "NKE", "TXN",
            "HOOD", "COIN", "RIVN", "NBIS", "CRWV", "TOST", "PLTR", "SOFI",
            "LCID", "RBLX", "SNOW", "DDOG", "NET", "ZM", "DOCN", "UPST",
            "AFRM", "OPEN", "WISH", "CLOV", "SPCE", "FUBO", "NIO", "XPEV",
            "LI", "BABA", "JD", "PDD", "BILI", "TME", "VIPS", "WB", "DKNG"
        ]
    elif exchange == "NASDAQ":
        return [
            "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "AVGO",
            "COST", "NFLX", "ADBE", "CMCSA", "INTC", "AMD", "QCOM", "AMGN",
            "ISRG", "BKNG", "REGN", "VRTX", "ADI", "SNPS", "CDNS", "KLAC",
            "MCHP", "CTSH", "FTNT", "PAYX", "FAST", "CTAS", "WBD", "LRCX",
            "HOOD", "COIN", "RIVN", "NBIS", "CRWV", "TOST", "PLTR", "SOFI",
            "LCID", "RBLX", "SNOW", "DDOG", "NET", "ZM", "DOCN", "UPST",
            "AFRM", "OPEN", "WISH", "CLOV", "SPCE", "FUBO", "NIO", "XPEV",
            "DKNG"
        ]
    return []


def get_exchange_symbols(exchange: str) -> List[str]:
    """
    Get list of all common stock symbols for an exchange.
    Uses NASDAQ Trader symbol directory (nasdaqlisted.txt for NASDAQ, otherlisted.txt for NYSE).
    Excludes ETFs, test issues, and preferred/warrants. Falls back to a short list if fetch fails.
    """
    try:
        if exchange == "NASDAQ":
            symbols = _fetch_nasdaq_listed()
        elif exchange == "NYSE":
            symbols = _fetch_other_listed_nyse()
        else:
            symbols = []
        if symbols:
            print(f"  Loaded {len(symbols)} symbols for {exchange}")
            return symbols
        print(f"Using fallback ticker list for {exchange}...")
        return _fallback_exchange_symbols(exchange)
    except Exception as e:
        print(f"Error getting {exchange} symbols: {e}")
        return _fallback_exchange_symbols(exchange)


def fetch_stock_data(symbol: str, period: str = "1mo") -> Optional[pd.DataFrame]:
    """Fetch historical stock data using yfinance."""
    try:
        ticker = yf.Ticker(symbol)
        data = ticker.history(period=period)
        if data.empty:
            return None
        return data
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None


def calculate_percent_change(data: pd.DataFrame, days: int) -> Optional[float]:
    """Calculate percentage change over N days."""
    if len(data) < days + 1:
        return None
    
    current_price = data["Close"].iloc[-1]
    past_price = data["Close"].iloc[-(days + 1)]
    
    if past_price == 0:
        return None
    
    pct_change = ((current_price - past_price) / past_price) * 100
    return pct_change


def screen_stock(symbol: str, config: Dict) -> Optional[Dict]:
    """
    Screen a single stock against thresholds.
    Returns dict with stock info if it passes screening, None otherwise.
    """
    thresholds = config["thresholds"]
    
    # Fetch data (5y so we have 6M/1Y/3Y % for reference)
    data = fetch_stock_data(symbol, period="5y")
    if data is None or len(data) < 5:  # Reduced minimum days requirement
        return None
    
    # Calculate changes (use available days, not fixed). 126/252/756 ≈ 6M/1Y/3Y trading days
    one_day_change = calculate_percent_change(data, 1)
    one_week_change = calculate_percent_change(data, min(5, len(data) - 1))
    one_month_change = calculate_percent_change(data, min(20, len(data) - 1))
    one_6m_change = calculate_percent_change(data, min(126, len(data) - 1))
    one_year_change = calculate_percent_change(data, min(252, len(data) - 1))
    three_year_change = calculate_percent_change(data, min(756, len(data) - 1))
    
    # Allow None for month if we don't have enough data, but require day and week
    if one_day_change is None or one_week_change is None:
        return None
    
    # Get current volume and price
    current_volume = data["Volume"].iloc[-1]
    current_price = data["Close"].iloc[-1]
    dollar_volume = current_price * current_volume
    
    # Check thresholds (stocks: price*volume >= $1B, price >= $10)
    passes_day = abs(one_day_change) >= thresholds["one_day_pct_abs"]
    passes_week = abs(one_week_change) >= thresholds["one_week_pct_abs"]
    passes_month = one_month_change is not None and abs(one_month_change) >= thresholds["one_month_pct_abs"]
    passes_volume = dollar_volume >= thresholds.get("min_dollar_volume", 1_000_000_000)
    passes_price = current_price >= thresholds.get("min_price", 0)  # Default to 0 if not specified
    
    # Stock passes if it meets any threshold AND volume AND price requirements
    if passes_volume and passes_price and (passes_day or passes_week or passes_month):
        
        # Fetch company name and sector
        company_name = symbol  # Default to symbol if name fetch fails
        sector = "Other"
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            if "longName" in info:
                company_name = info["longName"]
            elif "shortName" in info:
                company_name = info["shortName"]
            if info.get("sector") and isinstance(info["sector"], str):
                sector = info["sector"].strip() or "Other"
        except Exception:
            pass  # Use symbol as fallback
        
        return {
            "symbol": symbol,
            "company_name": company_name,
            "sector": sector,
            "price": round(current_price, 2),
            "volume": int(current_volume),
            "one_day_pct": round(one_day_change, 2),
            "one_week_pct": round(one_week_change, 2),
            "one_month_pct": round(one_month_change, 2) if one_month_change is not None else None,
            "one_6m_pct": round(one_6m_change, 2) if one_6m_change is not None else None,
            "one_year_pct": round(one_year_change, 2) if one_year_change is not None else None,
            "three_year_pct": round(three_year_change, 2) if three_year_change is not None else None,
            "passes_day": passes_day,
            "passes_week": passes_week,
            "passes_month": passes_month,
            "data": data,  # Include data for chart generation
        }
    
    return None


def run_screener(config: Dict) -> List[Dict]:
    """Run the screener across all configured exchanges."""
    all_results = []
    seen_symbols = set()  # Track symbols we've already processed
    exchanges = config.get("exchanges", [])
    
    for exchange in exchanges:
        print(f"Scanning {exchange}...")
        symbols = get_exchange_symbols(exchange)
        
        for symbol in symbols:
            # Skip if we've already processed this symbol
            if symbol in seen_symbols:
                continue
            
            result = screen_stock(symbol, config)
            if result:
                all_results.append(result)
                seen_symbols.add(symbol)  # Mark as processed
                print(f"  [MATCH] {symbol}: {result['one_day_pct']:.2f}% (1D)")
    
    return all_results


# Crypto display names (fallback if not in config)
CRYPTO_NAMES = {"BTC-USD": "Bitcoin", "ETH-USD": "Ethereum", "SOL-USD": "Solana"}


def screen_crypto(symbol: str, name: str, config: Dict) -> Optional[Dict]:
    """
    Screen a single crypto against % thresholds only (no volume/price filter).
    Returns dict if it passes (1D +/-3%, 1W +/-5%, 1M +/-10%), None otherwise.
    """
    thresholds = config["thresholds"]
    
    data = fetch_stock_data(symbol, period="5y")
    if data is None or len(data) < 5:
        return None
    
    one_day_change = calculate_percent_change(data, 1)
    one_week_change = calculate_percent_change(data, min(5, len(data) - 1))
    one_month_change = calculate_percent_change(data, min(20, len(data) - 1))
    one_6m_change = calculate_percent_change(data, min(126, len(data) - 1))
    one_year_change = calculate_percent_change(data, min(252, len(data) - 1))
    three_year_change = calculate_percent_change(data, min(756, len(data) - 1))
    
    if one_day_change is None or one_week_change is None:
        return None
    
    current_price = data["Close"].iloc[-1]
    current_volume = int(data["Volume"].iloc[-1]) if "Volume" in data.columns else 0
    
    passes_day = abs(one_day_change) >= thresholds["one_day_pct_abs"]
    passes_week = abs(one_week_change) >= thresholds["one_week_pct_abs"]
    passes_month = one_month_change is not None and abs(one_month_change) >= thresholds["one_month_pct_abs"]
    
    if not (passes_day or passes_week or passes_month):
        return None
    
    return {
        "symbol": symbol,
        "company_name": name,
        "sector": "Crypto",
        "price": round(current_price, 2),
        "volume": current_volume,
        "one_day_pct": round(one_day_change, 2),
        "one_week_pct": round(one_week_change, 2),
        "one_month_pct": round(one_month_change, 2) if one_month_change is not None else None,
        "one_6m_pct": round(one_6m_change, 2) if one_6m_change is not None else None,
        "one_year_pct": round(one_year_change, 2) if one_year_change is not None else None,
        "three_year_pct": round(three_year_change, 2) if three_year_change is not None else None,
        "passes_day": passes_day,
        "passes_week": passes_week,
        "passes_month": passes_month,
        "data": data,
    }


def run_crypto_screener(config: Dict) -> List[Dict]:
    """Run the crypto screener for configured coins. Returns only those meeting criteria."""
    results = []
    crypto_list = config.get("crypto") or []
    
    for item in crypto_list:
        if isinstance(item, dict):
            symbol = item.get("symbol")
            name = item.get("name") or CRYPTO_NAMES.get(symbol, symbol)
        else:
            symbol = item
            name = CRYPTO_NAMES.get(symbol, symbol)
        if not symbol:
            continue
        r = screen_crypto(symbol, name, config)
        if r:
            results.append(r)
            print(f"  [CRYPTO] {symbol}: {r['one_day_pct']:.2f}% (1D)")
    
    return results


def screen_forex(symbol: str, name: str, config: Dict) -> Optional[Dict]:
    """
    Screen a single forex pair against forex thresholds: 1D +/-0.5%, 1W +/-1%, 1M +/-3%.
    Returns dict if it passes, None otherwise.
    """
    thresholds = config.get("forex_thresholds") or {
        "one_day_pct_abs": 0.5,
        "one_week_pct_abs": 1.0,
        "one_month_pct_abs": 3.0,
    }
    
    data = fetch_stock_data(symbol, period="5y")
    if data is None or len(data) < 5:
        return None
    
    one_day_change = calculate_percent_change(data, 1)
    one_week_change = calculate_percent_change(data, min(5, len(data) - 1))
    one_month_change = calculate_percent_change(data, min(20, len(data) - 1))
    one_6m_change = calculate_percent_change(data, min(126, len(data) - 1))
    one_year_change = calculate_percent_change(data, min(252, len(data) - 1))
    three_year_change = calculate_percent_change(data, min(756, len(data) - 1))
    
    if one_day_change is None or one_week_change is None:
        return None
    
    current_price = data["Close"].iloc[-1]
    current_volume = int(data["Volume"].iloc[-1]) if "Volume" in data.columns else 0
    
    passes_day = abs(one_day_change) >= thresholds["one_day_pct_abs"]
    passes_week = abs(one_week_change) >= thresholds["one_week_pct_abs"]
    passes_month = one_month_change is not None and abs(one_month_change) >= thresholds["one_month_pct_abs"]
    
    if not (passes_day or passes_week or passes_month):
        return None
    
    return {
        "symbol": symbol,
        "company_name": name,
        "sector": "Forex",
        "price": round(current_price, 4),  # forex rates need more decimals
        "volume": current_volume,
        "one_day_pct": round(one_day_change, 2),
        "one_week_pct": round(one_week_change, 2),
        "one_month_pct": round(one_month_change, 2) if one_month_change is not None else None,
        "one_6m_pct": round(one_6m_change, 2) if one_6m_change is not None else None,
        "one_year_pct": round(one_year_change, 2) if one_year_change is not None else None,
        "three_year_pct": round(three_year_change, 2) if three_year_change is not None else None,
        "passes_day": passes_day,
        "passes_week": passes_week,
        "passes_month": passes_month,
        "data": data,
    }


def run_forex_screener(config: Dict) -> List[Dict]:
    """Run the forex screener for configured pairs. Returns only those meeting criteria."""
    results = []
    forex_list = config.get("forex") or []
    
    for item in forex_list:
        if isinstance(item, dict):
            symbol = item.get("symbol")
            name = item.get("name") or symbol
        else:
            symbol = item
            name = symbol
        if not symbol:
            continue
        r = screen_forex(symbol, name, config)
        if r:
            results.append(r)
            print(f"  [FOREX] {symbol}: {r['one_day_pct']:.2f}% (1D)")
    
    return results


def screen_commodity(symbol: str, name: str, config: Dict) -> Optional[Dict]:
    """
    Screen a single commodity future: 1D +/-2%, 1W +/-5%, 1M +/-10%, min 100K contracts.
    Returns dict if it passes, None otherwise.
    """
    thresholds = config.get("commodity_thresholds") or {
        "one_day_pct_abs": 2.0,
        "one_week_pct_abs": 5.0,
        "one_month_pct_abs": 10.0,
        "min_volume_contracts": 100000,
    }
    
    data = fetch_stock_data(symbol, period="5y")
    if data is None or len(data) < 5:
        return None
    
    one_day_change = calculate_percent_change(data, 1)
    one_week_change = calculate_percent_change(data, min(5, len(data) - 1))
    one_month_change = calculate_percent_change(data, min(20, len(data) - 1))
    one_6m_change = calculate_percent_change(data, min(126, len(data) - 1))
    one_year_change = calculate_percent_change(data, min(252, len(data) - 1))
    three_year_change = calculate_percent_change(data, min(756, len(data) - 1))
    
    if one_day_change is None or one_week_change is None:
        return None
    
    current_price = data["Close"].iloc[-1]
    current_volume = int(data["Volume"].iloc[-1]) if "Volume" in data.columns else 0
    
    passes_day = abs(one_day_change) >= thresholds["one_day_pct_abs"]
    passes_week = abs(one_week_change) >= thresholds["one_week_pct_abs"]
    passes_month = one_month_change is not None and abs(one_month_change) >= thresholds["one_month_pct_abs"]
    passes_volume = current_volume >= thresholds.get("min_volume_contracts", 100000)
    
    if not (passes_volume and (passes_day or passes_week or passes_month)):
        return None
    
    return {
        "symbol": symbol,
        "company_name": name,
        "sector": "Commodities",
        "price": round(current_price, 2),
        "volume": current_volume,
        "one_day_pct": round(one_day_change, 2),
        "one_week_pct": round(one_week_change, 2),
        "one_month_pct": round(one_month_change, 2) if one_month_change is not None else None,
        "one_6m_pct": round(one_6m_change, 2) if one_6m_change is not None else None,
        "one_year_pct": round(one_year_change, 2) if one_year_change is not None else None,
        "three_year_pct": round(three_year_change, 2) if three_year_change is not None else None,
        "passes_day": passes_day,
        "passes_week": passes_week,
        "passes_month": passes_month,
        "data": data,
    }


def run_commodity_screener(config: Dict) -> List[Dict]:
    """Run the commodity futures screener. Returns only those meeting criteria."""
    results = []
    commodity_list = config.get("commodities") or []
    
    for item in commodity_list:
        if isinstance(item, dict):
            symbol = item.get("symbol")
            name = item.get("name") or symbol
        else:
            symbol = item
            name = symbol
        if not symbol:
            continue
        r = screen_commodity(symbol, name, config)
        if r:
            results.append(r)
            print(f"  [COMMODITY] {symbol}: {r['one_day_pct']:.2f}% (1D), vol {r['volume']:,}")
    
    return results


def screen_etf(symbol: str, name: str, asset_class: str, config: Dict) -> Optional[Dict]:
    """
    Screen a single ETF: 1D +/-3%, 1W +/-5%, 1M +/-10%, price*volume >= $1B.
    Returns dict if it passes, None otherwise.
    """
    thresholds = config.get("etf_thresholds") or {
        "one_day_pct_abs": 3.0,
        "one_week_pct_abs": 5.0,
        "one_month_pct_abs": 10.0,
        "min_dollar_volume": 1_000_000_000,
    }
    
    data = fetch_stock_data(symbol, period="5y")
    if data is None or len(data) < 5:
        return None
    
    one_day_change = calculate_percent_change(data, 1)
    one_week_change = calculate_percent_change(data, min(5, len(data) - 1))
    one_month_change = calculate_percent_change(data, min(20, len(data) - 1))
    one_6m_change = calculate_percent_change(data, min(126, len(data) - 1))
    one_year_change = calculate_percent_change(data, min(252, len(data) - 1))
    three_year_change = calculate_percent_change(data, min(756, len(data) - 1))
    
    if one_day_change is None or one_week_change is None:
        return None
    
    current_price = data["Close"].iloc[-1]
    current_volume = int(data["Volume"].iloc[-1]) if "Volume" in data.columns else 0
    dollar_volume = current_price * current_volume
    
    passes_day = abs(one_day_change) >= thresholds["one_day_pct_abs"]
    passes_week = abs(one_week_change) >= thresholds["one_week_pct_abs"]
    passes_month = one_month_change is not None and abs(one_month_change) >= thresholds["one_month_pct_abs"]
    passes_volume = dollar_volume >= thresholds.get("min_dollar_volume", 1_000_000_000)
    
    if not (passes_volume and (passes_day or passes_week or passes_month)):
        return None
    
    return {
        "symbol": symbol,
        "company_name": name,
        "sector": "ETFs",
        "asset_class": asset_class,
        "price": round(current_price, 2),
        "volume": current_volume,
        "one_day_pct": round(one_day_change, 2),
        "one_week_pct": round(one_week_change, 2),
        "one_month_pct": round(one_month_change, 2) if one_month_change is not None else None,
        "one_6m_pct": round(one_6m_change, 2) if one_6m_change is not None else None,
        "one_year_pct": round(one_year_change, 2) if one_year_change is not None else None,
        "three_year_pct": round(three_year_change, 2) if three_year_change is not None else None,
        "passes_day": passes_day,
        "passes_week": passes_week,
        "passes_month": passes_month,
        "data": data,
    }


def run_etf_screener(config: Dict) -> List[Dict]:
    """Run the ETF screener. Returns only those meeting criteria (1D/1W/1M % and vol >= 20M)."""
    results = []
    etf_list = config.get("etfs") or []
    order = config.get("etf_asset_class_order") or [
        "Equity", "Fixed Income", "Commodities", "Currency", "Asset Location", "Alternatives"
    ]
    
    for item in etf_list:
        if isinstance(item, dict):
            symbol = item.get("symbol")
            name = item.get("name") or symbol
            asset_class = item.get("asset_class", "Other")
        else:
            symbol = item
            name = symbol
            asset_class = "Other"
        if not symbol:
            continue
        r = screen_etf(symbol, name, asset_class, config)
        if r:
            results.append(r)
            print(f"  [ETF] {symbol}: {r['one_day_pct']:.2f}% (1D), vol {r['volume']/1e6:.1f}M")
    
    return results
