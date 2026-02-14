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


def get_exchange_symbols(exchange: str) -> List[str]:
    """
    Get list of symbols for an exchange using yfinance.
    Fetches tickers from major indices to get comprehensive coverage.
    """
    import yfinance as yf
    
    all_symbols = set()
    
    try:
        # Get S&P 500 tickers (covers many NYSE stocks)
        if exchange == "NYSE":
            try:
                sp500 = yf.Ticker("^GSPC")
                # Get components from Wikipedia or use a known list
                # For now, we'll use a comprehensive approach
                import requests
                from bs4 import BeautifulSoup
                
                # Fetch S&P 500 list from Wikipedia
                url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
                response = requests.get(url, timeout=10)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, 'html.parser')
                    table = soup.find('table', {'id': 'constituents'})
                    if table:
                        for row in table.find_all('tr')[1:]:  # Skip header
                            cells = row.find_all('td')
                            if cells:
                                symbol = cells[0].text.strip()
                                all_symbols.add(symbol)
            except Exception as e:
                print(f"Error fetching S&P 500 list: {e}")
        
        # Get NASDAQ 100 tickers
        if exchange == "NASDAQ":
            try:
                import requests
                from bs4 import BeautifulSoup
                
                # Fetch NASDAQ 100 list
                url = "https://en.wikipedia.org/wiki/NASDAQ-100"
                response = requests.get(url, timeout=10)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, 'html.parser')
                    # Find the table with NASDAQ 100 components
                    tables = soup.find_all('table', {'class': 'wikitable'})
                    for table in tables:
                        for row in table.find_all('tr')[1:]:  # Skip header
                            cells = row.find_all('td')
                            if cells and len(cells) > 0:
                                symbol_cell = cells[0].find('a')
                                if symbol_cell:
                                    symbol = symbol_cell.text.strip()
                                    all_symbols.add(symbol)
            except Exception as e:
                print(f"Error fetching NASDAQ 100 list: {e}")
        
        # Fallback: Use comprehensive ticker lists if web scraping fails
        if not all_symbols:
            print(f"Using fallback ticker list for {exchange}...")
            # Expanded list covering more stocks
            if exchange == "NYSE":
                return [
                    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "JPM",
                    "V", "JNJ", "WMT", "PG", "MA", "UNH", "HD", "DIS", "BAC", "XOM",
                    "CVX", "ABBV", "PFE", "KO", "AVGO", "COST", "MRK", "PEP", "TMO",
                    "CSCO", "ABT", "ACN", "NFLX", "ADBE", "CMCSA", "NKE", "TXN",
                    "HOOD", "COIN", "RIVN", "NBIS", "CRWV", "TOST", "PLTR", "SOFI",
                    "LCID", "RBLX", "SNOW", "DDOG", "NET", "ZM", "DOCN", "UPST",
                    "AFRM", "OPEN", "WISH", "CLOV", "SPCE", "FUBO", "NIO", "XPEV",
                    "LI", "BABA", "JD", "PDD", "BILI", "TME", "VIPS", "WB"
                ]
            elif exchange == "NASDAQ":
                return [
                    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "AVGO",
                    "COST", "NFLX", "ADBE", "CMCSA", "INTC", "AMD", "QCOM", "AMGN",
                    "ISRG", "BKNG", "REGN", "VRTX", "ADI", "SNPS", "CDNS", "KLAC",
                    "MCHP", "CTSH", "FTNT", "PAYX", "FAST", "CTAS", "WBD", "LRCX",
                    "HOOD", "COIN", "RIVN", "NBIS", "CRWV", "TOST", "PLTR", "SOFI",
                    "LCID", "RBLX", "SNOW", "DDOG", "NET", "ZM", "DOCN", "UPST",
                    "AFRM", "OPEN", "WISH", "CLOV", "SPCE", "FUBO", "NIO", "XPEV"
                ]
        
        return list(all_symbols)
        
    except Exception as e:
        print(f"Error getting {exchange} symbols: {e}")
        # Return expanded fallback list
        if exchange == "NYSE":
            return [
                "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "JPM",
                "V", "JNJ", "WMT", "PG", "MA", "UNH", "HD", "DIS", "BAC", "XOM",
                "CVX", "ABBV", "PFE", "KO", "AVGO", "COST", "MRK", "PEP", "TMO",
                "CSCO", "ABT", "ACN", "NFLX", "ADBE", "CMCSA", "NKE", "TXN",
                "HOOD", "COIN", "RIVN", "NBIS", "CRWV", "TOST"
            ]
        elif exchange == "NASDAQ":
            return [
                "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "AVGO",
                "COST", "NFLX", "ADBE", "CMCSA", "INTC", "AMD", "QCOM", "AMGN",
                "ISRG", "BKNG", "REGN", "VRTX", "ADI", "SNPS", "CDNS", "KLAC",
                "MCHP", "CTSH", "FTNT", "PAYX", "FAST", "CTAS", "WBD", "LRCX",
                "HOOD", "COIN", "RIVN", "NBIS", "CRWV", "TOST"
            ]
        return []


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
    
    # Fetch data (6mo so we have 6-month % for reference)
    data = fetch_stock_data(symbol, period="6mo")
    if data is None or len(data) < 5:  # Reduced minimum days requirement
        return None
    
    # Calculate changes (use available days, not fixed)
    one_day_change = calculate_percent_change(data, 1)
    one_week_change = calculate_percent_change(data, min(5, len(data) - 1))
    one_month_change = calculate_percent_change(data, min(20, len(data) - 1))
    one_6m_change = calculate_percent_change(data, min(126, len(data) - 1))
    
    # Allow None for month if we don't have enough data, but require day and week
    if one_day_change is None or one_week_change is None:
        return None
    
    # Get current volume and price
    current_volume = data["Volume"].iloc[-1]
    current_price = data["Close"].iloc[-1]
    
    # Check thresholds
    passes_day = abs(one_day_change) >= thresholds["one_day_pct_abs"]
    passes_week = abs(one_week_change) >= thresholds["one_week_pct_abs"]
    passes_month = one_month_change is not None and abs(one_month_change) >= thresholds["one_month_pct_abs"]
    passes_volume = current_volume >= thresholds["min_volume"]
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
    
    data = fetch_stock_data(symbol, period="6mo")
    if data is None or len(data) < 5:
        return None
    
    one_day_change = calculate_percent_change(data, 1)
    one_week_change = calculate_percent_change(data, min(5, len(data) - 1))
    one_month_change = calculate_percent_change(data, min(20, len(data) - 1))
    one_6m_change = calculate_percent_change(data, min(126, len(data) - 1))
    
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
    
    data = fetch_stock_data(symbol, period="6mo")
    if data is None or len(data) < 5:
        return None
    
    one_day_change = calculate_percent_change(data, 1)
    one_week_change = calculate_percent_change(data, min(5, len(data) - 1))
    one_month_change = calculate_percent_change(data, min(20, len(data) - 1))
    one_6m_change = calculate_percent_change(data, min(126, len(data) - 1))
    
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
    
    data = fetch_stock_data(symbol, period="6mo")
    if data is None or len(data) < 5:
        return None
    
    one_day_change = calculate_percent_change(data, 1)
    one_week_change = calculate_percent_change(data, min(5, len(data) - 1))
    one_month_change = calculate_percent_change(data, min(20, len(data) - 1))
    one_6m_change = calculate_percent_change(data, min(126, len(data) - 1))
    
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
