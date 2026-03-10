"""MarketScout screener — config and symbol loading."""
from typing import Dict, List
import yaml
import requests


def load_config(config_path: str = "config.yaml") -> Dict:
    """Load configuration from YAML file."""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def _fetch_nasdaq_listed() -> List[str]:
    """Fetch NASDAQ common stocks from NASDAQ Trader."""
    url = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        symbols = []
        for line in r.text.strip().split("\n")[1:]:
            parts = line.split("|")
            if len(parts) < 8:
                continue
            symbol, test_issue, etf, next_shares = parts[0].strip(), parts[3].strip(), parts[6].strip(), parts[7].strip()
            if etf != "N" or test_issue != "N" or next_shares != "N" or not symbol or "$" in symbol:
                continue
            symbols.append(symbol)
        return list(dict.fromkeys(symbols))
    except Exception as e:
        print(f"Error fetching nasdaqlisted.txt: {e}")
        return []


def _fetch_other_listed_nyse() -> List[str]:
    """Fetch NYSE common stocks from NASDAQ Trader."""
    url = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        symbols = []
        for line in r.text.strip().split("\n")[1:]:
            parts = line.split("|")
            if len(parts) < 8:
                continue
            act_symbol, exchange, etf, test_issue = parts[0].strip(), parts[2].strip(), parts[4].strip(), parts[6].strip()
            nasdaq_symbol = (parts[7] or "").strip()
            if exchange != "N" or etf != "N" or test_issue != "N" or "$" in act_symbol:
                continue
            symbol = nasdaq_symbol or act_symbol
            if symbol:
                symbols.append(symbol)
        return list(dict.fromkeys(symbols))
    except Exception as e:
        print(f"Error fetching otherlisted.txt: {e}")
        return []


def _fallback_symbols(exchange: str) -> List[str]:
    """Fallback when fetch fails."""
    base = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "JPM", "V", "WMT"]
    if exchange == "NYSE":
        return base + ["KO", "XOM", "BAC", "HD", "DIS"]
    return base + ["COST", "NFLX", "AMD", "INTC"]


def get_exchange_symbols(exchange: str) -> List[str]:
    """Get list of common stock symbols for an exchange."""
    if exchange == "NASDAQ":
        symbols = _fetch_nasdaq_listed()
    elif exchange == "NYSE":
        symbols = _fetch_other_listed_nyse()
    else:
        symbols = []
    if symbols:
        print(f"  Loaded {len(symbols)} symbols for {exchange}")
        return symbols
    return _fallback_symbols(exchange)
