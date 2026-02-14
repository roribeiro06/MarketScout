"""Chart generation for stock screening results."""
import os
from pathlib import Path
from typing import Optional

import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.dates import DateFormatter
import pandas as pd
import yfinance as yf


def generate_stock_chart(symbol: str, company_name: str, data: pd.DataFrame, 
                         charts_dir: str = "charts", period: str = "1mo") -> Optional[str]:
    """
    Generate a price chart for a stock and save it.
    Returns the file path if successful, None otherwise.
    """
    try:
        # Create charts directory if it doesn't exist
        Path(charts_dir).mkdir(parents=True, exist_ok=True)
        
        # Create figure with subplots
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8), height_ratios=[3, 1])
        fig.suptitle(f'{company_name} ({symbol})', fontsize=14, fontweight='bold')
        
        # Plot price chart
        dates = data.index
        close_prices = data['Close']
        volume = data['Volume']
        
        # Color based on price movement
        color = 'green' if close_prices.iloc[-1] >= close_prices.iloc[0] else 'red'
        
        ax1.plot(dates, close_prices, linewidth=2, color=color, label='Close Price')
        ax1.fill_between(dates, close_prices, alpha=0.3, color=color)
        ax1.set_ylabel('Price ($)', fontsize=10)
        ax1.grid(True, alpha=0.3)
        ax1.legend(loc='upper left')
        
        # Format x-axis dates for price chart
        ax1.xaxis.set_major_formatter(DateFormatter('%m/%d'))
        ax1.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, len(dates) // 10)))
        # Ensure the top subplot shows x tick labels too
        ax1.tick_params(axis="x", labelbottom=True)
        ax1.set_xlabel("Date", fontsize=10)
        
        # Add current price annotation
        current_price = close_prices.iloc[-1]
        ax1.annotate(f'${current_price:.2f}', 
                    xy=(dates[-1], current_price),
                    xytext=(10, 10), textcoords='offset points',
                    bbox=dict(boxstyle='round,pad=0.5', facecolor='yellow', alpha=0.7),
                    arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0'))
        
        # Plot volume chart
        ax2.bar(dates, volume / 1_000_000, color='blue', alpha=0.6, width=0.8)
        ax2.set_ylabel('Volume (M)', fontsize=10)
        ax2.set_xlabel('Date', fontsize=10)
        ax2.grid(True, alpha=0.3)
        
        # Format x-axis dates for volume chart
        ax2.xaxis.set_major_formatter(DateFormatter('%m/%d'))
        ax2.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, len(dates) // 10)))
        
        # Rotate date labels for better readability
        fig.autofmt_xdate(rotation=45)
        
        plt.tight_layout()
        
        # Save chart
        chart_filename = f"{symbol}_chart.png"
        chart_path = os.path.join(charts_dir, chart_filename)
        plt.savefig(chart_path, dpi=100, bbox_inches='tight')
        plt.close(fig)
        
        return chart_path
        
    except Exception as e:
        print(f"Error generating chart for {symbol}: {e}")
        return None


def generate_charts_for_results(results: list, config: dict) -> list:
    """
    Generate charts for all stocks in results.
    Returns list of chart file paths.
    """
    # Handle both old and new config formats
    if "paths" in config and isinstance(config["paths"], dict):
        charts_dir = config["paths"].get("charts_dir", "charts")
    else:
        charts_dir = config.get("charts_dir", "charts")
    
    if not charts_dir:
        charts_dir = "charts"
    
    chart_paths = []
    
    for stock in results:
        symbol = stock["symbol"]
        company_name = stock.get("company_name", symbol)
        
        # Fetch data for chart (1-month period)
        try:
            ticker = yf.Ticker(symbol)
            data = ticker.history(period="1mo")  # Ensure 1-month period
            
            if data is not None and not data.empty:
                chart_path = generate_stock_chart(symbol, company_name, data, charts_dir, period="1mo")
                if chart_path:
                    chart_paths.append({
                        "symbol": symbol,
                        "company_name": company_name,
                        "chart_path": chart_path
                    })
        except Exception as e:
            print(f"Error fetching data for chart {symbol}: {e}")
    
    return chart_paths
