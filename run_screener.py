"""MarketScout — main entry point. Add your screener criteria here."""
import os
from dotenv import load_dotenv

load_dotenv()

# Telegram: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID (from .env or GitHub Secrets)
token = os.getenv("TELEGRAM_BOT_TOKEN")
chat_id = os.getenv("TELEGRAM_CHAT_ID")

if __name__ == "__main__":
    print("MarketScout ready.")
    if token and chat_id:
        print("Telegram configured.")
    else:
        print("Telegram not configured (set TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID).")
