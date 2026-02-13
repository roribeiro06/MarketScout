import os
from datetime import datetime

import requests
from dotenv import load_dotenv


def send_telegram_message(text: str) -> None:
    """Send a plain-text message to the configured Telegram chat."""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        raise RuntimeError(
            "Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID in environment/.env"
        )

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
    }

    response = requests.post(url, json=payload, timeout=10)
    response.raise_for_status()


def main() -> None:
    # Load .env from project root
    load_dotenv()

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message = f"MarketScout test notification at {now_str}"

    send_telegram_message(message)
    print("Telegram message sent:", message)


if __name__ == "__main__":
    main()

