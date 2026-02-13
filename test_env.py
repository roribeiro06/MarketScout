import os

from dotenv import load_dotenv


def main() -> None:
    # Load variables from .env in the project root
    load_dotenv()

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    print("TELEGRAM_BOT_TOKEN:", token)
    print("TELEGRAM_CHAT_ID:", chat_id)


if __name__ == "__main__":
    main()

