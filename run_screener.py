def send_telegram_message(chat_id, stocks, crypto, commodities, forex, etfs):
    stock_message = "Stocks and Indices:\n" + ", ".join([f"{stock['name']} ({stock['symbol']})" for stock in stocks])
    telegram_send_message(chat_id, stock_message)  # Send stocks and indices in the first message

    # Prepare the second message for crypto, commodities, forex, and ETFs
    second_message_elements = []
    if crypto:
        second_message_elements.append("Cryptocurrency:\n" + ", ".join([f"{c['name']} ({c['symbol']})" for c in crypto]))
    if commodities:
        second_message_elements.append("Commodities:\n" + ", ".join([f"{c['name']} ({c['symbol']})" for c in commodities]))
    if forex:
        second_message_elements.append("Forex:\n" + ", ".join([f"{f['name']} ({f['symbol']})" for f in forex]))
    if etfs:
        second_message_elements.append("ETFs:\n" + ", ".join([f"{e['name']} ({e['symbol']})" for e in etfs]))
    second_message = '\n'.join(second_message_elements)  # Create the second message

    telegram_send_message(chat_id, second_message)  # Send the second message


def format_stock_message(stock):
    return f"{stock['name']} ({stock['symbol']}) - Price: {stock['price']}, Change: {stock['change']}%"