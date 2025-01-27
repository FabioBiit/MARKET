import requests

# Funzione per ottenere i prezzi da Binance
def get_binance_price(symbol):
    url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
    response = requests.get(url)
    if response.status_code == 200:
        return float(response.json()["price"])
    else:
        raise Exception(f"Errore API Binance: {response.text}")

# Funzione per ottenere i prezzi da Kraken
def get_kraken_price(symbol):
    url = "https://api.kraken.com/0/public/Ticker"
    params = {"pair": symbol}
    response = requests.get(url, params=params)
    if response.status_code == 200:
        result = response.json()["result"]
        key = list(result.keys())[0]  # Ottieni la coppia corretta
        return float(result[key]["a"][0])  # Prezzo di acquisto
    else:
        raise Exception(f"Errore API Kraken: {response.text}")

# Funzione per ottenere i prezzi da Coinbase
def get_coinbase_price(symbol):
    url = f"https://api.coinbase.com/v2/prices/{symbol}/spot"
    response = requests.get(url)
    if response.status_code == 200:
        return float(response.json()["data"]["amount"])
    else:
        raise Exception(f"Errore API Coinbase: {response.text}")

# Funzione per ottenere i prezzi da KuCoin
def get_kucoin_price(symbol):
    url = f"https://api.kucoin.com/api/v1/market/orderbook/level1?symbol={symbol}"
    response = requests.get(url)
    if response.status_code == 200:
        return float(response.json()["data"]["price"])
    else:
        raise Exception(f"Errore API KuCoin: {response.text}")

# Funzione per ottenere i prezzi da Bitfinex
def get_bitfinex_price(symbol):
    url = f"https://api-pub.bitfinex.com/v2/ticker/t{symbol}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return float(data[6])  # Prezzo di chiusura (ultima transazione)
    else:
        raise Exception(f"Errore API Bitfinex: {response.text}")

# Wrapper per ottenere i prezzi da tutti gli exchange
def get_prices(symbols):
    prices = {}
    try:
        prices["Binance"] = get_binance_price(symbols["binance"])
    except Exception as e:
        print(e)
    try:
        prices["Kraken"] = get_kraken_price(symbols["kraken"])
    except Exception as e:
        print(e)
    try:
        prices["Coinbase"] = get_coinbase_price(symbols["coinbase"])
    except Exception as e:
        print(e)
    try:
        prices["KuCoin"] = get_kucoin_price(symbols["kucoin"])
    except Exception as e:
        print(e)
    try:
        prices["Bitfinex"] = get_bitfinex_price(symbols["bitfinex"])
    except Exception as e:
        print(e)

    return prices

# Simboli da verificare (adatta ai formati degli exchange)
symbols = {
    "binance": "SOLUSDT",
    "kraken": "SOLUSDT",  # Kraken usa formati particolari
    "coinbase": "SOL-USD",
    "kucoin": "SOL-USDT",
    "bitfinex": "SOLUSD"
}

# Ottieni i prezzi
prices = get_prices(symbols)
print("Prezzi raccolti dai principali exchange:")
for exchange, price in prices.items():
    print(f"{exchange}: {price:.2f} USD")

# Calcola le opportunità di arbitraggio
min_price = min(prices.values())
max_price = max(prices.values())

exchange_min = [exchange for exchange, price in prices.items() if price == min_price][0]
exchange_max = [exchange for exchange, price in prices.items() if price == max_price][0]

if max_price > min_price:
    profit = 10000 * ((max_price/min_price)-1)
    print(f"\nArbitraggio possibile: compra a {min_price:.2f} USD su {exchange_min} e vendi a {max_price:.2f} USD su {exchange_max}. Guadagno potenziale: {profit:.2f} USD")
else:
    print("\nNessuna opportunità di arbitraggio al momento.")
