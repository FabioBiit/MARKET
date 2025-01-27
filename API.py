from binance.client import Client

# Inserisci le tue API key e secret key
api_key = "BlSWVI0QLPhZXBhE2GpGpM4dwpIHFIkozhZVZYzJVfCaBfkgsyaV8AzBeUXv5AQy"
api_secret = "vM8TEQKn0TM5awsSxx43okpS0qguVczFFmGqnxXp6UaZ70f9Cb08U4LNdrNlQ8ZO"

# Crea il client Binance
client = Client(api_key, api_secret)

# Coppie di trading
pair1 = "BTCUSDT"  # USDT → BTC
pair2 = "ETHBTC"   # BTC → ETH
pair3 = "ETHUSDT"  # ETH → USDT

ticker1 = client.get_symbol_ticker(symbol=pair1)
ticker2 = client.get_symbol_ticker(symbol=pair2)
ticker3 = client.get_symbol_ticker(symbol=pair3)

# Ottieni i prezzi
btc_usdt = float(ticker1['price'])  # Prezzo BTC in USDT
eth_btc = float(ticker2['price'])  # Prezzo ETH in BTC
eth_usdt = float(ticker3['price'])  # Prezzo ETH in USDT

# Calcolo ciclo
starting_usdt = 100  # Importo iniziale in USDT
btc = starting_usdt / btc_usdt
eth = btc / eth_btc
final_usdt = eth * eth_usdt

if final_usdt > starting_usdt:
    print(f"Arbitraggio possibile! Guadagno netto: {final_usdt - starting_usdt:.2f} USDT")
else:
    print("Nessuna opportunità di arbitraggio.")

