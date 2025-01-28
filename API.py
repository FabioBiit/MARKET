from binance.client import Client
from time import sleep
from datetime import datetime

# Inserisci le tue API key e secret key
api_key = "BlSWVI0QLPhZXBhE2GpGpM4dwpIHFIkozhZVZYzJVfCaBfkgsyaV8AzBeUXv5AQy"
api_secret = "vM8TEQKn0TM5awsSxx43okpS0qguVczFFmGqnxXp6UaZ70f9Cb08U4LNdrNlQ8ZO"

# Crea il client Binance
client = Client(api_key, api_secret)

# Coppie di trading
pair1 = "BTCUSDT"  # USDT → BTC
pair2 = "ETHBTC"   # BTC → ETH
pair3 = "ETHUSDT"  # ETH → USDT

# Ottieni i prezzi dalle API Binance
ticker1 = client.get_symbol_ticker(symbol=pair1)
ticker2 = client.get_symbol_ticker(symbol=pair2)
ticker3 = client.get_symbol_ticker(symbol=pair3)

while True:
    # Prezzi delle coppie
    btc_usdt = float(ticker1['price'])  # Prezzo BTC in USDT
    eth_btc = float(ticker2['price'])  # Prezzo ETH in BTC
    eth_usdt = float(ticker3['price'])  # Prezzo ETH in USDT

    # Commissioni di trading (da Binance, ad esempio 0.1%)
    commissione = 0.001  # Commissione (0.1%)

    # Importo iniziale in USDT
    starting_usdt = 3000  

    # Calcolo del ciclo di arbitraggio
    btc = starting_usdt / (btc_usdt * (1 + commissione))                # Converti USDT → BTC
    eth = btc / (eth_btc * (1 + commissione))                           # Converti BTC → ETH
    final_usdt = eth * eth_usdt * (1 - commissione)                     # Converti ETH → USDT

    # Verifica il guadagno netto
    guadagno_netto = final_usdt - starting_usdt

    if guadagno_netto > 0:
    # Scrivere i risultati su un file di testo
        with open("risultati_arbitraggio_market.csv", "a") as file:
            file.write(f"Importo investito, {starting_usdt} USDT\n")
            file.write(f"Guadagno netto, {guadagno_netto:.2f} USDT\n")
            file.write(f"Data e ora, {datetime.now()}\n")

    if guadagno_netto > 0:
        print(f"Arbitraggio possibile! Guadagno netto: {guadagno_netto:.2f} USDT")
    else:
        print("Nessuna opportunità di arbitraggio.")
    sleep(5)

