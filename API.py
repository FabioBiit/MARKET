import requests
from time import sleep
from datetime import datetime

def get_binance_price(symbol):
    """
    Ottieni il prezzo attuale di una coppia di trading da Binance.
    """
    url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
    response = requests.get(url)
    data = response.json()
    if "price" in data:
        return float(data["price"])
    else:
        raise ValueError(f"Errore nel recuperare il prezzo per {symbol}: {data}")

def arbitraggio_triangolare_binance(initial_amount, pairs, fees):
    """
    Calcola il profitto netto da un arbitraggio triangolare usando i prezzi dinamici da Binance.
    """
    # Recupera i tassi di cambio dinamici da Binance
    rates = [get_binance_price(pair) for pair in pairs]
    
    # Step 1: Converti USDT in BTC
    btc_amount = initial_amount * (1 - fees[0]) / rates[0]  # Dividiamo per il tasso BTCUSDT
    
    # Step 2: Converti BTC in ETH
    eth_amount = btc_amount * (1 - fees[1]) / rates[1]  # Dividiamo per il tasso ETHBTC
    
    # Step 3: Converti ETH in USDT
    final_amount = eth_amount * (1 - fees[2]) * rates[2]  # Moltiplichiamo per il tasso ETHUSDT
    
    return final_amount, rates

while True:

    # Parametri iniziali
    initial_amount = 1000  # Inizia con 1000 USDT
    pairs = ["BTCUSDT", "ETHBTC", "ETHUSDT"]  # Coppie di trading in ordine
    fees = [0.001, 0.001, 0.001]  # Commissioni: 0.1% per ogni transazione

    # Esecuzione del calcolos
    try:
        final_amount, rates = arbitraggio_triangolare_binance(initial_amount, pairs, fees)
        profit = final_amount - initial_amount

        # Output dei risultati
        print(f"Tassi di cambio dinamici:")
        print(f"  {pairs[0]}: {rates[0]:.2f}")
        print(f"  {pairs[1]}: {rates[1]:.2f}")
        print(f"  {pairs[2]}: {rates[2]:.2f}")
        print(f"Quantità iniziale: {initial_amount:.2f} USDT")
        print(f"Quantità finale: {final_amount:.2f} USDT")
        print(f"Profitto netto: {profit:.2f} USDT")
        print(f"Data e ora: {datetime.now()}\n")

        with open("risultati_arbitraggio_triangolare_all_binance.csv", "a") as file:
            file.write(f"Quantita_iniziale, {initial_amount} USDT\n")
            file.write(f"Quantita_finale, {final_amount} USDT\n")
            file.write(f"Profitto_netto, {profit} USDT\n")
            file.write(f"Data_e_ora, {datetime.now()}\n")

        if profit > 0:
            with open("risultati_arbitraggio_triangolare_positivo_binance.csv", "a") as file:
                file.write(f"Quantita_iniziale, {initial_amount} USDT\n")
                file.write(f"Quantita_finale, {final_amount} USDT\n")
                file.write(f"Profitto_netto, {profit} USDT\n")
                file.write(f"Data_e_ora, {datetime.now()}\n")

        sleep(5)

    except ValueError as e:
        print(e)










"""
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

    # Scrivere i risultati su un file di testo
    with open("risultati_arbitraggio_coppie_all_binance.csv", "a") as file:
        file.write(f"Importo investito, {starting_usdt} USDT\n")
        file.write(f"Guadagno netto, {guadagno_netto:.2f} USDT\n")
        file.write(f"Data e ora, {datetime.now()}\n")

    if guadagno_netto > 0:
    # Scrivere i risultati su un file di testo
        with open("risultati_arbitraggio_coppie_positivo_binance.csv", "a") as file:
            file.write(f"Importo investito, {starting_usdt} USDT\n")
            file.write(f"Guadagno netto, {guadagno_netto:.2f} USDT\n")
            file.write(f"Data e ora, {datetime.now()}\n")

    if guadagno_netto > 0:
        print(f"Arbitraggio possibile! Guadagno netto: {guadagno_netto:.2f} USDT")
    else:
        print("Nessuna opportunità di arbitraggio.")
    sleep(5)
"""
