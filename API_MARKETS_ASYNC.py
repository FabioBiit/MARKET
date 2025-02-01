import requests
from time import sleep
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# Funzione per calcolare il guadagno dell'arbitraggio
def calcola_guadagno_arbitraggio(capitale_investito, prezzo_acquisto, prezzo_vendita, ex_acq, ex_ven, commissione_acquisto, commissione_vendita, crypto):
    # Calcolo della quantità acquistata dopo la commissione
    quantita_acquistata = (capitale_investito * (1 - commissione_acquisto)) / prezzo_acquisto
    
    # Calcolo dell'importo netto di vendita dopo la commissione
    importo_vendita_netto = (quantita_acquistata * prezzo_vendita) * (1 - commissione_vendita)
    
    # Guadagno netto
    guadagno_netto = importo_vendita_netto - capitale_investito

    # Struttura dati per il log
    log_entry = (
        f"{datetime.now()}, {capitale_investito}, {crypto}, {prezzo_acquisto}, {ex_acq}, {prezzo_vendita}, {ex_ven}, "
        f"{commissione_acquisto * 100}%, {commissione_vendita * 100}%, {guadagno_netto:.2f} USDT\n"
    )

    # Scrittura nei file CSV
    with open("risultati_arbitraggio_market_all.csv", "a") as file:
        file.write(log_entry)

    if guadagno_netto > 1:
        with open("risultati_arbitraggio_new_market_positivo.csv", "a") as file:
            file.write(log_entry)

    return guadagno_netto

# Funzione generica per ottenere il prezzo da un exchange
def get_price(url, key_path, exchange):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Controlla errori HTTP
        data = response.json()
        for key in key_path:
            data = data[key]
        return (exchange, float(data))  # Restituisce l'exchange e il prezzo
    except Exception as e:
        print(f"Errore su {url}: {e}")
        return (exchange, None)

# URL e chiavi per ottenere il prezzo dai vari exchange
EXCHANGE_API = {
    "Binance": {"url": "https://api.binance.com/api/v3/ticker/price?symbol={}", "key_path": ["price"]},
    "KuCoin": {"url": "https://api.kucoin.com/api/v1/market/orderbook/level1?symbol={}", "key_path": ["data", "price"]},
    "Bitfinex": {"url": "https://api.bitfinex.com/v1/pubticker/{}", "key_path": ["last_price"]},
}

# Simboli per i vari exchange
SYMBOLS = {
    "SOL": {"Binance": "SOLUSDT", "KuCoin": "SOL-USDT", "Bitfinex": "SOLUSD"},
    "BTC": {"Binance": "BTCUSDT", "KuCoin": "BTC-USDT", "Bitfinex": "BTCUSD"},
    "ETH": {"Binance": "ETHUSDT", "KuCoin": "ETH-USDT", "Bitfinex": "ETHUSD"},
    "PEPE": {"Binance": "PEPEUSDT", "KuCoin": "PEPE-USDT"},
    "DOGE": {"Binance": "DOGEUSDT", "KuCoin": "DOGE-USDT"},
    "XRP": {"Binance": "XRPUSDT", "KuCoin": "XRP-USDT", "Bitfinex": "XRPUSD"},
    "ADA": {"Binance": "ADAUSDT", "KuCoin": "ADA-USDT", "Bitfinex": "ADAUSD"},
    "LTC": {"Binance": "LTCUSDT", "KuCoin": "LTC-USDT", "Bitfinex": "LTCUSD"},
}

# Commissioni per ogni exchange
COMMISSIONI = {
    "Binance": {"acquisto": 0.001, "vendita": 0.001},
    "KuCoin": {"acquisto": 0.001, "vendita": 0.001},
    "Bitfinex": {"acquisto": 0.002, "vendita": 0.001},
}

# Funzione per ottenere i prezzi da tutti gli exchange per una data criptovaluta in parallelo
def get_prices(symbol_map):
    prices = {}
    with ThreadPoolExecutor() as executor:
        futures = []
        for exchange, symbol in symbol_map.items():
            api = EXCHANGE_API.get(exchange)
            if api:
                url = api["url"].format(symbol)
                futures.append(executor.submit(get_price, url, api["key_path"], exchange))
        
        for future in futures:
            exchange, price = future.result()
            if price is not None:
                prices[exchange] = price
    return prices

capitale_investito = 3000  # USDT
while True:
    print("\n--- Analisi Arbitraggio ---")

    for crypto, symbol_map in SYMBOLS.items():
        prices = get_prices(symbol_map)
        if len(prices) < 2:
            print(f"Prezzi insufficienti per {crypto}.")
            continue

        min_ex, min_price = min(prices.items(), key=lambda x: x[1])
        max_ex, max_price = max(prices.items(), key=lambda x: x[1])

        commissione_acquisto = COMMISSIONI[min_ex]["acquisto"]
        commissione_vendita = COMMISSIONI[max_ex]["vendita"]

        guadagno = calcola_guadagno_arbitraggio(
            capitale_investito, min_price, max_price, min_ex, max_ex, commissione_acquisto, commissione_vendita, crypto
        )

        if guadagno > 1:
            print(f"\n{crypto}: Compra a {min_price:.2f} USDT su {min_ex}, Vendi a {max_price:.2f} USDT su {max_ex}")
            print(f"Guadagno potenziale: {guadagno:.2f} USDT")
        else:
            print("Nessuna opportunità di arbitraggio.")

    sleep(20)  # Pausa prima del prossimo ciclo
