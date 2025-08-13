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
    # log_entry = (
        # f"{datetime.now()}, {capitale_investito}, {crypto}, {prezzo_acquisto:.10f}, {ex_acq}, {prezzo_vendita:.10f}, {ex_ven}, "
        # f"{commissione_acquisto * 100}%, {commissione_vendita * 100}%, {guadagno_netto:.2f} USDT\n"
    # )

    # Scrittura nei file CSV
    # with open("risultati_arbitraggio_market_all.csv", "a") as file:
        # file.write(log_entry)

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
    "Bitfinex": {"url": "https://api-pub.bitfinex.com/v2/ticker/{}", "key_path": [0]},
    "Bybit": {"url": "https://api.bybit.com/v5/market/tickers?category=spot&symbol={}", "key_path": ["result", "list", 0, "lastPrice"]}
}

# Simboli per i vari exchange
SYMBOLS = {
    "SOL": {"Binance": "SOLUSDT", "KuCoin": "SOL-USDT", "Bitfinex": "tSOLUST", "Bybit": "SOLUSDT"},
    #"BTC": {"Binance": "BTCUSDT", "KuCoin": "BTC-USDT", "Bitfinex": "tBTCUST", "Bybit": "BTCUSDT"},
    #"PEPE": {"Binance": "PEPEUSDT", "KuCoin": "PEPE-USDT", "Bybit": "PEPEUSDT"},
    #"DOGE": {"Binance": "DOGEUSDT", "KuCoin": "DOGE-USDT", "Bybit": "DOGEUSDT"},
    "XRP": {"Binance": "XRPUSDT", "KuCoin": "XRP-USDT", "Bitfinex": "tXRPUST", "Bybit": "XRPUSDT"},
    "ADA": {"Binance": "ADAUSDT", "KuCoin": "ADA-USDT", "Bitfinex": "tADAUST", "Bybit": "ADAUSDT"},
    "LTC": {"Binance": "LTCUSDT", "KuCoin": "LTC-USDT", "Bitfinex": "tLTCUST", "Bybit": "LTCUSDT"}
}

# Commissioni per ogni exchange
COMMISSIONI = {
    "Binance": {"acquisto": 0.001, "vendita": 0.001},
    "KuCoin": {"acquisto": 0.001, "vendita": 0.001},
    "Bitfinex": {"acquisto": 0.002, "vendita": 0.001},
    "Bybit": {"acquisto": 0.001, "vendita": 0.001}
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

capitale_investito = 5000 # USDT
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

        if guadagno >= 5:
            print(f"\n{crypto}: Compra a {min_price:.8f} USDT su {min_ex}, Vendi a {max_price:.8f} USDT su {max_ex}")
            print(f"Guadagno potenziale: {guadagno:.8f} USDT")

            """
            if min_ex saldo is empty: # Da implementare
                print(f"Recupera tutto il capitale dall'ultima transazione effettuata sull'exchange {max_ex}") # USDT
                print(f"Trasferisci tutto il capitale recuperato sull'exchange {min_ex}") # USDT
            """

            print(f"Trasferisci tutto il capitale recuperato sull'exchange {min_ex}") # USDT

            print(f"Eseguo Ordine Acquisto su {min_ex} di {crypto}")

            print(f"Trasferisci la criptovaluta acquistata {crypto} sull'exchange {max_ex}")

            # Ho ipotizzato che la differenza di slippage possa essere del 20% rispetto al guadagno, fosse così sarebbe accettabile!
            slippage = guadagno * 0.50

            print(f"Guadagno lordo 'stimato' considerando lo slippage: {guadagno - slippage:.8f} USDT")

            sleep(300)  # Simulo il tempo per il completamento di un ordine Acquisto-Vendita

            # Recupero del prezzo di vendita aggiornato dopo lo sleep
            exchange_vendita = max_ex
            symbol_vendita = symbol_map[exchange_vendita]
            api_vendita = EXCHANGE_API.get(exchange_vendita)

            if api_vendita:
                url_vendita = api_vendita["url"].format(symbol_vendita)
                price_vendita = get_price(url_vendita, api_vendita["key_path"], exchange_vendita)[1]
                print(f"Prezzo di vendita aggiornato: {price_vendita:.8f} USDT")

                # Calcolo del guadagno reale
                guadagno_reale = calcola_guadagno_arbitraggio(
                    capitale_investito, min_price, price_vendita, min_ex, max_ex, commissione_acquisto, commissione_vendita, crypto
                )

                guadagno_reale = guadagno_reale - 3 # -1 per la commissione di prelievo su rete TRON-20 più -2 per la commissione sulla rete crypto

                # Imposta i vari tempi di attesa trasferimento per le cripto e i vari costi di prelievo

                print(f"Guadagno netto stimato: {guadagno_reale:.8f} USDT")

                if guadagno_reale >= 1: 
                    print(f"Eseguo Ordine Vendita su {max_ex} di {crypto}")

                    log_entry = (
                        f"{datetime.now()}, {capitale_investito}, {crypto}, {min_price:.10f}, {min_ex}, {price_vendita:.10f}, {max_ex}, "
                        f"{commissione_acquisto * 100}%, {commissione_vendita * 100}%, {guadagno_reale:.2f} USDT\n"
    )

                    with open("risultati_arbitraggio_new_market_positivo_v2.csv", "a") as file:
                        file.write(log_entry)

                else:
                    print(f"Arbitraggio non conveniente. Crea Ordine limit all'ultimo price rilevato {max_price:.10f}.")


                    """  # Da Testare!!!
                    while(True):
                    
                        url_vendita = api_vendita["url"].format(symbol_vendita)
                        price_vendita = get_price(url_vendita, api_vendita["key_path"], exchange_vendita)[1]
                        print(f"Prezzo di vendita aggiornato: {price_vendita:.8f} USDT")

                        sleep(5)

                        if price_vendita >= max_price:
                    
                            print(f"Esecuzione ordine limit a: {max_price:.8f} USDT")

                        break
                    """

        else:
            print("Nessuna opportunità di arbitraggio.")

    sleep(10)  # Pausa prima del prossimo ciclo