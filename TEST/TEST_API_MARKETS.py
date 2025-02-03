import requests
from time import sleep
from datetime import datetime

def calcola_guadagno_arbitraggio(capitale_investito, prezzo_acquisto, prezzo_vendita, ex_acq, ex_ven, commissione_acquisto, commissione_vendita):
    # Calcolo della commissione di acquisto
    tot_commissione_acq = capitale_investito * commissione_acquisto

    # Calcolo della quantità acquistata (dopo aver tolto la commissione)
    quantita_acquistata = (capitale_investito - tot_commissione_acq) / prezzo_acquisto

    # Calcolo della commissione di vendita
    tot_commissione_ven = (quantita_acquistata * prezzo_vendita) * commissione_vendita

    # Calcolo dell'importo netto di vendita
    importo_vendita_netto = (quantita_acquistata * prezzo_vendita) - tot_commissione_ven

    # Guadagno netto
    guadagno_netto = importo_vendita_netto - capitale_investito

    
    # Scrivere i risultati su un file di testo
    with open("risultati_arbitraggio_market_all.csv", "a") as file:
        file.write(f"Importo_investito, {capitale_investito} USDT\n")
        file.write(f"Prezzo_di_acquisto, {prezzo_acquisto} USDT su {ex_acq}\n")
        file.write(f"Prezzo_di_vendita, {prezzo_vendita} USDT su {ex_ven}\n")
        file.write(f"Commissioni_acquisto, {commissione_acquisto * 100}%\n")
        file.write(f"Commissioni_vendita, {commissione_vendita * 100}%\n")
        file.write(f"Guadagno_netto, {guadagno_netto:.2f} USDT\n")
        file.write(f"Data_e_ora, {datetime.now()}\n")

    if guadagno_netto > 1:
        with open("risultati_arbitraggio_market_positivo.csv", "a") as file:
            file.write(f"Importo_investito, {capitale_investito} USDT\n")
            file.write(f"Prezzo_di_acquisto, {prezzo_acquisto} USDT su {ex_acq}\n")
            file.write(f"Prezzo_di_vendita, {prezzo_vendita} USDT su {ex_ven}\n")
            file.write(f"Commissioni_acquisto, {commissione_acquisto * 100}%\n")
            file.write(f"Commissioni_vendita, {commissione_vendita * 100}%\n")
            file.write(f"Guadagno_netto, {guadagno_netto:.2f} USDT\n")
            file.write(f"Data_e_ora, {datetime.now()}\n")
    
    return guadagno_netto

# Funzioni per ottenere i prezzi dai vari exchange (come definite sopra)

# Funzione per ottenere il prezzo da Binance
def get_binance_price(symbol):
    url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
    response = requests.get(url)
    data = response.json()
    return float(data["price"])

# Funzione per ottenere il prezzo da Bybit
def get_bybit_price(symbol):
    url = "https://api.bybit.com/v5/market/tickers?category=spot"
    response = requests.get(url).json()
    for ticker in response["result"]["list"]:
        if ticker["symbol"] == symbol:
            return float(ticker["lastPrice"])
    return None

# Funzione per ottenere il prezzo da KuCoin
def get_kucoin_price(symbol):
    url = f"https://api.kucoin.com/api/v1/market/orderbook/level1?symbol={symbol}"
    response = requests.get(url)
    data = response.json()
    return float(data["data"]["price"])

# Funzione per ottenere il prezzo da Bitfinex
def get_bitfinex_price(symbol):
    url = f"https://api.bitfinex.com/v1/pubticker/{symbol}"
    response = requests.get(url)
    data = response.json()
    return float(data["last_price"])

# Wrapper per ottenere i prezzi da tutti gli exchange
def get_prices(symbols):
    prices = {}
    try:
        prices["Binance"] = get_binance_price(symbols["binance"])
    except Exception as e:
        print(f"Errore Binance: {e}")
    try:
        prices["Bybit"] = get_bybit_price(symbols["bybit"])
    except Exception as e:
        print(f"Errore Bybit: {e}")
    try:
        prices["KuCoin"] = get_kucoin_price(symbols["kucoin"])
    except Exception as e:
        print(f"Errore KuCoin: {e}")
    try:
        prices["Bitfinex"] = get_bitfinex_price(symbols["bitfinex"])
    except Exception as e:
        print(f"Errore Bitfinex: {e}")

    return prices

# Simboli per i vari exchange
symbolsSol = {
    "binance": "SOLUSDT",
    "bybit": "SOLUSDT",
    "kucoin": "SOL-USDT",
    "bitfinex": "SOLUSD"
}

symbolsBtc = {
    "binance": "BTCUSDT",
    "bybit": "BTCUSDT",
    "kucoin": "SOL-USDT",
    "bitfinex": "BTCUSD"
}

symbolsEth = {    
    "binance": "ETHUSDT",
    "bybit": "ETHUSDT",
    "kucoin": "ETH-USDT",
    "bitfinex": "ETHUSD"
}

# Commissioni per ogni exchange (valori in percentuale)
commissioni = {
    "Binance": {"acquisto": 0.001, "vendita": 0.001},
    "Bybit": {"acquisto": 0.001, "vendita": 0.001},
    "KuCoin": {"acquisto": 0.001, "vendita": 0.001},
    "Bitfinex": {"acquisto": 0.002, "vendita": 0.001}
}

while True:
    # Ottieni i prezzi dai vari exchange
    pricesSol = get_prices(symbolsSol)
    pricesBtc = get_prices(symbolsBtc)
    pricesEth = get_prices(symbolsEth)

    tot_prices = [pricesSol, pricesBtc, pricesEth]

    print("Prezzi raccolti dai principali exchange:")
    for i, prices in enumerate(tot_prices):  # Itera su ogni dizionario nella lista
        print(f"Prezzi per la criptovaluta {i + 1}:")

        # Determina l'exchange con il prezzo più basso e quello con il prezzo più alto
        min_price = min(prices.values())
        max_price = max(prices.values())

        exchange_min = [exchange for exchange, price in prices.items() if price == min_price][0]
        exchange_max = [exchange for exchange, price in prices.items() if price == max_price][0]

        for exchange, price in prices.items():  # Itera su ogni coppia chiave-valore nel dizionario
            print(f"{exchange}: {price:.2f} USD")

        # Calcola il guadagno potenziale
        capitale_investito = 3000  # in USDT
        commissione_acquisto = commissioni[exchange_min]["acquisto"]
        commissione_vendita = commissioni[exchange_max]["vendita"]

        #print(commissione_acquisto)
        #print(commissione_vendita)

        ex_acq = exchange_min
        ex_ven = exchange_max

        guadagno = calcola_guadagno_arbitraggio(capitale_investito, min_price, max_price, ex_acq, ex_ven, commissione_acquisto, commissione_vendita)

        if guadagno > 0:
            print(f"\nArbitraggio possibile: compra a {min_price:.2f} USD su {exchange_min} e vendi a {max_price:.2f} USD su {exchange_max}.")
            print(f"Guadagno potenziale: {guadagno:.2f} USD")
        else:
            print("\nNessuna opportunità di arbitraggio al momento.")
    
    sleep(5)  # Attesa prima della prossima iterazione