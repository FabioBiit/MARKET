import requests
from requests.adapters import HTTPAdapter, Retry
from time import sleep
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

# --- Configurazioni ---
SLEEP_ARBITRAGGIO = 10      # sec
SLEEP_ORDER = 300           # sec simulazione ordine
NETWORK_FEE = 3             # Fee di rete fissa stimata (USDT)
SLIPPAGE_PERC = 0.5         # Percentuale di slippage stimata
GUADAGNO_MINIMO = 5         # Guadagno minimo per segnalazione (USDT)
GUADAGNO_VENDITA_MINIMO = 1 # Guadagno minimo per vendere (USDT)
THREADS = 4                 # Numero massimo di thread per parallellismo

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(message)s"
)
logger = logging.getLogger("arbitraggio")

# --- Funzione calcolo guadagno ---
def calcola_guadagno_arbitraggio(capitale_investito, prezzo_acquisto, prezzo_vendita, commissione_acquisto, commissione_vendita):
    quantita_acquistata = (capitale_investito * (1 - commissione_acquisto)) / prezzo_acquisto
    importo_vendita_netto = (quantita_acquistata * prezzo_vendita) * (1 - commissione_vendita)
    guadagno_netto = importo_vendita_netto - capitale_investito
    return guadagno_netto

# --- Sessione HTTP ottimizzata ---
def get_requests_session():
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    return session

session = get_requests_session()

# --- Funzione recupero prezzo ---
def get_price(url, key_path, exchange):
    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        for key in key_path:
            data = data[key] if not isinstance(key, int) else data[key]
        return (exchange, float(data))
    except Exception as e:
        logger.warning(f"Errore su {exchange} ({url}): {e}")
        return (exchange, None)

# --- Dati Exchange ---
EXCHANGE_API = {
    "Binance": {"url": "https://api.binance.com/api/v3/ticker/price?symbol={}", "key_path": ["price"]},
    "KuCoin": {"url": "https://api.kucoin.com/api/v1/market/orderbook/level1?symbol={}", "key_path": ["data", "price"]},
    "Bitfinex": {"url": "https://api-pub.bitfinex.com/v2/ticker/{}", "key_path": [0]},
    "Bybit": {"url": "https://api.bybit.com/v5/market/tickers?category=spot&symbol={}", "key_path": ["result", "list", 0, "lastPrice"]}
}

SYMBOLS = {
    "SOL": {"Binance": "SOLUSDT", "KuCoin": "SOL-USDT", "Bitfinex": "tSOLUST", "Bybit": "SOLUSDT"},
    "XRP": {"Binance": "XRPUSDT", "KuCoin": "XRP-USDT", "Bitfinex": "tXRPUST", "Bybit": "XRPUSDT"},
    "ADA": {"Binance": "ADAUSDT", "KuCoin": "ADA-USDT", "Bitfinex": "tADAUST", "Bybit": "ADAUSDT"},
    "LTC": {"Binance": "LTCUSDT", "KuCoin": "LTC-USDT", "Bitfinex": "tLTCUST", "Bybit": "LTCUSDT"}
}

COMMISSIONI = {
    "Binance": {"acquisto": 0.001, "vendita": 0.001},
    "KuCoin": {"acquisto": 0.001, "vendita": 0.001},
    "Bitfinex": {"acquisto": 0.002, "vendita": 0.001},
    "Bybit": {"acquisto": 0.001, "vendita": 0.001}
}

# --- Recupero prezzi parallelo ---
def get_prices(symbol_map):
    prices = {}
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        future_to_exchange = {
            executor.submit(
                get_price, 
                EXCHANGE_API[exchange]["url"].format(symbol),
                EXCHANGE_API[exchange]["key_path"],
                exchange
            ): exchange
            for exchange, symbol in symbol_map.items()
            if exchange in EXCHANGE_API
        }
        for future in as_completed(future_to_exchange):
            exchange, price = future.result()
            if price is not None:
                prices[exchange] = price
    return prices

def salva_log(path, contenuto):
    try:
        with open(path, "a") as file:
            file.write(contenuto)
    except Exception as e:
        logger.error(f"Errore nella scrittura del file di log: {e}")

# --- Main Loop ---
def main():
    capitale_investito = 5000
    while True:
        logger.info("--- Analisi Arbitraggio ---")
        for crypto, symbol_map in SYMBOLS.items():
            prices = get_prices(symbol_map)
            if len(prices) < 2:
                logger.warning(f"Prezzi insufficienti per {crypto}.")
                continue

            min_ex, min_price = min(prices.items(), key=lambda x: x[1])
            max_ex, max_price = max(prices.items(), key=lambda x: x[1])

            commissione_acquisto = COMMISSIONI[min_ex]["acquisto"]
            commissione_vendita = COMMISSIONI[max_ex]["vendita"]

            guadagno = calcola_guadagno_arbitraggio(
                capitale_investito, min_price, max_price, commissione_acquisto, commissione_vendita
            )

            if guadagno >= GUADAGNO_MINIMO:
                logger.info(f"{crypto}: Compra a {min_price:.8f} USDT su {min_ex}, Vendi a {max_price:.8f} USDT su {max_ex}")
                logger.info(f"Guadagno potenziale: {guadagno:.8f} USDT")

                slippage = guadagno * SLIPPAGE_PERC
                logger.info(f"Guadagno lordo stimato (slippage 50%): {guadagno - slippage:.8f} USDT")

                sleep(SLEEP_ORDER)  # Simula tempo ordine

                # Prezzo vendita aggiornato
                symbol_vendita = symbol_map[max_ex]
                api_vendita = EXCHANGE_API.get(max_ex)
                url_vendita = api_vendita["url"].format(symbol_vendita)
                price_vendita = get_price(url_vendita, api_vendita["key_path"], max_ex)[1]
                logger.info(f"Prezzo vendita aggiornato: {price_vendita:.8f} USDT")

                guadagno_reale = calcola_guadagno_arbitraggio(
                    capitale_investito, min_price, price_vendita, commissione_acquisto, commissione_vendita
                ) - NETWORK_FEE

                logger.info(f"Guadagno netto stimato: {guadagno_reale:.8f} USDT")

                if guadagno_reale >= GUADAGNO_VENDITA_MINIMO:
                    logger.info(f"Eseguo Ordine Vendita su {max_ex} di {crypto}")

                    log_entry = (
                        f"{datetime.now()}, {capitale_investito}, {crypto}, {min_price:.10f}, {min_ex}, {price_vendita:.10f}, {max_ex}, "
                        f"{commissione_acquisto * 100}%, {commissione_vendita * 100}%, {guadagno_reale:.2f} USDT\n"
                    )
                    salva_log("risultati_arbitraggio_new_market_positivo_v2.csv", log_entry)
                else:
                    logger.info(f"Arbitraggio non conveniente. Crea Ordine limit all'ultimo price rilevato. {max_price:.10f}.")
            else:
                logger.info("Nessuna opportunità di arbitraggio.")

        sleep(SLEEP_ARBITRAGGIO)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Interrotto dall'utente.")
    except Exception as e:
        logger.error(f"Errore imprevisto: {e}")
