
import os
import re
import gc
import hmac
import json
import base64
import hashlib
import time
import yaml
import asyncio
import random
import logging
from logging.handlers import RotatingFileHandler
from decimal import Decimal, getcontext, InvalidOperation
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple, Optional

import aiohttp
from aiohttp import web
from pydantic import BaseModel, PositiveInt, validator

# ======================
# Precisione numerica
# ======================
getcontext().prec = 28
D = Decimal
def Dsafe(x) -> D:
    try:
        return x if isinstance(x, D) else D(str(x))
    except (InvalidOperation, ValueError, TypeError):
        return D("0")

# ======================
# Config & Validazione
# ======================
class FeeCfg(BaseModel):
    acquisto: D
    vendita: D

class BotCfg(BaseModel):
    sleep_scan_sec: PositiveInt = 5
    order_hold_sec: PositiveInt = 2
    max_orderbook_levels: PositiveInt = 10
    max_staleness_sec: PositiveInt = 2
    latency_warn_ms: PositiveInt = 1000
    spread_min_bps: D = D("0.001")     # 0.10%
    gain_min_usdt: D = D("5")
    gain_rt_min_usdt: D = D("1")
    risk_cushion_bps: D = D("0.0002")
    drop_threshold_rel: D = D("0.0008")
    prefunding_mode: bool = True
    network_fee_usdt: D = D("0")
    fee_safety_bps: D = D("0.0001")
    threads_limit_per_host: PositiveInt = 8
    global_concurrency: PositiveInt = 64
    cb_max_negative_streak: PositiveInt = 5
    cb_cooldown_sec: PositiveInt = 10
    log_file: str = "arb_bot.jsonl"
    log_csv: str = "arb_profits.csv"
    capital_usdt: D = D("5000")
    max_slippage_bps: D = D("0.0015")   # 0.15% slippage target
    max_notional_per_trade_usdt: D = D("10000")
    min_depth_levels: PositiveInt = 3
    http_listen_host: str = "0.0.0.0"
    http_listen_port: PositiveInt = 8080
    fee_cache_sec: PositiveInt = 600     # cache 10 minuti
    print_spreads: bool = False          # stampa tabellare spread migliori a console

class AppCfg(BaseModel):
    # simboli per exchange (spot)
    symbols: Dict[str, Dict[str, str]]
    # fee conservative fisse di default (fallback)
    commissioni: Dict[str, FeeCfg]
    bot: BotCfg
    # network fee per asset (facoltativo): {"SOL": 0.01, ...} in USDT-equivalenti
    network_fee_asset: Dict[str, D] = {}

    @validator("symbols")
    def validate_symbols(cls, v):
        assert all(isinstance(k, str) and isinstance(v[k], dict) and v[k] for k in v), "symbols invalidi"
        return v

DEFAULT_CFG = AppCfg(
    symbols={
        "SOL": {"Binance": "SOLUSDT", "KuCoin": "SOL-USDT", "MEXC": "SOLUSDT", "OKX": "SOL-USDT", "Bybit": "SOLUSDT"},
        "XRP": {"Binance": "XRPUSDT", "KuCoin": "XRP-USDT", "MEXC": "XRPUSDT", "OKX": "XRP-USDT", "Bybit": "XRPUSDT"},
        "ADA": {"Binance": "ADAUSDT", "KuCoin": "ADA-USDT", "MEXC": "ADAUSDT", "OKX": "ADA-USDT", "Bybit": "ADAUSDT"},
        "LTC": {"Binance": "LTCUSDT", "KuCoin": "LTC-USDT", "MEXC": "LTCUSDT", "OKX": "LTC-USDT", "Bybit": "LTCUSDT"},
    },
    commissioni={
        # decimali (0.001 = 0.1%)
        "Binance": FeeCfg(acquisto=D("0.001"), vendita=D("0.001")),
        "KuCoin":  FeeCfg(acquisto=D("0.001"), vendita=D("0.001")),
        "MEXC":    FeeCfg(acquisto=D("0.001"), vendita=D("0.001")),  # fallback prudente
        "OKX":     FeeCfg(acquisto=D("0.0008"), vendita=D("0.001")), # maker 0.08%, taker 0.1% (indicativo)
        "Bybit":   FeeCfg(acquisto=D("0.001"), vendita=D("0.001")),
    },
    bot=BotCfg(),
    network_fee_asset={}
)

def load_cfg(path: Optional[str]) -> AppCfg:
    if path and os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        if "commissioni" in data:
            for k, v in list(data["commissioni"].items()):
                data["commissioni"][k] = FeeCfg(**v)
        if "bot" in data:
            data["bot"] = BotCfg(**data["bot"])
        if "symbols" not in data:
            data["symbols"] = DEFAULT_CFG.symbols
        return AppCfg(**data)
    return DEFAULT_CFG

CFG = load_cfg(os.getenv("ARB_CFG"))

# ======================
# Logging strutturato
# ======================
logger = logging.getLogger("arb")
logger.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s %(levelname)s:%(message)s")
sh = logging.StreamHandler(); sh.setFormatter(fmt); logger.addHandler(sh)
rfh = RotatingFileHandler(CFG.bot.log_file, maxBytes=10_000_000, backupCount=5)
rfh.setFormatter(fmt); logger.addHandler(rfh)

try:
    import orjson
    def log_json(event: str, **payload):
        rec = {"ts": datetime.now(timezone.utc).isoformat(), "event": event, **payload}
        with open(CFG.bot.log_file, "a", encoding="utf-8") as f:
            f.write(orjson.dumps(rec).decode() + "\n")
except ImportError:
    def log_json(event: str, **payload):
        rec = {"ts": datetime.now(timezone.utc).isoformat(), "event": event, **payload}
        with open(CFG.bot.log_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

# ======================
# Metrics (Prometheus-like)
# ======================
METRICS = {
    "loop_runs_total": 0,
    "opportunities_found_total": 0,
    "trades_rt_ok_total": 0,
    "cb_tripped_total": 0,
    "api_errors_total": 0,
    "latency_ms_sum": 0,
    "latency_ms_count": 0,
}
def metrics_text():
    avg_latency = METRICS["latency_ms_sum"] / max(METRICS["latency_ms_count"], 1)
    lines = [
        f"# HELP avg_latency_ms Average latency across requests",
        f"# TYPE avg_latency_ms gauge",
        f"avg_latency_ms {avg_latency:.2f}"
    ]
    for k, v in METRICS.items():
        lines.append(f"# TYPE {k} counter")
        lines.append(f"{k} {v}")
    return "\n".join(lines) + "\n"

# ======================
# HTTP server (health & metrics)
# ======================
async def handle_health(request):
    return web.json_response({"status":"ok","ts": datetime.now(timezone.utc).isoformat()})

async def handle_metrics(request):
    return web.Response(text=metrics_text(), content_type="text/plain; version=0.0.4")

async def start_http_server():
    web_app = web.Application()
    web_app.router.add_get("/healthz", handle_health)
    web_app.router.add_get("/metrics", handle_metrics)
    runner = web.AppRunner(web_app); await runner.setup()
    site = web.TCPSite(runner, CFG.bot.http_listen_host, CFG.bot.http_listen_port)
    await site.start()
    logger.info(f"HTTP server on {CFG.bot.http_listen_host}:{CFG.bot.http_listen_port} (healthz/metrics)")

# ======================
# Parsers & Endpoints (orderbook)
# ======================
EXCHANGE_OB = {
    "Binance":  ("https://api.binance.com/api/v3/depth?symbol={}&limit={}", "binance"),
    "KuCoin":   ("https://api.kucoin.com/api/v1/market/orderbook/level2_20?symbol={}", "kucoin"),
    "MEXC":     ("https://api.mexc.com/api/v3/depth?symbol={}&limit={}", "mexc"),
    "OKX":      ("https://www.okx.com/api/v5/market/books?instId={}&sz={}", "okx"),
    "Bybit":    ("https://api.bybit.com/v5/market/orderbook?category=spot&symbol={}", "bybit"),
}

async def parse_binance_ob(data: Dict[str, Any]):
    bids = [[float(p), float(q)] for p, q in data.get("bids", [])]
    asks = [[float(p), float(q)] for p, q in data.get("asks", [])]
    return bids, asks

async def parse_kucoin_ob(data: Dict[str, Any]):
    lvl = data.get("data", {})
    bids = [[float(p), float(q)] for p, q in lvl.get("bids", [])]
    asks = [[float(p), float(q)] for p, q in lvl.get("asks", [])]
    return bids, asks

async def parse_mexc_ob(data: Dict[str, Any]):
    d = data or {}
    bids = [[float(p), float(q)] for p, q in d.get("bids", [])] if "bids" in d else [[float(p), float(q)] for p, q in d.get("data", {}).get("bids", [])]
    asks = [[float(p), float(q)] for p, q in d.get("asks", [])] if "asks" in d else [[float(p), float(q)] for p, q in d.get("data", {}).get("asks", [])]
    bids.sort(key=lambda x: x[0], reverse=True)
    asks.sort(key=lambda x: x[0])
    return bids, asks

async def parse_okx_ob(data: Dict[str, Any]):
    # OKX: { "code":"0","data":[{"bids":[["price","sz",...],...],"asks":[["price","sz",...],...]}] }
    arr = data.get("data", [])
    if not arr:
        return [], []
    entry = arr[0] or {}
    bids = [[float(p), float(sz)] for p, sz, *rest in entry.get("bids", [])]
    asks = [[float(p), float(sz)] for p, sz, *rest in entry.get("asks", [])]
    bids.sort(key=lambda x: x[0], reverse=True)
    asks.sort(key=lambda x: x[0])
    return bids, asks

async def parse_bybit_ob(data: Dict[str, Any]):
    res = data.get("result", {}) or {}
    bids = [[float(p), float(s)] for p, s in res.get("b", [])]
    asks = [[float(p), float(s)] for p, s in res.get("a", [])]
    bids.sort(key=lambda x: x[0], reverse=True)
    asks.sort(key=lambda x: x[0])
    return bids, asks

PARSERS = {
    "binance": parse_binance_ob,
    "kucoin": parse_kucoin_ob,
    "mexc": parse_mexc_ob,
    "okx": parse_okx_ob,
    "bybit": parse_bybit_ob,
}

# ======================
# Fee realtime (autenticate) + cache
# ======================
FEE_CACHE: Dict[str, Dict[str, Any]] = {}  # {"Exchange": {"ts": ..., "maker": D, "taker": D}}
def _cache_ok(name: str, ttl_sec: int) -> bool:
    ent = FEE_CACHE.get(name)
    return bool(ent and (time.time() - ent["ts"] < ttl_sec))

def _put_cache(name: str, maker: D, taker: D):
    FEE_CACHE[name] = {"ts": time.time(), "maker": Dsafe(maker), "taker": Dsafe(taker)}

async def get_fees_fallback(exchange: str) -> Tuple[D, D]:
    f = CFG.commissioni.get(exchange)
    if not f:
        return D("0.001"), D("0.001")
    return f.acquisto, f.vendita

# --- Binance: GET /sapi/v1/asset/tradeFee (user_data signed)
def _binance_sign(secret: str, query: str) -> str:
    return hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()

async def fees_binance(session: aiohttp.ClientSession) -> Tuple[D, D]:
    if _cache_ok("Binance", CFG.bot.fee_cache_sec):
        e = FEE_CACHE["Binance"]; return e["maker"], e["taker"]
    key = os.getenv("BINANCE_KEY"); secret = os.getenv("BINANCE_SECRET")
    if not key or not secret:
        return await get_fees_fallback("Binance")
    ts = str(int(time.time()*1000))
    symbol = "BTCUSDT"
    query = f"symbol={symbol}&timestamp={ts}"
    sig = _binance_sign(secret, query)
    url = f"https://api.binance.com/sapi/v1/asset/tradeFee?{query}&signature={sig}"
    headers = {"X-MBX-APIKEY": key}
    async with session.get(url, headers=headers, timeout=10) as r:
        data = await r.json()
    if isinstance(data, list) and data:
        mk = Dsafe(data[0].get("makerCommission","0.001"))
        tk = Dsafe(data[0].get("takerCommission","0.001"))
        _put_cache("Binance", mk, tk); return mk, tk
    return await get_fees_fallback("Binance")

# --- Bybit v5: GET /v5/account/fee-rate (auth)
def _bybit_sign(secret: str, params: Dict[str, Any]) -> str:
    s = "&".join(f"{k}={params[k]}" for k in sorted(params))
    return hmac.new(secret.encode(), s.encode(), hashlib.sha256).hexdigest()

async def fees_bybit(session: aiohttp.ClientSession) -> Tuple[D, D]:
    if _cache_ok("Bybit", CFG.bot.fee_cache_sec):
        e = FEE_CACHE["Bybit"]; return e["maker"], e["taker"]
    key = os.getenv("BYBIT_KEY"); secret = os.getenv("BYBIT_SECRET")
    if not key or not secret:
        return await get_fees_fallback("Bybit")
    ts = str(int(time.time()*1000))
    params = {
        "api_key": key,
        "timestamp": ts,
        "recv_window": "5000",
        "category": "spot",
        "symbol": "BTCUSDT",
    }
    params["sign"] = _bybit_sign(secret, params)
    url = "https://api.bybit.com/v5/account/fee-rate"
    async with session.get(url, params=params, timeout=10) as r:
        data = await r.json()
    if data.get("retCode") == 0 and data.get("result"):
        res = data["result"]
        mk = Dsafe(res.get("makerFeeRate","0.001"))
        tk = Dsafe(res.get("takerFeeRate","0.001"))
        _put_cache("Bybit", mk, tk); return mk, tk
    return await get_fees_fallback("Bybit")

# --- KuCoin v2: GET /api/v1/trade-fees?symbols=BTC-USDT (auth)
def _kucoin_sign(secret: str, ts: str, method: str, path: str, query: str, body: str) -> str:
    payload = f"{ts}{method}{path}{query}{body}"
    mac = hmac.new(secret.encode(), payload.encode(), hashlib.sha256).digest()
    return base64.b64encode(mac).decode()

def _kucoin_passphrase(secret: str, passphrase: str) -> str:
    mac = hmac.new(secret.encode(), passphrase.encode(), hashlib.sha256).digest()
    return base64.b64encode(mac).decode()

async def fees_kucoin(session: aiohttp.ClientSession) -> Tuple[D, D]:
    if _cache_ok("KuCoin", CFG.bot.fee_cache_sec):
        e = FEE_CACHE["KuCoin"]; return e["maker"], e["taker"]
    key = os.getenv("KUCOIN_KEY"); secret = os.getenv("KUCOIN_SECRET"); p = os.getenv("KUCOIN_PASSPHRASE")
    if not key or not secret or not p:
        return await get_fees_fallback("KuCoin")
    base = "https://api.kucoin.com"
    path = "/api/v1/trade-fees"
    query = "?symbols=BTC-USDT"
    ts = str(int(time.time()*1000))
    sign = _kucoin_sign(secret, ts, "GET", path, query, "")
    pass_b64 = _kucoin_passphrase(secret, p)
    headers = {
        "KC-API-KEY": key,
        "KC-API-SIGN": sign,
        "KC-API-TIMESTAMP": ts,
        "KC-API-PASSPHRASE": pass_b64,
        "KC-API-KEY-VERSION": "2",
    }
    async with session.get(base+path+query, headers=headers, timeout=10) as r:
        data = await r.json()
    if data.get("code") == "200000" and data.get("data"):
        mk = Dsafe(data["data"][0].get("makerFeeRate","0.001"))
        tk = Dsafe(data["data"][0].get("takerFeeRate","0.001"))
        _put_cache("KuCoin", mk, tk); return mk, tk
    return await get_fees_fallback("KuCoin")

# --- MEXC fees (fallback per ora)
async def fees_mexc(session: aiohttp.ClientSession) -> Tuple[D, D]:
    if _cache_ok("MEXC", CFG.bot.fee_cache_sec):
        e = FEE_CACHE["MEXC"]; return e["maker"], e["taker"]
    # Se in futuro aggiungi un endpoint auth per fee, implementalo qui.
    mk, tk = await get_fees_fallback("MEXC")
    _put_cache("MEXC", mk, tk)
    return mk, tk

# --- OKX: GET /api/v5/account/trade-fee (auth)
def _okx_timestamp() -> str:
    # es: 2020-03-20T02:41:29.452Z
    return datetime.utcnow().replace(tzinfo=None).isoformat(timespec="milliseconds") + "Z"

def _okx_sign(secret: str, prehash: str) -> str:
    return base64.b64encode(hmac.new(secret.encode(), prehash.encode(), hashlib.sha256).digest()).decode()

async def fees_okx(session: aiohttp.ClientSession) -> Tuple[D, D]:
    if _cache_ok("OKX", CFG.bot.fee_cache_sec):
        e = FEE_CACHE["OKX"]; return e["maker"], e["taker"]
    key = os.getenv("OKX_KEY"); secret = os.getenv("OKX_SECRET"); passphrase = os.getenv("OKX_PASSPHRASE")
    if not key or not secret or not passphrase:
        return await get_fees_fallback("OKX")
    ts = _okx_timestamp()
    method = "GET"
    path = "/api/v5/account/trade-fee"
    query = "?instType=SPOT"
    prehash = f"{ts}{method}{path}{query}"
    sign = _okx_sign(secret, prehash)
    headers = {
        "OK-ACCESS-KEY": key,
        "OK-ACCESS-SIGN": sign,
        "OK-ACCESS-TIMESTAMP": ts,
        "OK-ACCESS-PASSPHRASE": passphrase,
    }
    url = f"https://www.okx.com{path}{query}"
    try:
        async with session.get(url, headers=headers, timeout=10) as r:
            data = await r.json()
        if isinstance(data, dict) and data.get("code") == "0" and data.get("data"):
            d0 = data["data"][0]
            mk = Dsafe(d0.get("maker", "0.0008"))
            tk = Dsafe(d0.get("taker", "0.001"))
            _put_cache("OKX", mk, tk)
            return mk, tk
    except Exception as e:
        METRICS["api_errors_total"] += 1
        logger.warning(f"fee realtime error OKX: {e}")
    return await get_fees_fallback("OKX")

async def get_fees(session: aiohttp.ClientSession, exchange: str) -> Tuple[D, D]:
    try:
        if exchange == "Binance":   return await fees_binance(session)
        if exchange == "Bybit":     return await fees_bybit(session)
        if exchange == "KuCoin":    return await fees_kucoin(session)
        if exchange == "MEXC":      return await fees_mexc(session)
        if exchange == "OKX":       return await fees_okx(session)
    except Exception as e:
        METRICS["api_errors_total"] += 1
        logger.warning(f"fee realtime error {exchange}: {e}")
    return await get_fees_fallback(exchange)

# ======================
# Concurrency tools
# ======================
HOST_SEMAPHORES: Dict[str, asyncio.Semaphore] = {}
def sem_for_host(url: str, limit: int) -> asyncio.Semaphore:
    host = re.findall(r"https?://([^/]+)/", url)
    key = host[0] if host else url
    if key not in HOST_SEMAPHORES:
        HOST_SEMAPHORES[key] = asyncio.Semaphore(limit)
    return HOST_SEMAPHORES[key]

# ======================
# Orderbook fetch (async)
# ======================
async def fetch_orderbook(session: aiohttp.ClientSession, exchange: str, symbol: str, bot: BotCfg):
    url_tmpl, parser_key = EXCHANGE_OB[exchange]
    url = url_tmpl.format(symbol, bot.max_orderbook_levels) if "{}" in url_tmpl else url_tmpl.format(symbol)
    sem = sem_for_host(url, bot.threads_limit_per_host)
    await asyncio.sleep(random.uniform(0, 0.4))  # jitter aumentato
    t0 = time.perf_counter()
    async with sem:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                data = await resp.json()
        except Exception as e:
            METRICS["api_errors_total"] += 1
            raise RuntimeError(f"{exchange} fetch failed: {e}") from e
    latency_ms = int((time.perf_counter() - t0) * 1000)
    METRICS["latency_ms_sum"] += latency_ms; METRICS["latency_ms_count"] += 1
    if latency_ms > bot.latency_warn_ms:
        logger.warning(f"Latenza alta {latency_ms} ms su {exchange} {symbol}")
    bids, asks = await PARSERS[parser_key](data)
    bids, asks = bids[:bot.max_orderbook_levels], asks[:bot.max_orderbook_levels]
    if not bids or not asks:
        raise RuntimeError(f"{exchange} empty book for {symbol}")
    return bids, asks, latency_ms

# ======================
# VWAP & sizing
# ======================
def vwap_buy_with_capital(asks: List[List[float]], capital: D):
    remaining = capital
    qty = D("0"); spent = D("0"); legs = []
    for p, s in asks:
        pD, sD = Dsafe(p), Dsafe(s)
        if pD <= 0 or sD <= 0:
            continue
        max_qty = remaining / pD
        take = min(max_qty, sD)
        if take <= 0:
            continue
        cost = take * pD
        qty += take; spent += cost; remaining -= cost
        legs.append((pD, take))
        if remaining <= 0:
            break
    if qty <= 0:
        return None, None, []
    return qty, (spent / qty), legs

def vwap_sell_for_qty(bids: List[List[float]], qty: D):
    remaining = qty
    proceeds = D("0"); filled = D("0"); legs = []
    for p, s in bids:
        pD, sD = Dsafe(p), Dsafe(s)
        if pD <= 0 or sD <= 0:
            continue
        take = min(remaining, sD)
        if take <= 0:
            continue
        proceeds += take * pD; filled += take
        legs.append((pD, take))
        remaining -= take
        if remaining <= 0:
            break
    if filled <= 0:
        return None, None, []
    return proceeds, (proceeds / filled), legs

def compute_slippage_bps(entry_px: D, vwap_px: D) -> D:
    if entry_px <= 0:
        return D("1e9")
    return abs(vwap_px - entry_px) / entry_px

def dynamic_position_size(asks: List[List[float]], bids: List[List[float]], best_ask: D, best_bid: D, bot: BotCfg) -> D:
    if len(asks) < bot.min_depth_levels or len(bids) < bot.min_depth_levels:
        return D("0")
    lo, hi = D("0"), bot.max_notional_per_trade_usdt
    for _ in range(18):
        mid = (lo + hi) / 2
        if mid <= 0:
            lo = mid; continue
        qty, vwap_buy, _ = vwap_buy_with_capital(asks, mid)
        if qty is None:
            hi = mid; continue
        slip_bps = compute_slippage_bps(best_ask, vwap_buy)
        if slip_bps <= bot.max_slippage_bps:
            lo = mid
        else:
            hi = mid
    return lo.quantize(D("0.0001"))

# ======================
# Guardrail
# ======================
def spread_ok(best_ask: D, best_bid: D, bot: BotCfg) -> bool:
    if best_ask <= 0 or best_bid <= 0 or best_bid <= best_ask:
        return False
    return (best_bid - best_ask) / best_ask >= bot.spread_min_bps

def fee_guard_ok(f_ass_buy: D, f_ass_sell: D, bot: BotCfg, f_api_buy: Optional[D]=None, f_api_sell: Optional[D]=None) -> bool:
    if f_api_buy is None or f_api_sell is None:
        return True
    return not (f_api_buy > f_ass_buy + bot.fee_safety_bps or f_api_sell > f_ass_sell + bot.fee_safety_bps)

# ======================
# Circuit breaker
# ======================
CB_STATE: Dict[str, Dict[str, Any]] = {}
def _init_cb_state():
    global CB_STATE
    CB_STATE = {asset: {"streak": 0, "until": 0.0} for asset in CFG.symbols}

def cb_blocked(asset: str) -> bool:
    return time.time() < CB_STATE[asset]["until"]

def cb_note(asset: str, success: bool, bot: BotCfg):
    if success:
        CB_STATE[asset]["streak"] = 0; CB_STATE[asset]["until"] = 0.0
    else:
        CB_STATE[asset]["streak"] += 1
        if CB_STATE[asset]["streak"] >= bot.cb_max_negative_streak:
            CB_STATE[asset]["until"] = time.time() + bot.cb_cooldown_sec
            METRICS["cb_tripped_total"] += 1
            logger.warning(f"[CB] {asset} cooldown {bot.cb_cooldown_sec}s")

# ======================
# CSV append
# ======================
def append_csv(path: str, fields: List[Any]):
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(",".join(str(x) for x in fields) + "\n")
    except Exception as e:
        logger.error(f"CSV write error: {e}")

# ======================
# Core scan
# ======================
async def scan_once(session: aiohttp.ClientSession):
    METRICS["loop_runs_total"] += 1
    bot = CFG.bot
    logger.info("— Scan (async VWAP + fee realtime) —")

    # opzionale: stampa tabellare spread migliori
    spreads_rows = []

    for asset, by_ex in CFG.symbols.items():
        if cb_blocked(asset):
            logger.info(f"{asset}: cooldown attivo"); continue

        # fetch parallelo book (mappa tasks per exchange)
        books: Dict[str, Dict[str, Any]] = {}
        tasks = {exg: asyncio.create_task(fetch_orderbook(session, exg, sym, bot))
                 for exg, sym in by_ex.items() if exg in EXCHANGE_OB}
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        for (exg, sym), res in zip(tasks.items(), results):
            if isinstance(res, Exception):
                logger.warning(f"{asset}: orderbook error {exg}: {res}")
                continue
            bids, asks, lat = res
            books[exg] = {"symbol": sym, "bids": bids, "asks": asks, "latency_ms": lat}

        if len(books) < 2:
            cb_note(asset, success=False, bot=bot)
            logger.warning(f"{asset}: insufficient books")
            continue

        best = {exg: {"ask": Dsafe(b["asks"][0][0]), "bid": Dsafe(b["bids"][0][0])} for exg, b in books.items()}
        buy_ex = min(best.items(), key=lambda kv: kv[1]["ask"])[0]
        sell_ex = max(best.items(), key=lambda kv: kv[1]["bid"])[0]
        if buy_ex == sell_ex:
            cb_note(asset, success=False, bot=bot); continue

        best_ask = best[buy_ex]["ask"]; best_bid = best[sell_ex]["bid"]
        if CFG.bot.print_spreads:
            sp = (best_bid - best_ask) / best_ask if best_ask > 0 else D("0")
            spreads_rows.append([asset, buy_ex, f"{best_ask:.6f}", sell_ex, f"{best_bid:.6f}", f"{(sp*100):.4f}%"])

        if not spread_ok(best_ask, best_bid, bot):
            cb_note(asset, success=False, bot=bot)
            logger.info(f"{asset}: spread insuff ({(((best_bid-best_ask)/best_ask)*100 if best_ask>0 else D(0)):.4f}%)")
            continue

        # Fee assunte + fee realtime (se possibili)
        f_buy_ass, f_sell_ass = (await get_fees_fallback(buy_ex))
        f_buy_api_mk, f_buy_api_tk = await get_fees(session, buy_ex)   # maker/taker
        f_sell_api_mk, f_sell_api_tk = await get_fees(session, sell_ex)

        # per semplicità: compra = taker sul lato ask; vendi = taker sul lato bid.
        fee_buy = f_buy_api_tk if f_buy_api_tk is not None else f_buy_ass
        fee_sell = f_sell_api_tk if f_sell_api_tk is not None else f_sell_ass

        if not fee_guard_ok(f_buy_ass, f_sell_ass, bot, f_buy_api_tk, f_sell_api_tk):
            cb_note(asset, success=False, bot=bot)
            logger.warning(f"{asset}: fee guard attiva ({buy_ex}/{sell_ex})")
            continue

        asks_buy = books[buy_ex]["asks"]; bids_sell = books[sell_ex]["bids"]
        notional = dynamic_position_size(asks_buy, bids_sell, best_ask, best_bid, bot)
        if notional <= 0:
            cb_note(asset, success=False, bot=bot)
            logger.info(f"{asset}: sizing=0 (slippage/depth)"); continue
        notional = min(notional, bot.capital_usdt)

        # Profitto stimato VWAP
        qty, pbuy, legs_buy = vwap_buy_with_capital(asks_buy, notional)
        if qty is None or pbuy is None:
            cb_note(asset, success=False, bot=bot); logger.info(f"{asset}: VWAP buy impossibile"); continue
        qty_net = qty * (D(1) - fee_buy)

        proceeds, psell, legs_sell = vwap_sell_for_qty(bids_sell, qty_net)
        if proceeds is None or psell is None:
            cb_note(asset, success=False, bot=bot); logger.info(f"{asset}: VWAP sell impossibile"); continue

        proceeds_net = proceeds * (D(1) - fee_sell)
        network_fee = D("0") if bot.prefunding_mode else CFG.network_fee_asset.get(asset, bot.network_fee_usdt)
        gain = proceeds_net - notional - network_fee
        gain_adj = gain - (notional * bot.risk_cushion_bps)

        if gain_adj >= bot.gain_min_usdt:
            METRICS["opportunities_found_total"] += 1
            logger.info(f"{asset}: BUY {buy_ex}@~{pbuy:.6f} SELL {sell_ex}@~{psell:.6f} notional={notional} gain_est={gain_adj:.4f}")
            log_json("signal", asset=asset, buy_ex=buy_ex, sell_ex=sell_ex,
                     notional=str(notional), pbuy=str(pbuy), psell=str(psell),
                     gain=str(gain), gain_adj=str(gain_adj),
                     fees={"buy_taker": str(fee_buy), "sell_taker": str(fee_sell)})

            # stop/limit runtime
            await asyncio.sleep(bot.order_hold_sec)
            try:
                reb_buy = await fetch_orderbook(session, buy_ex, books[buy_ex]["symbol"], bot)
                reb_sell = await fetch_orderbook(session, sell_ex, books[sell_ex]["symbol"], bot)
            except Exception as e:
                cb_note(asset, success=False, bot=bot); logger.warning(f"{asset}: refetch error {e}"); continue

            asks2 = reb_buy[1]; bids2 = reb_sell[0]
            qty2, pbuy2, _ = vwap_buy_with_capital(asks2, notional)
            if qty2 is None or pbuy2 is None:
                cb_note(asset, success=False, bot=bot); continue
            qty2_net = qty2 * (D(1) - fee_buy)
            proceeds2, psell2, _ = vwap_sell_for_qty(bids2, qty2_net)
            if proceeds2 is None or psell2 is None:
                cb_note(asset, success=False, bot=bot); continue
            gain_rt = (proceeds2 * (D(1) - fee_sell)) - notional - network_fee

            # stop dinamico vs iniziale
            denom = abs(gain) + D("1e-12")
            if (gain_rt - gain) / denom < -bot.drop_threshold_rel:
                cb_note(asset, success=False, bot=bot)
                logger.info(f"{asset}: stop dinamico → abort (gain_rt={gain_rt:.4f})")
                continue

            if gain_rt >= bot.gain_rt_min_usdt:
                METRICS["trades_rt_ok_total"] += 1
                logger.info(f"{asset}: ESECUZIONE simulata OK. gain_rt={gain_rt:.4f}")
                append_csv(CFG.bot.log_csv, [
                    datetime.now().isoformat(), str(notional), asset,
                    f"{pbuy:.10f}", buy_ex, f"{psell2:.10f}", sell_ex,
                    str(fee_buy), str(fee_sell), f"{gain_rt:.4f}"
                ])
                cb_note(asset, success=True, bot=bot)
                log_json("trade_ok", asset=asset, notional=str(notional),
                         pbuy=str(pbuy2), psell=str(psell2), gain_rt=str(gain_rt))
            else:
                cb_note(asset, success=False, bot=bot)
                logger.info(f"{asset}: opportunità degradata sotto soglia runtime ({gain_rt:.4f})")
        else:
            cb_note(asset, success=False, bot=bot)
            logger.info(f"{asset}: no-op (gain_adj={gain_adj:.4f} < {bot.gain_min_usdt})")

    # Stampa tabellare spread se richiesto
    if CFG.bot.print_spreads and spreads_rows:
        try:
            widths = [max(len(str(row[i])) for row in spreads_rows + [["ASSET","BUY_EX","ASK","SELL_EX","BID","SPREAD%"]]) for i in range(6)]
            header = ["ASSET","BUY_EX","ASK","SELL_EX","BID","SPREAD%"]
            line = " | ".join(h.ljust(widths[i]) for i, h in enumerate(header))
            print("\n" + line)
            print("-" * (sum(widths) + 3*5))
            for r in spreads_rows:
                print(" | ".join(str(r[i]).ljust(widths[i]) for i in range(6)))
            print()
        except Exception:
            pass

# ======================
# Main loop
# ======================
async def main():
    _init_cb_state()
    logger.info("== avvio arbitraggio async v7 (OKX+MEXC) ==")
    timeout = aiohttp.ClientTimeout(total=12)
    conn = aiohttp.TCPConnector(limit=CFG.bot.global_concurrency, ssl=False)
    async with aiohttp.ClientSession(timeout=timeout, connector=conn) as session:
        await start_http_server()
        while True:
            try:
                await scan_once(session)
                # GC ogni 10 cicli
                if METRICS["loop_runs_total"] % 10 == 0:
                    gc.collect()
            except Exception as e:
                METRICS["api_errors_total"] += 1
                logger.error(f"Errore imprevisto: {e}")
                log_json("error", msg=str(e))
            await asyncio.sleep(CFG.bot.sleep_scan_sec)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrotto dall’utente.")
