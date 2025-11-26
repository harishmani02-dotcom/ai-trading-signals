#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Finspark AI – v6.1 FINAL PRODUCTION SINGLE FILE ENGINE
- Builds on v6.0 with 8 fixes (delete old signals, volume spike, ATR fallback,
  improved 429 backoff, signal limiter, risk log, cleaned output, speed tweaks)
"""
import os
import sys
import asyncio
import logging
import time
from datetime import datetime, time as dt_time
import pytz
import aiohttp
import pandas as pd
import numpy as np
from supabase import create_client
import random
from typing import Tuple, Optional

# ------------------ CONFIGURATION ------------------
SUPABASE_URL = os.getenv('SUPABASE_URL', 'YOUR_SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY', 'YOUR_SUPABASE_KEY')

# Tables (non-disruptive)
DB_TABLE_SIGNALS = "intraday_signals_v6_final"
DB_TABLE_ERRORS = "engine_errors_v6_final"
DB_TABLE_RISK = "intraday_risk_log_v6"   # new risk log table

TIME_FRAMES = ['5m', '15m', '30m', '1h']
DEFAULT_PERIOD = "7d"

IST = pytz.timezone("Asia/Kolkata")
MARKET_OPEN = dt_time(9, 15)
MARKET_CLOSE = dt_time(15, 30)

STOCK_LIST = [
    'RELIANCE.NS', 'TCS.NS', 'INFY.NS', 'HDFCBANK.NS', 'ICICIBANK.NS',
    'KOTAKBANK.NS', 'WIPRO.NS', 'HCLTECH.NS', 'LT.NS', 'ADANIENT.NS'
]

TICKER_MAP = {
    'CADILAHC.NS': 'ZYDUSLIFE.NS',
    'LTI.NS': 'LTIM.NS',
    'MINDTREE.NS': 'LTIM.NS',
    'ADANIGAS.NS': 'ATGL.NS',
}

CONCURRENCY_LIMIT = int(os.getenv("CONCURRENCY_LIMIT", "20"))
FETCH_TIMEOUT = int(os.getenv("FETCH_TIMEOUT", "30"))
RETRY_COUNT = int(os.getenv("RETRY_COUNT", "4"))  # allow one extra retry for backoff
SEMAPHORE = asyncio.Semaphore(CONCURRENCY_LIMIT)

RSI_PERIOD = 14
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

BB_PERIOD = 20
VOL_PERIOD = 20

SL_ATR_MULT = 1.2
TP_ATR_MULT = 2.4

MIN_CANDLE_COUNT = 100
MAX_CIRCUIT_PCT = 0.18

# NEW: Volume spike multiplier and max signals per run
VOLUME_SPIKE_MULT = 1.8
MAX_SIGNALS_PER_RUN = int(os.getenv("MAX_SIGNALS_PER_RUN", "40"))

# Backoff jitter bounds (seconds)
BACKOFF_BASE = 2.0
BACKOFF_JITTER = 0.3

# ------------------ LOGGING ------------------
LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("FinsparkV6.1")

# ------------------ UTILITIES ------------------
def get_safe_ticker(t: str) -> str:
    return TICKER_MAP.get(t.upper(), t.upper())

def safe_iloc(series: pd.Series, idx: int, fallback=np.nan):
    if not isinstance(series, pd.Series) or series.empty:
        return fallback
    try:
        # Negative and positive index handling
        if idx < 0 and abs(idx) > len(series):
            return fallback
        if idx >= 0 and idx >= len(series):
            return fallback
        val = series.iloc[idx]
        return val if pd.notna(val) else fallback
    except Exception:
        return fallback

def is_market_open() -> Tuple[bool, str]:
    now = datetime.now(IST)
    if now.weekday() > 4:
        return False, "Weekend"
    if (now.time() >= MARKET_OPEN) and (now.time() <= MARKET_CLOSE):
        return True, "Open"
    return False, "Closed"

# ------------------ FETCHING (with improved backoff) ------------------
async def fetch_data(ticker: str, session: aiohttp.ClientSession, interval: str):
    pretty = ticker.replace(".NS", "")
    mapped = get_safe_ticker(ticker)
    url = f"https://query2.finance.yahoo.com/v8/finance/chart/{mapped}"
    params = {"range": DEFAULT_PERIOD, "interval": interval, "includePrePost": False}
    key = (ticker, interval)

    for attempt in range(RETRY_COUNT):
        async with SEMAPHORE:
            try:
                resp = await asyncio.wait_for(session.get(url, params=params), timeout=FETCH_TIMEOUT)
                async with resp:
                    if resp.status == 429:
                        # raise to go into except and backoff
                        raise aiohttp.ClientResponseError(resp.request_info, resp.history, status=429, message="RateLimit")
                    resp.raise_for_status()
                    raw = await resp.json()
                result = raw.get("chart", {}).get("result", [])
                if not result:
                    return key, None, None
                meta = result[0].get('meta', {})
                q = result[0]['indicators'].get('quote', [{}])[0]
                ts = result[0].get('timestamp', [])
                if not ts or not q.get('close'):
                    return key, None, None
                df = pd.DataFrame({
                    "Datetime": [datetime.fromtimestamp(x, IST) for x in ts],
                    "Open": q.get('open', []),
                    "High": q.get('high', []),
                    "Low": q.get('low', []),
                    "Close": q.get('close', []),
                    "Volume": q.get('volume', [])
                }).set_index("Datetime")
                prev_close = meta.get("previousClose")
                return key, df, prev_close
            except aiohttp.ClientResponseError as cre:
                # specific handling
                if getattr(cre, "status", None) == 429:
                    wait = BACKOFF_BASE * (2 ** attempt) + random.uniform(0, BACKOFF_JITTER)
                    logger.warning(f"{pretty} [{interval}] 429 — backoff {wait:.1f}s (attempt {attempt+1}/{RETRY_COUNT})")
                    await asyncio.sleep(wait)
                    continue
                else:
                    logger.warning(f"{pretty} [{interval}] HTTP error {getattr(cre,'status',cre)} (attempt {attempt+1})")
            except asyncio.TimeoutError:
                wait = BACKOFF_BASE * (2 ** attempt) + random.uniform(0, BACKOFF_JITTER)
                logger.warning(f"{pretty} [{interval}] Timeout — backoff {wait:.1f}s (attempt {attempt+1})")
                await asyncio.sleep(wait)
            except Exception as e:
                # generic
                wait = BACKOFF_BASE * (2 ** attempt) + random.uniform(0, BACKOFF_JITTER)
                logger.debug(f"{pretty} [{interval}] fetch error {e} — backoff {wait:.1f}s (attempt {attempt+1})")
                await asyncio.sleep(wait)
    # final fail
    return key, None, None

# ------------------ INDICATORS ------------------
def indicators(df: pd.DataFrame) -> Optional[dict]:
    if df is None or len(df) < MIN_CANDLE_COUNT:
        return None
    close = df["Close"]
    high = df["High"]
    low = df["Low"]
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)
    atr = tr.ewm(com=13, adjust=False).mean()
    ema_20 = close.ewm(span=20).mean()
    ema_50 = close.ewm(span=50).mean()
    ema20_start = safe_iloc(ema_20, -5)
    ema20_end = safe_iloc(ema_20, -1)
    slope = (ema20_end - ema20_start) / 5 if ema20_start is not np.nan else 0
    angle = np.arctan(slope) * (180.0 / np.pi)
    delta = close.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.ewm(com=13, adjust=False).mean()
    avg_loss = loss.ewm(com=13, adjust=False).mean().mask(lambda x: x == 0, 1e-10)
    rsi = 100 - (100 / (1 + (avg_gain / avg_loss)))
    ema_fast = close.ewm(span=MACD_FAST).mean()
    ema_slow = close.ewm(span=MACD_SLOW).mean()
    macd = ema_fast - ema_slow
    macd_signal = macd.ewm(span=MACD_SIGNAL).mean()
    macd_hist = macd - macd_signal
    sma = close.rolling(BB_PERIOD).mean()
    std = close.rolling(BB_PERIOD).std()
    bb_upper = sma + 2 * std
    bb_lower = sma - 2 * std
    bb_width = ((bb_upper - bb_lower) / sma * 100).iloc[-1] if not sma.empty else np.nan
    vol_avg_series = df['Volume'].rolling(VOL_PERIOD).mean()
    vol_avg = safe_iloc(vol_avg_series, -1, df['Volume'].mean())
    # ATR fallback
    atr_val = safe_iloc(atr, -1, np.nan)
    if pd.isna(atr_val) or atr_val <= 0:
        last_close = safe_iloc(close, -1, None)
        atr_val = (last_close * 0.005) if last_close and last_close > 0 else 1.0
    return {
        "rsi": safe_iloc(rsi, -1),
        "atr": atr_val,
        "ema20": ema20_end,
        "ema50": safe_iloc(ema_50, -1),
        "angle": angle,
        "macd_hist": safe_iloc(macd_hist, -1),
        "bb_upper": safe_iloc(bb_upper, -1),
        "bb_lower": safe_iloc(bb_lower, -1),
        "bb_width": bb_width,
        "vol_avg": vol_avg
    }

# ------------------ TREND ------------------
def trend_logic(ind: dict, close: float) -> str:
    if pd.isna(ind.get("ema20")) or pd.isna(ind.get("ema50")):
        return "Sideways"
    if ind.get("bb_width", 9999) < 1.5:
        return "Squeeze"
    if ind["ema20"] > ind["ema50"] and close > ind["ema20"] and ind["angle"] > 5:
        return "Uptrend"
    if ind["ema20"] < ind["ema50"] and close < ind["ema20"] and ind["angle"] < -5:
        return "Downtrend"
    return "Sideways"

# ------------------ SIGNAL LOGIC (with volume spike & ATR fallback) ------------------
def generate_signal(df: pd.DataFrame, prev_close: Optional[float], symbol: str, interval: str) -> Optional[dict]:
    close = safe_iloc(df.get("Close"), -1)
    open_ = safe_iloc(df.get("Open"), -1)
    high = safe_iloc(df.get("High"), -1)
    low = safe_iloc(df.get("Low"), -1)
    volume = safe_iloc(df.get("Volume"), -1)
    if pd.isna(close) or pd.isna(open_):
        return None
    if prev_close and abs(close - prev_close) / prev_close > MAX_CIRCUIT_PCT:
        # log as risk
        return None
    ind = indicators(df)
    if ind is None:
        return None
    votes = []
    reasons = []
    tr = trend_logic(ind, close)
    if tr != "Sideways":
        reasons.append(tr)
    # RSI
    rsi_val = ind.get("rsi", 50)
    if rsi_val < 30:
        votes.append("Buy"); reasons.append(f"RSI {rsi_val:.1f}")
    elif rsi_val > 70:
        votes.append("Sell"); reasons.append(f"RSI {rsi_val:.1f}")
    # MACD histogram
    macdh = ind.get("macd_hist", 0.0)
    if macdh > 0:
        votes.append("Buy"); reasons.append("MACD+")
    elif macdh < 0:
        votes.append("Sell"); reasons.append("MACD-")
    # Bollinger
    if close < ind.get("bb_lower", -1e12):
        votes.append("Buy"); reasons.append("BB lower")
    elif close > ind.get("bb_upper", 1e12):
        votes.append("Sell"); reasons.append("BB upper")
    # Volume spike rule (NEW)
    vol_avg = ind.get("vol_avg", 0)
    if volume and vol_avg and volume > vol_avg * VOLUME_SPIKE_MULT:
        # correlate direction with candle
        if close > open_:
            votes.append("Buy"); reasons.append("Vol spike Bull")
        elif close < open_:
            votes.append("Sell"); reasons.append("Vol spike Bear")
    buy = votes.count("Buy")
    sell = votes.count("Sell")
    signal = "Hold"
    base_conf = 40
    if buy > sell and buy >= 2:
        signal = "Buy"; base_conf += buy * 5
    elif sell > buy and sell >= 2:
        signal = "Sell"; base_conf += sell * 5
    conf = min(base_conf + np.log1p(len(reasons)) * 10, 95)
    # ATR fallback already handled in indicators
    atr = ind.get("atr", max(close * 0.005, 1.0))
    stop_loss = target = rr = None
    if signal != "Hold" and atr and atr > 0:
        risk = atr * SL_ATR_MULT
        reward = atr * TP_ATR_MULT
        rr = round(reward / risk, 2) if risk > 0 else None
        if signal == "Buy":
            stop_loss = round(close - risk, 2)
            target = round(close + reward, 2)
        else:
            stop_loss = round(close + risk, 2)
            target = round(close - reward, 2)
    now = datetime.now(IST)
    return {
        "symbol": symbol.replace(".NS", ""),
        "interval": interval,
        "signal": signal,
        "confidence": round(conf, 1),
        "close_price": round(close, 2),
        "stop_loss": stop_loss,
        "target": target,
        "risk_reward": rr,
        "trend": tr,
        "signal_date": now.date().isoformat(),
        "signal_time": now.strftime("%H:%M:%S"),
        "reasons": ", ".join(reasons)
    }

# ------------------ SUPABASE HELPERS ------------------
def log_error(supabase, err_type: str, msg: str, stock: str = "N/A", interval: str = "N/A"):
    try:
        supabase.table(DB_TABLE_ERRORS).insert({
            "timestamp": datetime.now(IST).isoformat(),
            "error_type": err_type,
            "message": msg,
            "stock": stock,
            "interval": interval
        }).execute()
    except Exception:
        logger.debug("Failed to log error to DB (non-critical).")

def log_risk(supabase, event: str, details: str, stock: str = "N/A", interval: str = "N/A"):
    try:
        supabase.table(DB_TABLE_RISK).insert({
            "timestamp": datetime.now(IST).isoformat(),
            "event": event,
            "details": details,
            "stock": stock,
            "interval": interval
        }).execute()
    except Exception:
        logger.debug("Failed to log risk event to DB (non-critical).")

async def upload_signals(supabase, data: list):
    try:
        if not data:
            return 0
        res = supabase.table(DB_TABLE_SIGNALS).insert(data).execute()
        return len(res.data) if hasattr(res, "data") and isinstance(res.data, list) else len(data)
    except Exception as e:
        log_error(supabase, "UploadError", str(e))
        return 0

def delete_old_signals_once(supabase):
    try:
        today_iso = datetime.now(IST).date().isoformat()
        supabase.table(DB_TABLE_SIGNALS).delete().eq("signal_date", today_iso).execute()
        logger.info("Old signals for today deleted (one-time).")
    except Exception as e:
        logger.warning(f"Failed to delete old signals: {e}")

# ------------------ MAIN ENGINE (async processing + early cancel) ------------------
async def run_engine(supabase):
    start_time = time.time()
    market_ok, status = is_market_open()
    if not market_ok:
        logger.info("Market is closed. Exiting run.")
        return
    # delete today's old signals (once)
    delete_old_signals_once(supabase)
    # create client session
    timeout = aiohttp.ClientTimeout(total=FETCH_TIMEOUT + 5)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # schedule tasks
        tasks = [asyncio.create_task(fetch_data(stock, session, tf)) for stock in STOCK_LIST for tf in TIME_FRAMES]
        total_tasks = len(tasks)
        logger.info(f"Fetching {total_tasks} tasks with concurrency {CONCURRENCY_LIMIT}...")
        signals = []
        # process as they complete (speed & early-stop)
        try:
            for coro in asyncio.as_completed(tasks):
                key, df, prev = await coro
                stock, interval = key
                if df is None or prev is None:
                    log_error(supabase, "FetchFail", "No data or parse fail", stock, interval)
                else:
                    s = generate_signal(df, prev, stock, interval)
                    if s:
                        signals.append(s)
                        # risk logging for high impact signals (example: confidence > 85 or huge move)
                        if s.get("confidence", 0) >= 85 or (s.get("risk_reward") and s["risk_reward"] < 0.5):
                            log_risk(supabase, "HighImpactSignal", str(s), stock, interval)
                # early stop if reached max signals
                if len(signals) >= MAX_SIGNALS_PER_RUN:
                    logger.info(f"Reached max signals ({MAX_SIGNALS_PER_RUN}). Cancelling remaining fetch tasks.")
                    # cancel remaining tasks
                    for t in tasks:
                        if not t.done():
                            t.cancel()
                    break
        except asyncio.CancelledError:
            logger.info("Remaining tasks cancelled.")
        except Exception as e:
            logger.error(f"Engine run error: {e}")
            log_error(supabase, "EngineError", str(e))
        # upload collected signals
        uploaded = await upload_signals(supabase, signals)
        elapsed = time.time() - start_time
        logger.info(f"Run complete: generated {len(signals)} signals, uploaded {uploaded}. Time: {elapsed:.1f}s")

# ------------------ ENTRY POINT ------------------
def main():
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        # quick health check (non-destructive)
        try:
            supabase.table(DB_TABLE_ERRORS).select('*').limit(1).execute()
        except Exception:
            logger.info("DB health check failed or tables missing — continuing (non-destructive).")
        asyncio.run(run_engine(supabase))
    except Exception as e:
        logger.critical(f"Fatal: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
