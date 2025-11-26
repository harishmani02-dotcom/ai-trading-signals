#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Finspark AI — v6.2.1 HOTFIX (Production Ready)
- Fixed Yahoo Finance API boolean parameter issue
- All v6.2 improvements + critical bug fix
- Ready for immediate deployment
"""
import os
import sys
import asyncio
import logging
import time
import json
import random
from datetime import datetime, time as dt_time
import pytz
import aiohttp
import pandas as pd
import numpy as np
from supabase import create_client

# ---------------- CONFIG (ENV knobs)
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
DB_TABLE_SIGNALS = os.getenv("DB_TABLE_SIGNALS", "signals")

# Behavior flags
CLEAR_TODAY = os.getenv("CLEAR_TODAY", "false").lower() in ("1", "true", "yes")
MAX_SIGNALS = int(os.getenv("MAX_SIGNALS", "150"))
VOL_SPIKE_MULT = float(os.getenv("VOL_SPIKE_MULT", "3.0"))
MIN_CONF_TO_UPLOAD = float(os.getenv("MIN_CONF_TO_UPLOAD", "0"))

# Timeframes & Tickers
TIME_FRAMES = os.getenv("TIME_FRAMES", "5m,15m,30m,1h").split(",")
DEFAULT_PERIOD = os.getenv("DEFAULT_PERIOD", "7d")
IST = pytz.timezone("Asia/Kolkata")
MARKET_OPEN = dt_time(00, 15)
MARKET_CLOSE = dt_time(15, 30)

STOCK_LIST = os.getenv("STOCK_LIST", "RELIANCE.NS,TCS.NS,INFY.NS,HDFCBANK.NS,ICICIBANK.NS").split(",")
TICKER_MAP = {}

# Network & concurrency
CPU_COUNT = max(1, (os.cpu_count() or 2))
CONCURRENCY_LIMIT = int(os.getenv("CONCURRENCY_LIMIT", str(min(40, CPU_COUNT * 4))))
FETCH_TIMEOUT = int(os.getenv("FETCH_TIMEOUT", "30"))
RETRY_COUNT = int(os.getenv("RETRY_COUNT", "3"))

# Strategy params
RSI_PERIOD = 14
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
BB_PERIOD = 20
VOL_PERIOD = 20
SL_ATR_MULT = float(os.getenv("SL_ATR_MULT", "1.2"))
TP_ATR_MULT = float(os.getenv("TP_ATR_MULT", "2.4"))
MIN_CANDLE_COUNT = int(os.getenv("MIN_CANDLE_COUNT", "100"))
MAX_CIRCUIT_PCT = float(os.getenv("MAX_CIRCUIT_PCT", "0.18"))

# Logging - define START_TS at top (stylistic improvement)
START_TS = time.time()
LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("FinsparkV6.2.1")

def json_log(event_type: str, payload: dict):
    """Structured JSON logging - minimal payloads only"""
    out = {"ts": datetime.now(IST).isoformat(), "event": event_type}
    out.update(payload)
    logger.info(json.dumps(out, default=str))

# ---------------- SAFE HELPERS ----------------
def get_safe_ticker(t):
    return TICKER_MAP.get(t.upper(), t.upper())

def safe_iloc(series: pd.Series, idx: int, fallback=np.nan):
    """Robust iloc supporting negative indices and NaN-safe returns"""
    if not isinstance(series, pd.Series) or series.empty:
        return fallback
    try:
        if idx < 0 and abs(idx) > len(series):
            return fallback
        if idx >= 0 and idx >= len(series):
            return fallback
        val = series.iloc[idx]
        return val if pd.notna(val) else fallback
    except (IndexError, KeyError, TypeError):
        return fallback

def is_market_open():
    now = datetime.now(IST)
    if now.weekday() > 4:
        return False, "Weekend"
    is_open = (MARKET_OPEN <= now.time() < MARKET_CLOSE)
    return is_open, "Open" if is_open else "Closed"

# ---------------- BACKOFF + JITTER ----------------
async def backoff_sleep(attempt: int, base: float = 1.0, cap: float = 30.0):
    delay = min(cap, base * (2 ** attempt))
    jitter = delay * (0.5 + random.random() * 0.5)
    await asyncio.sleep(jitter)

# ---------------- FETCH (Yahoo V8 endpoint) ----------------
SEM = asyncio.Semaphore(CONCURRENCY_LIMIT)

async def fetch_data(session: aiohttp.ClientSession, ticker: str, interval: str):
    pretty = ticker.replace(".NS", "")
    mapped = get_safe_ticker(ticker)
    url = f"https://query2.finance.yahoo.com/v8/finance/chart/{mapped}"
    
    # CRITICAL FIX v6.2.1: Yahoo API requires string "false", not boolean False
    params = {
        "range": DEFAULT_PERIOD,
        "interval": interval,
        "includePrePost": "false"  # Fixed: was False (boolean)
    }

    key = (ticker, interval)
    for attempt in range(RETRY_COUNT):
        async with SEM:
            try:
                resp = await asyncio.wait_for(session.get(url, params=params), timeout=FETCH_TIMEOUT)
                async with resp:
                    if resp.status == 429:
                        json_log("rate_limit", {"ticker": pretty, "interval": interval, "attempt": attempt + 1})
                        raise aiohttp.ClientResponseError(resp.request_info, resp.history, status=429)
                    resp.raise_for_status()
                    raw = await resp.json()
                chart = raw.get("chart", {}).get("result", [])
                if not chart:
                    return key, None, None
                meta = chart[0].get("meta", {})
                quote = chart[0].get("indicators", {}).get("quote", [{}])[0]
                timestamps = chart[0].get("timestamp", [])
                if not timestamps or not quote.get("close"):
                    return key, None, None
                df = pd.DataFrame({
                    "Datetime": [datetime.fromtimestamp(ts, IST) for ts in timestamps],
                    "Open": quote.get("open", []),
                    "High": quote.get("high", []),
                    "Low": quote.get("low", []),
                    "Close": quote.get("close", []),
                    "Volume": quote.get("volume", []),
                }).set_index("Datetime")
                
                # FIX #8: Reject empty/invalid frames
                if df["Close"].isna().all() or len(df) < 10:
                    return key, None, None
                
                return key, df, meta.get("previousClose")
            except aiohttp.ClientResponseError as e:
                if getattr(e, "status", None) == 429:
                    await backoff_sleep(attempt, base=1.0)
                    continue
                await backoff_sleep(attempt, base=0.5)
                continue
            except (aiohttp.ClientError, asyncio.TimeoutError):
                await backoff_sleep(attempt, base=0.5)
                continue
            except Exception as e:
                json_log("fetch_fatal", {"ticker": pretty, "interval": interval, "error": str(e)})
                break
    return key, None, None

# ---------------- INDICATORS ----------------
def compute_indicators(df: pd.DataFrame):
    if df is None or len(df) < MIN_CANDLE_COUNT:
        return None
    close = df["Close"].astype(float)
    high = df["High"].astype(float)
    low = df["Low"].astype(float)

    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)
    atr = tr.ewm(com=13, adjust=False).mean()

    ema20 = close.ewm(span=20, adjust=False).mean()
    ema50 = close.ewm(span=50, adjust=False).mean()
    ema20_start = safe_iloc(ema20, -5)
    ema20_end = safe_iloc(ema20, -1)
    slope = (ema20_end - ema20_start) / 5 if not pd.isna(ema20_start) else 0
    angle = np.degrees(np.arctan(slope))

    delta = close.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.ewm(com=RSI_PERIOD - 1, adjust=False).mean()
    avg_loss = loss.ewm(com=RSI_PERIOD - 1, adjust=False).mean().replace(0, 1e-10)
    rsi = 100 - (100 / (1 + (avg_gain / avg_loss)))

    ema_fast = close.ewm(span=MACD_FAST, adjust=False).mean()
    ema_slow = close.ewm(span=MACD_SLOW, adjust=False).mean()
    macd = ema_fast - ema_slow
    macd_signal = macd.ewm(span=MACD_SIGNAL, adjust=False).mean()
    macd_hist = macd - macd_signal

    sma = close.rolling(BB_PERIOD).mean()
    std = close.rolling(BB_PERIOD).std()
    bb_upper = sma + 2 * std
    bb_lower = sma - 2 * std
    bb_width = ((bb_upper - bb_lower) / sma).replace([np.inf, -np.inf], np.nan).fillna(0) * 100

    vol_avg_series = df["Volume"].rolling(VOL_PERIOD).mean()
    vol_avg = safe_iloc(vol_avg_series, -1, fallback=float(df["Volume"].mean()))

    return {
        "rsi": float(safe_iloc(rsi, -1, fallback=np.nan)),
        "atr": float(safe_iloc(atr, -1, fallback=np.nan)),
        "ema20": float(ema20_end) if not pd.isna(ema20_end) else np.nan,
        "ema50": float(safe_iloc(ema50, -1, fallback=np.nan)),
        "angle": float(angle),
        "macd_hist": float(safe_iloc(macd_hist, -1, fallback=0.0)),
        "bb_upper": float(safe_iloc(bb_upper, -1, fallback=np.nan)),
        "bb_lower": float(safe_iloc(bb_lower, -1, fallback=np.nan)),
        "bb_width": float(safe_iloc(bb_width, -1, fallback=0.0)),
        "vol_avg": float(vol_avg)
    }

# ---------------- SIGNAL GENERATION (FIXED) ----------------
def generate_signal(df: pd.DataFrame, prev_close, ticker: str, interval: str):
    close = safe_iloc(df["Close"], -1, fallback=np.nan)
    open_ = safe_iloc(df["Open"], -1, fallback=np.nan)
    volume = safe_iloc(df["Volume"], -1, fallback=np.nan)

    if pd.isna(close) or pd.isna(open_) or pd.isna(volume):
        return None

    # Improved prev_close check (stylistic)
    if prev_close not in (None, 0):
        if abs(close - prev_close) / prev_close > MAX_CIRCUIT_PCT:
            return None

    ind = compute_indicators(df)
    if not ind:
        return None

    # FIX #2: Remove votes[] list - use only counters
    buy_votes = 0
    sell_votes = 0
    hold_votes = 0
    reasons = []
    strength = 0

    # Trend detection
    tr = "Sideways"
    if not pd.isna(ind["ema20"]) and not pd.isna(ind["ema50"]):
        if ind["bb_width"] < 1.5:
            tr = "Squeeze"
        elif ind["ema20"] > ind["ema50"] and close > ind["ema20"] and ind["angle"] > 5:
            tr = "Uptrend"
            buy_votes += 1
            strength += 2
        elif ind["ema20"] < ind["ema50"] and close < ind["ema20"] and ind["angle"] < -5:
            tr = "Downtrend"
            sell_votes += 1
            strength += 2

    if tr != "Sideways":
        reasons.append(tr)

    # RSI
    if ind["rsi"] < 30:
        buy_votes += 1
        reasons.append(f"RSI_Low({ind['rsi']:.1f})")
        strength += 2
    elif ind["rsi"] > 70:
        sell_votes += 1
        reasons.append(f"RSI_High({ind['rsi']:.1f})")
        strength += 2
    else:
        hold_votes += 1

    # MACD
    if ind["macd_hist"] > 0:
        buy_votes += 1
        reasons.append("MACD+")
        strength += 1
    elif ind["macd_hist"] < 0:
        sell_votes += 1
        reasons.append("MACD-")
        strength += 1
    else:
        hold_votes += 1

    # Bollinger
    if close < ind["bb_lower"]:
        buy_votes += 1
        reasons.append("BB_Oversold")
        strength += 1
    elif close > ind["bb_upper"]:
        sell_votes += 1
        reasons.append("BB_Overbought")
        strength += 1
    else:
        hold_votes += 1

    # Volume spike
    vol_avg = ind.get("vol_avg") or 1
    vol_ratio = volume / vol_avg if vol_avg > 0 else 0
    if vol_ratio >= VOL_SPIKE_MULT:
        if close > open_:
            buy_votes += 1
            reasons.append(f"VolSpike({vol_ratio:.1f}x)")
            strength += 3
        else:
            sell_votes += 1
            reasons.append(f"VolDump({vol_ratio:.1f}x)")
            strength += 3

    # FIX #3: Correct signal logic
    signal = "Hold"
    base_conf = 40
    
    if buy_votes > sell_votes and buy_votes >= 2:
        signal = "Buy"
        base_conf += min(strength, 50) + (buy_votes * 5)
    elif sell_votes > buy_votes and sell_votes >= 2:
        signal = "Sell"
        base_conf += min(strength, 50) + (sell_votes * 5)
    else:
        signal = "Hold"
        hold_votes = max(1, hold_votes)

    confidence = min(base_conf, 95.0)

    # FIX #7: Better ATR fallback for penny stocks
    atr = ind.get("atr")
    if pd.isna(atr) or atr <= 0:
        atr = max(close * 0.002, 0.1)

    sl = tp = rr = None
    if signal != "Hold" and atr > 0:
        risk = atr * SL_ATR_MULT
        reward = atr * TP_ATR_MULT
        if signal == "Buy":
            sl = round(close - risk, 2)
            tp = round(close + reward, 2)
        else:
            sl = round(close + risk, 2)
            tp = round(close - reward, 2)
        rr = round(reward / risk, 2) if risk > 0 else None

    now = datetime.now(IST)
    
    # FIX #1: Remove underscore fields, use clean names
    # FIX #4: Force uppercase symbol
    # FIX #6: Store macd_hist as macd (matches DB column)
    result = {
        "symbol": ticker.replace(".NS", "").upper(),
        "interval": interval,
        "signal": signal,
        "confidence": round(float(confidence), 1),
        "close_price": round(float(close), 2),
        "stop_loss": sl,
        "target": tp,
        "risk_reward": rr,
        "signal_date": now.date().isoformat(),
        "signal_time": now.strftime("%H:%M:%S"),
        "rsi": round(float(ind["rsi"]), 2) if not pd.isna(ind["rsi"]) else None,
        "buy_votes": buy_votes,
        "sell_votes": sell_votes,
        "hold_votes": hold_votes,
        "macd": round(float(ind["macd_hist"]), 4) if not pd.isna(ind["macd_hist"]) else None
    }
    
    return result

# ---------------- SUPABASE HELPERS ----------------
def ensure_supabase():
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.critical("Supabase credentials missing. Set SUPABASE_URL and SUPABASE_KEY.")
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def clear_today_signals(supabase, table_name):
    if not CLEAR_TODAY:
        logger.info("CLEAR_TODAY not enabled — skipping deletion.")
        return
    today = datetime.now(IST).date().isoformat()
    try:
        supabase.table(table_name).delete().eq("signal_date", today).execute()
        logger.info(f"Cleared today's signals from {table_name} (date={today}).")
    except Exception as e:
        json_log("delete_failed", {"table": table_name, "error": str(e)})

async def upload_signals(supabase, rows, table_name):
    try:
        if not rows:
            return 0
        
        # FIX #5: Deduplicate by natural key (symbol + interval + signal_time)
        seen = set()
        unique_rows = []
        for r in rows:
            if r.get("confidence", 0) < MIN_CONF_TO_UPLOAD:
                continue
            key = (r["symbol"], r["interval"], r["signal_time"])
            if key not in seen:
                seen.add(key)
                unique_rows.append(r)
        
        if not unique_rows:
            return 0
        
        resp = supabase.table(table_name).insert(unique_rows).execute()
        count = len(resp.data) if hasattr(resp, "data") and isinstance(resp.data, list) else len(unique_rows)
        json_log("upload_complete", {"table": table_name, "uploaded": count})
        return count
    except Exception as e:
        json_log("upload_failed", {"table": table_name, "error": str(e)})
        return 0

# ---------------- MAIN ENGINE ----------------
async def run_engine():
    supabase = ensure_supabase()
    market_open, status = is_market_open()
    json_log("engine_start", {"market_status": status, "concurrency": CONCURRENCY_LIMIT})
    
    if not market_open:
        logger.info("Market closed — exiting.")
        if CLEAR_TODAY:
            clear_today_signals(supabase, DB_TABLE_SIGNALS)
        return

    if CLEAR_TODAY:
        clear_today_signals(supabase, DB_TABLE_SIGNALS)

    async with aiohttp.ClientSession() as session:
        tasks = []
        for ticker in STOCK_LIST:
            for tf in TIME_FRAMES:
                tasks.append(fetch_data(session, ticker.strip(), tf.strip()))

        raw_results = await asyncio.gather(*tasks)
    
    signals = []
    for item in raw_results:
        try:
            (ticker, interval), df, prev = item
        except Exception:
            continue

        if df is None:
            continue

        s = generate_signal(df, prev, ticker, interval)
        if s:
            signals.append(s)

    if signals:
        signals_sorted = sorted(signals, key=lambda r: r.get("confidence", 0), reverse=True)
        top_signals = signals_sorted[:MAX_SIGNALS]
    else:
        top_signals = []

    uploaded_count = await upload_signals(supabase, top_signals, DB_TABLE_SIGNALS)

    json_log("engine_end", {
        "signals_generated": len(signals),
        "signals_uploaded": uploaded_count,
        "duration_s": round(time.time() - START_TS, 2)
    })

# ---------------- ENTRY ----------------
def main():
    try:
        asyncio.run(run_engine())
    except Exception as e:
        json_log("engine_fatal", {"error": str(e)})
        logger.exception("Engine fatal error")
        sys.exit(1)

if __name__ == "__main__":
    main()
