#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Finspark AI — v6.1 FINAL (single-file)
- Implements v6.0 fixes + v6.1 improvements:
  1) Safe daily deletion (non-destructive, env gated)
  2) Volume-spike signal + ATR fallback
  3) Exponential backoff + jitter for 429/network
  4) Signal count limiter (top-K by confidence)
  5) Append-only risk log (engine_risk_log)
  6) Structured JSON logging + human logs
  7) Dynamic concurrency tuning (based on runners)
  8) Minor speed optimizations & safer NaN checks
Usage: set SUPABASE_URL and SUPABASE_KEY as secrets/env and run.
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
DB_TABLE_SIGNALS = os.getenv("DB_TABLE_SIGNALS", "intraday_signals_v6_final")
DB_TABLE_ERRORS = os.getenv("DB_TABLE_ERRORS", "engine_errors_v6_final")
DB_TABLE_RISKLOG = os.getenv("DB_TABLE_RISKLOG", "engine_risk_log")

# Behavior flags:
CLEAR_TODAY = os.getenv("CLEAR_TODAY", "false").lower() in ("1", "true", "yes")
MAX_SIGNALS = int(os.getenv("MAX_SIGNALS", "150"))         # top-K per run
VOL_SPIKE_MULT = float(os.getenv("VOL_SPIKE_MULT", "3.0")) # volume spike multiplier
MIN_CONF_TO_UPLOAD = float(os.getenv("MIN_CONF_TO_UPLOAD", "0")) # optional threshold

# Timeframes & Tickers
TIME_FRAMES = os.getenv("TIME_FRAMES", "5m,15m,30m,1h").split(",")
DEFAULT_PERIOD = os.getenv("DEFAULT_PERIOD", "7d")
IST = pytz.timezone("Asia/Kolkata")
MARKET_OPEN = dt_time(9, 15)
MARKET_CLOSE = dt_time(15, 30)

STOCK_LIST = os.getenv("STOCK_LIST", "RELIANCE.NS,TCS.NS,INFY.NS,HDFCBANK.NS,ICICIBANK.NS").split(",")
TICKER_MAP = {}  # keep if needed to remap tickers

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

# Logging
LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("FinsparkV6.1")

# JSON logger for structured events
def json_log(event_type: str, payload: dict):
    out = {"ts": datetime.now(IST).isoformat(), "event": event_type}
    out.update(payload)
    logger.info(json.dumps(out, default=str))

# ---------------- SAFE HELPERS ----------------
def get_safe_ticker(t):
    return TICKER_MAP.get(t.upper(), t.upper())

def safe_iloc(series: pd.Series, idx: int, fallback=np.nan):
    """Robust iloc supporting negative indices and NaN-safe returns."""
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
    # Market open inclusive at MARKET_OPEN, exclusive of MARKET_CLOSE end-of-minute (configurable)
    is_open = (MARKET_OPEN <= now.time() < MARKET_CLOSE)
    return is_open, "Open" if is_open else "Closed"

# ---------------- BACKOFF + JITTER ----------------
async def backoff_sleep(attempt: int, base: float = 1.0, cap: float = 30.0):
    # exponential backoff with jitter
    delay = min(cap, base * (2 ** attempt))
    jitter = delay * (0.5 + random.random() * 0.5)  # 0.5x - 1.0x jitter
    await asyncio.sleep(jitter)

# ---------------- FETCH (Yahoo V8 endpoint) ----------------
SEM = asyncio.Semaphore(CONCURRENCY_LIMIT)

async def fetch_data(session: aiohttp.ClientSession, ticker: str, interval: str):
    pretty = ticker.replace(".NS", "")
    mapped = get_safe_ticker(ticker)
    url = f"https://query2.finance.yahoo.com/v8/finance/chart/{mapped}"
    params = {"range": DEFAULT_PERIOD, "interval": interval, "includePrePost": False}

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
                return key, df, meta.get("previousClose")
            except aiohttp.ClientResponseError as e:
                if getattr(e, "status", None) == 429:
                    await backoff_sleep(attempt, base=1.0)
                    continue
                # other client errors - short backoff
                await backoff_sleep(attempt, base=0.5)
                continue
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                json_log("network_error", {"ticker": pretty, "interval": interval, "error": str(e), "attempt": attempt + 1})
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

# ---------------- SIGNAL GENERATION ----------------
def generate_signal(df: pd.DataFrame, prev_close, ticker: str, interval: str):
    close = safe_iloc(df["Close"], -1, fallback=np.nan)
    open_ = safe_iloc(df["Open"], -1, fallback=np.nan)
    high = safe_iloc(df["High"], -1, fallback=np.nan)
    low = safe_iloc(df["Low"], -1, fallback=np.nan)
    volume = safe_iloc(df["Volume"], -1, fallback=np.nan)

    if pd.isna(close) or pd.isna(open_) or pd.isna(volume):
        return None

    # circuit safety
    if prev_close and prev_close != 0:
        if abs(close - prev_close) / prev_close > MAX_CIRCUIT_PCT:
            json_log("skip_circuit", {"ticker": ticker, "interval": interval, "close": close, "prev_close": prev_close})
            return None

    ind = compute_indicators(df)
    if not ind:
        return None

    votes = []
    reasons = []
    strength = 0

    # trend
    tr = "Sideways"
    if not pd.isna(ind["ema20"]) and not pd.isna(ind["ema50"]):
        if ind["bb_width"] < 1.5:
            tr = "Squeeze"
        elif ind["ema20"] > ind["ema50"] and close > ind["ema20"] and ind["angle"] > 5:
            tr = "Uptrend"
        elif ind["ema20"] < ind["ema50"] and close < ind["ema20"] and ind["angle"] < -5:
            tr = "Downtrend"

    if tr != "Sideways":
        reasons.append(tr)
        strength += 2

    # RSI votes
    if ind["rsi"] < 30:
        votes.append("Buy"); reasons.append(f"RSI({ind['rsi']:.1f})"); strength += 2
    elif ind["rsi"] > 70:
        votes.append("Sell"); reasons.append(f"RSI({ind['rsi']:.1f})"); strength += 2

    # MACD
    if ind["macd_hist"] > 0:
        votes.append("Buy"); reasons.append("MACD_hist_pos"); strength += 1
    elif ind["macd_hist"] < 0:
        votes.append("Sell"); reasons.append("MACD_hist_neg"); strength += 1

    # Bollinger
    if close < ind["bb_lower"]:
        votes.append("Buy"); reasons.append("BelowBB"); strength += 1
    elif close > ind["bb_upper"]:
        votes.append("Sell"); reasons.append("AboveBB"); strength += 1

    # Volume spike vote (v6.1 feature)
    vol_avg = ind.get("vol_avg") or 1
    vol_ratio = volume / vol_avg if vol_avg > 0 else 0
    if vol_ratio >= VOL_SPIKE_MULT:
        if close > open_:
            votes.append("Buy"); reasons.append(f"VolSpike({vol_ratio:.1f}x)"); strength += 3
        else:
            votes.append("Sell"); reasons.append(f"VolSpike({vol_ratio:.1f}x)"); strength += 3

    # Final decision
    buy_count = votes.count("Buy")
    sell_count = votes.count("Sell")

    signal = "Hold"
    base_conf = 40 + min(max(strength, 0) * 4, 50)
    if buy_count > sell_count and buy_count >= 2:
        signal = "Buy"
        base_conf += buy_count * 5
    elif sell_count > buy_count and sell_count >= 2:
        signal = "Sell"
        base_conf += sell_count * 5

    confidence = min(base_conf, 95.0)

    # ATR fallback
    atr = ind.get("atr") if not pd.isna(ind.get("atr")) else max(close * 0.003, 1.0)

    sl = tp = rr = None
    if signal != "Hold" and atr and atr > 0:
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
    result = {
        "symbol": ticker.replace(".NS", ""),
        "interval": interval,
        "signal": signal,
        "confidence": round(float(confidence), 1),
        "close_price": round(float(close), 2),
        "stop_loss": sl,
        "target": tp,
        "risk_reward": rr,
        "trend": tr,
        "vol_ratio": round(float(vol_ratio), 2),
        "signal_date": now.date().isoformat(),
        "signal_time": now.strftime("%H:%M:%S"),
        "reasons": ", ".join(reasons),
        "raw_indicators": ind
    }
    json_log("signal_generated", {"symbol": result["symbol"], "interval": interval, "signal": signal, "confidence": result["confidence"]})
    return result

# ---------------- SUPABASE HELPERS ----------------
def ensure_supabase():
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.critical("Supabase credentials missing. Set SUPABASE_URL and SUPABASE_KEY.")
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def clear_today_signals(supabase, table_name):
    if not CLEAR_TODAY:
        logger.info("CLEAR_TODAY not enabled — skipping deletion of today's signals.")
        return
    today = datetime.now(IST).date().isoformat()
    try:
        supabase.table(table_name).delete().eq("signal_date", today).execute()
        logger.info(f"Cleared today's signals from {table_name} (date={today}).")
    except Exception as e:
        json_log("delete_failed", {"table": table_name, "error": str(e)})

def append_risklog(supabase, rows):
    try:
        if not rows:
            return
        # keep raw_indicators JSON-friendly
        for r in rows:
            r_copy = dict(r)
            # convert raw_indicators to json serializable (if present)
            if "raw_indicators" in r_copy:
                r_copy["raw_indicators"] = json.dumps(r_copy["raw_indicators"], default=str)
            supabase.table(DB_TABLE_RISKLOG).insert(r_copy).execute()
    except Exception as e:
        json_log("risklog_failed", {"error": str(e)})

async def upload_signals(supabase, rows, table_name):
    try:
        if not rows:
            return 0
        # Filter by confidence threshold if set
        filtered = [r for r in rows if r.get("confidence", 0) >= MIN_CONF_TO_UPLOAD]
        resp = supabase.table(table_name).insert(filtered).execute()
        # try to get number inserted
        count = len(resp.data) if hasattr(resp, "data") and isinstance(resp.data, list) else len(filtered)
        json_log("upload_complete", {"table": table_name, "uploaded": count})
        return count
    except Exception as e:
        json_log("upload_failed", {"table": table_name, "error": str(e)})
        return 0

# ---------------- MAIN ENGINE ----------------
async def run_engine():
    supabase = ensure_supabase()
    market_open, status = is_market_open()
    json_log("engine_start", {"market_status": status, "timestamp": datetime.now(IST).isoformat(), "concurrency": CONCURRENCY_LIMIT})
    if not market_open:
        logger.info("Market closed — exiting without generating signals.")
        # still do controlled cleanup if requested
        if CLEAR_TODAY:
            clear_today_signals(supabase, DB_TABLE_SIGNALS)
        return

    # safe daily deletion (env gated)
    if CLEAR_TODAY:
        clear_today_signals(supabase, DB_TABLE_SIGNALS)

    async with aiohttp.ClientSession() as session:
        tasks = []
        for ticker in STOCK_LIST:
            for tf in TIME_FRAMES:
                tasks.append(fetch_data(session, ticker.strip(), tf.strip()))

        # run fetches concurrently
        raw_results = await asyncio.gather(*tasks)
    # map by key robustly
    signals = []
    risklog_rows = []
    for item in raw_results:
        try:
            (ticker, interval), df, prev = item
        except Exception:
            # Some fetch tasks may return (key, None, None) shape; handle defensively
            if isinstance(item, tuple) and len(item) == 3:
                key = item[0] if len(item) > 0 else ("UNK", "UNK")
                ticker, interval = key if isinstance(key, tuple) else ("UNK", "UNK")
                df = item[1] if len(item) > 1 else None
                prev = item[2] if len(item) > 2 else None
            else:
                continue

        if df is None:
            json_log("fetch_no_data", {"ticker": ticker, "interval": interval})
            # log error
            try:
                supabase.table(DB_TABLE_ERRORS).insert({
                    "timestamp": datetime.now(IST).isoformat(),
                    "error_type": "FetchNoData",
                    "message": f"No data for {ticker} {interval}",
                    "stock": ticker, "interval": interval
                }).execute()
            except Exception:
                pass
            continue

        s = generate_signal(df, prev, ticker, interval)
        if s:
            signals.append(s)
            # prepare risk log row (append-only)
            risklog_rows.append({
                "timestamp": datetime.now(IST).isoformat(),
                "symbol": s["symbol"],
                "interval": s["interval"],
                "signal": s["signal"],
                "confidence": s["confidence"],
                "close_price": s["close_price"],
                "stop_loss": s["stop_loss"],
                "target": s["target"],
                "risk_reward": s["risk_reward"],
                "reasons": s["reasons"],
                "raw_indicators": json.dumps(s.get("raw_indicators", {}), default=str)
            })

    # apply top-K limiter (v6.1 feature)
    if signals:
        signals_sorted = sorted(signals, key=lambda r: (r.get("confidence", 0)), reverse=True)
        top_signals = signals_sorted[:MAX_SIGNALS]
    else:
        top_signals = []

    # upload signals (non-destructive to other tables)
    uploaded_count = await upload_signals(supabase, top_signals, DB_TABLE_SIGNALS)

    # append to risk log regardless of upload (append-only)
    try:
        append_risklog(supabase, risklog_rows)
    except Exception as e:
        json_log("risklog_upload_error", {"error": str(e)})

    json_log("engine_end", {
        "signals_generated": len(signals),
        "signals_uploaded": uploaded_count,
        "duration_s": round(time.time() - START_TS, 2)
    })

# ---------------- ENTRY ----------------
START_TS = time.time()
def main():
    try:
        asyncio.run(run_engine())
    except Exception as e:
        json_log("engine_fatal", {"error": str(e)})
        logger.exception("Engine fatal error")
        sys.exit(1)

if __name__ == "__main__":
    main()
