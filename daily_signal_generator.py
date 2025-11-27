#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
daily_signal_generator.py — AI INTRADAY SIGNAL GENERATOR v3 (Option C: Clean & Verify custom list)

Features:
- Accepts custom STOCK_LIST (env var or symbols.txt)
- Sanitizes tickers, applies corrections for renamed/delisted tickers
- Verifies tickers with yfinance and falls back with heuristics
- Robust intraday fetch (15m, 5d) with retries
- Indicator engine: RSI, MACD, Bollinger Bands, EMAs, ATR, vol avg
- Voting-based Buy/Sell/Hold decision (preserves your scoring logic)
- Batched insert to Supabase (table: 'signals' by default)
- Safe for GitHub Actions / Cron

Usage:
    SUPABASE_URL=... SUPABASE_KEY=... STOCK_LIST="RELIANCE.NS,TCS.NS,INFY.NS" python daily_signal_generator.py
Or:
    put a list of tickers (one per line) in symbols.txt
"""

import os
import sys
import time
import re
import json
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import numpy as np
import yfinance as yf

# Optional: supabase client (will be imported at runtime)
try:
    from supabase import create_client
except Exception:
    create_client = None

# -------------------------
# Logger
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
LOG = logging.getLogger("daily_signal_generator")

# -------------------------
# Configuration (env overrides)
# -------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
STOCK_LIST_ENV = os.getenv("STOCK_LIST", "").strip()
STOCKS_FILE = os.path.join(os.getcwd(), "symbols.txt")  # optional fallback file
INTERVAL = os.getenv("INTERVAL", "15m")
INTRADAY_PERIOD = os.getenv("INTRADAY_PERIOD", "5d")
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20"))
RETRY_DELAY = float(os.getenv("RETRY_DELAY", "0.5"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
MARKET_OPEN = (9, 15)
MARKET_CLOSE = (15, 30)
RSI_PERIOD = int(os.getenv("RSI_PERIOD", "14"))
MACD_FAST = int(os.getenv("MACD_FAST", "12"))
MACD_SLOW = int(os.getenv("MACD_SLOW", "26"))
MACD_SIGNAL = int(os.getenv("MACD_SIGNAL", "9"))
BB_PERIOD = int(os.getenv("BB_PERIOD", "20"))
VOL_PERIOD = int(os.getenv("VOL_PERIOD", "10"))
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "1.5"))
TARGET_PCT = float(os.getenv("TARGET_PCT", "3.0"))
MIN_CONFIDENCE = float(os.getenv("MIN_CONFIDENCE", "60"))
MIN_VOTES_FOR_ACTION = int(os.getenv("MIN_VOTES_FOR_ACTION", "5"))
MIN_STRENGTH_SCORE = int(os.getenv("MIN_STRENGTH_SCORE", "8"))
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "signals")
VERBOSE = os.getenv("VERBOSE", "1") != "0"

# -------------------------
# Symbol correction mapping
# (extend this mapping as you discover more mismatches)
# -------------------------
SYMBOL_FIX = {
    # common typos/old→new
    "MM.NS": "M&M.NS",            # Mahindra & Mahindra
    "MINDTREE.NS": "LTIM.NS",     # Mindtree merged/renamed -> LTIM (L&T Infotech common mapping)
    "LTI.NS": "LTIM.NS",          # LTI -> LTIM
    "CADILAHC.NS": "ZYDUSLIFE.NS",# Cadila Healthcare reorg mapping (example)
    "ADANITRANS.NS": "ADANITRANS.NS", # keep as-is, but include stub
    "ADANIGAS.NS": "ATGL.NS",     # Adani Gas -> ATGL (example)
    "BERGER.NS": "BERGEPAINT.NS",
    "PHOENIX.NS": "PHOENIXLTD.NS",
    # add more mappings you need...
}

# A small set of heuristics for symbol normalization
def normalize_symbol_token(t):
    # Remove stray characters, keep uppercase
    t = str(t).strip()
    t = t.strip('"\'`')
    # common replacements
    t = t.replace(" ", "").replace("‐", "-").replace("–", "-").replace("—", "-")
    # sometimes users paste "M&M.NS" as "M&amp;M.NS", fix amp entity
    t = t.replace("&AMP;", "&").replace("&amp;", "&")
    t = t.upper()
    # ensure it ends with .NS if it looks like NSE symbol and not already suffixed
    if not re.search(r"\.[A-Z]{1,5}$", t) and not t.endswith(".NS"):
        # don't force .NS for non NSE symbols, but if the user's list appears NSE style, default .NS
        # we will attempt without .NS first and then try .NS
        pass
    return t

# -------------------------
# Utility: read stock list
# -------------------------
def read_stock_list():
    # Priority: STOCK_LIST env -> symbols.txt file -> default minimal list
    tickers = []
    if STOCK_LIST_ENV:
        # split on comma or newline
        raw = STOCK_LIST_ENV
        parts = re.split(r"[\n,]+", raw)
        tickers = [p.strip() for p in parts if p.strip()]
        LOG.info("Loaded %d tickers from STOCK_LIST env", len(tickers))
    elif os.path.exists(STOCKS_FILE):
        with open(STOCKS_FILE, "r", encoding="utf-8") as fh:
            tickers = [line.strip() for line in fh if line.strip()]
        LOG.info("Loaded %d tickers from symbols.txt", len(tickers))
    else:
        # safe default minimal list (user asked custom - but fallback to NIFTY 50 subset)
        LOG.warning("No STOCK_LIST provided and symbols.txt not found — using safe default subset (NIFTY sample)")
        tickers = [
            "RELIANCE.NS", "TCS.NS", "INFY.NS", "HDFCBANK.NS", "ICICIBANK.NS",
            "HINDUNILVR.NS", "SBIN.NS", "KOTAKBANK.NS", "LTIM.NS", "AXISBANK.NS",
        ]
    return tickers

# -------------------------
# Sanitizer & dedupe
# -------------------------
TICKER_RE = re.compile(r"^[A-Z0-9&\.\-_\+]{1,30}(\.NS|\.BO|\.NSX|\.BSE)?$")

def sanitize_and_fix(raw_list):
    seen = set()
    cleaned = []
    for raw in raw_list:
        if not raw:
            continue
        t = normalize_symbol_token(raw)
        # common patterns: if user gave 'M&M.NS', keep; if 'MM.NS' map
        if t in SYMBOL_FIX:
            fixed = SYMBOL_FIX[t]
            LOG.debug("Mapping %s -> %s (SYMBOL_FIX)", t, fixed)
            t = fixed
        # strip trailing dots/spaces
        t = t.strip().strip(".")
        # uppercase enforced
        t = t.upper()
        # remove illegal characters except &, ., -, _
        t = re.sub(r"[^A-Z0-9&\.\-_\+]", "", t)
        # if no suffix and seems like NSE ticker, add .NS attempt later in verification process
        if t not in seen:
            seen.add(t)
            cleaned.append(t)
    return cleaned

# -------------------------
# Verify tickers via yfinance (probe)
# Attempts multiple heuristics:
#  - try as-is
#  - try adding .NS
#  - try replacing '&' with nothing or 'AND'
#  - attempt SYMBOL_FIX mapping
# -------------------------
def verify_ticker_with_yf(ticker, timeout=5):
    """
    Returns tuple (verified_symbol, reason)
     - verified_symbol: corrected ticker string that yields data, or None
     - reason: short explanation
    """
    attempts = []
    # helper to probe small history (1d, 1m optional)
    def probe(sym):
        try:
            tk = yf.Ticker(sym)
            # Try to fetch a tiny slice: recent 1 day (or 5d with 1d interval) to see if exists
            df = tk.history(period="5d", interval="1d", prepost=False, actions=False)
            if df is None or df.empty:
                return False
            # also check if Close exists and numeric
            if "Close" not in df.columns:
                return False
            last_close = df["Close"].dropna().iloc[-1]
            if pd.isna(last_close) or float(last_close) <= 0:
                return False
            return True
        except Exception as e:
            LOG.debug("probe error for %s: %s", sym, e)
            return False

    # 1) try as-is
    attempts.append(ticker)
    if probe(ticker):
        return ticker, "ok"

    # 2) try common suffix ".NS"
    if not ticker.endswith(".NS"):
        candidate = f"{ticker}.NS"
        attempts.append(candidate)
        if probe(candidate):
            return candidate, "added .NS"

    # 3) try replacing & with nothing or AND (e.g., M&M -> MANDM or MM)
    if "&" in ticker:
        cand1 = ticker.replace("&", "")
        cand2 = ticker.replace("&", "AND")
        for c in (cand1, cand2):
            if c not in attempts:
                attempts.append(c)
                if probe(c):
                    return c, "ampersand-normalized"

    # 4) apply symbol fix mapping if exists
    if ticker in SYMBOL_FIX:
        fixed = SYMBOL_FIX[ticker]
        attempts.append(fixed)
        if probe(fixed):
            return fixed, "symbol_fix_map"

    # 5) try replacing "-" with nothing
    if "-" in ticker:
        c = ticker.replace("-", "")
        if c not in attempts:
            attempts.append(c)
            if probe(c):
                return c, "hyphen-removed"

    # 6) if ticker is like "MM.NS" try known mapping to "M&M.NS" etc (already in SYMBOL_FIX usually)
    # 7) last resort: try uppercase + .NS
    upper_ns = ticker.upper()
    if not upper_ns.endswith(".NS"):
        upper_ns += ".NS"
    if upper_ns not in attempts:
        attempts.append(upper_ns)
        if probe(upper_ns):
            return upper_ns, "upper_ns"

    # if still not found, return None with attempts list
    return None, f"not_found (tried {attempts[:5]})"

# -------------------------
# yfinance data fetch with retries and multiindex handling
# -------------------------
def fetch_intraday_history(ticker, period=INTRADAY_PERIOD, interval=INTERVAL, max_retries=MAX_RETRIES):
    """Return DataFrame or None"""
    for attempt in range(1, max_retries + 1):
        try:
            tk = yf.Ticker(ticker)
            df = tk.history(period=period, interval=interval, auto_adjust=True, prepost=False, actions=False)
            if hasattr(df, "columns") and isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            if df is None or df.empty:
                LOG.debug("%s: empty data on attempt %d", ticker, attempt)
                time.sleep(RETRY_DELAY)
                continue
            # ensure numeric dtype for main columns
            for col in ["Open", "High", "Low", "Close", "Volume"]:
                if col in df.columns:
                    try:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                    except Exception:
                        pass
            # basic last value sanity
            if "Close" not in df.columns or df["Close"].dropna().empty:
                LOG.debug("%s: no valid closes", ticker)
                time.sleep(RETRY_DELAY)
                continue
            last_close = df["Close"].dropna().iloc[-1]
            if pd.isna(last_close) or float(last_close) <= 0:
                LOG.debug("%s: invalid last close %s", ticker, last_close)
                time.sleep(RETRY_DELAY)
                continue
            return df
        except Exception as e:
            LOG.debug("%s: fetch attempt %d error: %s", ticker, attempt, e)
            time.sleep(RETRY_DELAY)
    LOG.info("❌ %s: Failed to fetch data after %d attempts", ticker, max_retries)
    return None

# -------------------------
# Data quality checks
# -------------------------
def validate_data_quality(df):
    """
    - at least 50 candles
    - not stale (last timestamp within 30 minutes)
    - not too many gaps > 2 * interval
    - not too many extreme moves
    """
    if df is None or df.empty:
        return False, "Empty data"
    if len(df) < 50:
        return False, f"Insufficient data ({len(df)} candles)"
    try:
        idx = pd.DatetimeIndex(df.index)
        diffs = idx.to_series().diff()
        expected = pd.Timedelta(minutes=15)
        gaps = (diffs > expected * 2).sum()
        if gaps > 3:
            return False, f"Too many data gaps ({int(gaps)})"
    except Exception:
        # skip gap checks if index invalid
        pass
    # staleness
    try:
        last_time = pd.Timestamp(df.index[-1])
        if getattr(last_time, "tzinfo", None) is not None:
            try:
                last_time = last_time.tz_convert(None)
            except Exception:
                try:
                    last_time = last_time.tz_localize(None)
                except Exception:
                    pass
    except Exception:
        last_time = pd.Timestamp.now()

    staleness_mins = (pd.Timestamp.now() - last_time).total_seconds() / 60
    if staleness_mins > 30:
        return False, f"Stale data ({staleness_mins:.0f} min old)"

    if "Close" in df.columns:
        price_changes = df["Close"].pct_change().abs().fillna(0)
        extreme_moves = (price_changes > 0.10).sum()
        if extreme_moves > 2:
            return False, f"Extreme volatility ({int(extreme_moves)} spikes)"
    return True, "OK"

# -------------------------
# Indicator engine
# -------------------------
def calculate_intraday_indicators(prices_df):
    close = prices_df["Close"].astype(float)
    high = prices_df["High"].astype(float)
    low = prices_df["Low"].astype(float)
    volume = prices_df["Volume"].astype(float) if "Volume" in prices_df.columns else pd.Series(np.nan, index=prices_df.index)

    # RSI (Wilder's RSI approx via SMA)
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(RSI_PERIOD).mean()
    avg_loss = loss.rolling(RSI_PERIOD).mean().replace(0, 1e-8)
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    # MACD
    ema_fast = close.ewm(span=MACD_FAST, adjust=False).mean()
    ema_slow = close.ewm(span=MACD_SLOW, adjust=False).mean()
    macd = ema_fast - ema_slow
    macd_signal = macd.ewm(span=MACD_SIGNAL, adjust=False).mean()
    macd_hist = macd - macd_signal

    # Bollinger Bands
    sma = close.rolling(BB_PERIOD).mean()
    std = close.rolling(BB_PERIOD).std()
    bb_upper = sma + (2 * std)
    bb_lower = sma - (2 * std)
    bb_middle = sma

    # Volume average
    vol_avg = volume.rolling(VOL_PERIOD).mean()

    # EMAs for trend
    ema_20 = close.ewm(span=20, adjust=False).mean()
    ema_50 = close.ewm(span=50, adjust=False).mean()

    # ATR
    tr1 = high - low
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(14).mean()

    return {
        "rsi": rsi,
        "macd": macd,
        "macd_signal": macd_signal,
        "macd_hist": macd_hist,
        "bb_upper": bb_upper,
        "bb_lower": bb_lower,
        "bb_mid": bb_middle,
        "vol_avg": vol_avg,
        "ema_20": ema_20,
        "ema_50": ema_50,
        "atr": atr,
    }

# -------------------------
# Helper: safe extraction
# -------------------------
def get_value(series, idx=-1):
    try:
        if series is None:
            return None
        if isinstance(series, pd.Series):
            if len(series) == 0:
                return None
            val = series.iloc[idx]
        elif isinstance(series, (list, tuple, np.ndarray)):
            if len(series) == 0:
                return None
            val = series[idx]
        else:
            val = series
        if pd.isna(val):
            return None
        f = float(val)
        if f <= 0 or f < 0.0001:
            return None
        return f
    except Exception:
        return None

# -------------------------
# Generate signal (preserving your vote-based logic)
# -------------------------
def generate_intraday_signal_for_symbol(symbol, stock_num=0, total=0):
    pretty = symbol.replace(".NS", "").replace(".BO", "")
    prefix = f"[{stock_num}/{total}] " if total > 0 else ""
    try:
        df = fetch_intraday_history(symbol)
        if df is None:
            LOG.info("%s%s: Failed to fetch data", prefix, symbol)
            return None

        is_valid, msg = validate_data_quality(df)
        if not is_valid:
            LOG.info("%s%s: %s", prefix, symbol, msg)
            return None

        for col in ["Close", "Open", "High", "Low", "Volume"]:
            if col not in df.columns:
                LOG.info("%s%s: Missing %s", prefix, symbol, col)
                return None

        last_idx = -1
        close_price = get_value(df["Close"], last_idx)
        open_price = get_value(df["Open"], last_idx)
        high_price = get_value(df["High"], last_idx)
        low_price = get_value(df["Low"], last_idx)
        volume = get_value(df["Volume"], last_idx)
        if close_price is None:
            LOG.info("%s%s: Invalid last close", prefix, symbol)
            return None

        ind = calculate_intraday_indicators(df)
        rsi_val = get_value(ind["rsi"], last_idx) or 50.0
        macd_val = get_value(ind["macd"], last_idx) or 0.0
        macd_sig_val = get_value(ind["macd_signal"], last_idx) or 0.0
        macd_hist = get_value(ind["macd_hist"], last_idx) or 0.0
        bb_up_val = get_value(ind["bb_upper"], last_idx) or close_price * 1.02
        bb_low_val = get_value(ind["bb_lower"], last_idx) or close_price * 0.98
        bb_mid_val = get_value(ind["bb_mid"], last_idx) or close_price
        vol_avg = get_value(ind["vol_avg"], last_idx)
        ema_20 = get_value(ind["ema_20"], last_idx) or close_price
        ema_50 = get_value(ind["ema_50"], last_idx) or close_price
        atr_val = get_value(ind["atr"], last_idx) or (close_price * 0.02)

        votes = []
        strength_score = 0
        reasons = []

        # Trend
        if ema_20 > ema_50 * 1.002 and close_price > ema_20:
            trend = "Uptrend"
            strength_score += 3
            reasons.append("Uptrend")
        elif ema_20 < ema_50 * 0.998 and close_price < ema_20:
            trend = "Downtrend"
            strength_score += 3
            reasons.append("Downtrend")
        else:
            trend = "Sideways"

        # RSI
        if rsi_val < 30:
            votes.append("Buy")
            strength_score += 3
            reasons.append(f"RSI oversold ({rsi_val:.1f})")
        elif rsi_val > 70:
            votes.append("Sell")
            strength_score += 3
            reasons.append(f"RSI overbought ({rsi_val:.1f})")
        elif rsi_val < 40 and trend == "Uptrend":
            votes.append("Buy")
            strength_score += 1
            reasons.append("RSI pullback")
        elif rsi_val > 60 and trend == "Downtrend":
            votes.append("Sell")
            strength_score += 1
            reasons.append("RSI bounce")
        else:
            votes.append("Hold")

        # MACD
        if macd_val > macd_sig_val and macd_hist > 0:
            votes.append("Buy")
            strength_score += 2
            reasons.append("MACD bullish")
        elif macd_val < macd_sig_val and macd_hist < 0:
            votes.append("Sell")
            strength_score += 2
            reasons.append("MACD bearish")
        else:
            votes.append("Hold")

        # Bollinger position
        bb_width = max(bb_up_val - bb_low_val, 1e-8)
        bb_position = (close_price - bb_low_val) / bb_width if bb_width > 0 else 0.5
        if bb_position < 0.15:
            votes.append("Buy")
            strength_score += 2
            reasons.append("Near BB lower")
        elif bb_position > 0.85:
            votes.append("Sell")
            strength_score += 2
            reasons.append("Near BB upper")
        else:
            votes.append("Hold")

        # Volume
        if volume and vol_avg:
            vol_ratio = volume / vol_avg if vol_avg > 0 else 1.0
            if vol_ratio > 1.8:
                if close_price > open_price:
                    votes.append("Buy")
                    strength_score += 3
                    reasons.append("High vol bullish")
                elif close_price < open_price:
                    votes.append("Sell")
                    strength_score += 3
                    reasons.append("High vol bearish")
                else:
                    votes.append("Hold")
            elif vol_ratio < 0.6:
                votes.append("Hold")
                strength_score -= 2
            else:
                votes.append("Hold")
        else:
            votes.append("Hold")

        # Candle body
        candle_body = (close_price - open_price) if open_price is not None else 0
        candle_range = (high_price - low_price) if (high_price and low_price) else 0
        if candle_range > 0:
            body_ratio = abs(candle_body) / candle_range
            if candle_body > 0 and body_ratio > 0.7:
                votes.append("Buy")
                strength_score += 2
                reasons.append("Strong bull candle")
            elif candle_body < 0 and body_ratio > 0.7:
                votes.append("Sell")
                strength_score += 2
                reasons.append("Strong bear candle")
            elif body_ratio < 0.1:
                votes.append("Hold")
                strength_score -= 1
            else:
                votes.append("Hold")
        else:
            votes.append("Hold")

        # Tally
        buy_count = votes.count("Buy")
        sell_count = votes.count("Sell")
        hold_count = votes.count("Hold")

        signal = "Hold"
        base_conf = 30.0

        if buy_count >= MIN_VOTES_FOR_ACTION and trend != "Downtrend":
            signal = "Buy"
            base_conf = (buy_count / 6.0) * 60.0
        elif sell_count >= MIN_VOTES_FOR_ACTION and trend != "Uptrend":
            signal = "Sell"
            base_conf = (sell_count / 6.0) * 60.0
        elif buy_count == 4 and trend == "Uptrend" and strength_score >= MIN_STRENGTH_SCORE:
            signal = "Buy"
            base_conf = (buy_count / 6.0) * 50.0
        elif sell_count == 4 and trend == "Downtrend" and strength_score >= MIN_STRENGTH_SCORE:
            signal = "Sell"
            base_conf = (sell_count / 6.0) * 50.0
        else:
            signal = "Hold"
            base_conf = 30.0

        bonus = min(max(strength_score, 0) * 2, 25)
        confidence = min(base_conf + bonus, 80)

        if signal != "Hold" and confidence < MIN_CONFIDENCE:
            signal = "Hold"
            confidence = 50.0

        if signal == "Buy":
            stop_loss = round(close_price * (1 - STOP_LOSS_PCT / 100.0), 2)
            target = round(close_price * (1 + TARGET_PCT / 100.0), 2)
        elif signal == "Sell":
            stop_loss = round(close_price * (1 + STOP_LOSS_PCT / 100.0), 2)
            target = round(close_price * (1 - TARGET_PCT / 100.0), 2)
        else:
            stop_loss = None
            target = None

        risk_reward = None
        if stop_loss is not None and target is not None:
            risk = abs(close_price - stop_loss)
            reward = abs(target - close_price)
            risk_reward = round((reward / risk), 2) if risk > 0 else None

        result = {
            "symbol": pretty,
            "orig_symbol": symbol,
            "resolved_symbol": symbol,
            "signal": signal,
            "confidence": round(float(confidence), 1),
            "close_price": round(float(close_price), 2),
            "rsi": round(float(rsi_val), 1),
            "macd": round(float(macd_val), 3),
            "stop_loss": stop_loss,
            "target": target,
            "risk_reward": risk_reward,
            "buy_votes": int(buy_count),
            "sell_votes": int(sell_count),
            "hold_votes": int(hold_count),
            "signal_date": datetime.now().date().isoformat(),
            "signal_time": datetime.now().strftime("%H:%M:%S"),
            "interval": INTERVAL,
            "reasons": reasons[:6],
            "strength_score": int(strength_score),
        }
        emoji = "🟢" if signal == "Buy" else "🔴" if signal == "Sell" else "⚪"
        reason_str = ", ".join(reasons[:2]) if reasons else ""
        LOG.info("%s%s %s (%s%%) @ ₹%s %s [%s]", prefix, pretty.ljust(12), signal, int(confidence), result["close_price"], f"SL:{result['stop_loss']} T:{result['target']}" if stop_loss else "", reason_str)
        return result
    except Exception as e:
        LOG.exception("Error generating signal for %s: %s", symbol, e)
        return None

# -------------------------
# Supabase upload (batched)
# -------------------------
def connect_supabase():
    if create_client is None:
        LOG.error("supabase client library not installed. Set up supabase-py in your environment.")
        return None
    if not SUPABASE_URL or not SUPABASE_KEY:
        LOG.error("SUPABASE_URL or SUPABASE_KEY missing.")
        return None
    try:
        client = create_client(SUPABASE_URL, SUPABASE_KEY)
        return client
    except Exception as e:
        LOG.exception("Failed to create supabase client: %s", e)
        return None

def upload_batch_to_supabase(supabase_client, table_name, batch_list):
    if supabase_client is None:
        LOG.warning("No supabase client provided; skipping upload.")
        return 0
    try:
        valid = [b for b in batch_list if b and b.get("close_price")]
        if not valid:
            return 0
        resp = supabase_client.table(table_name).insert(valid).execute()
        inserted = 0
        try:
            if hasattr(resp, "data") and resp.data:
                inserted = len(resp.data)
            elif isinstance(resp, dict) and resp.get("data"):
                inserted = len(resp["data"])
            else:
                inserted = len(valid)
        except Exception:
            inserted = len(valid)
        LOG.info("Inserted %d rows to %s", inserted, table_name)
        return inserted
    except Exception as e:
        LOG.exception("Batch upload failed: %s", e)
        return 0

# -------------------------
# Main run
# -------------------------
def main():
    start = time.time()
    raw_list = read_stock_list()
    if not raw_list:
        LOG.error("No tickers to process. Provide STOCK_LIST env or symbols.txt.")
        sys.exit(1)

    # sanitize and apply static fixes
    cleaned = sanitize_and_fix(raw_list)
    LOG.info("Sanitized list length: %d", len(cleaned))

    # Verify each ticker using yfinance probes (this may take a bit)
    resolved = []
    unresolved = []
    LOG.info("Verifying %d tickers with yfinance (this may take ~a few seconds per ticker)", len(cleaned))
    for t in cleaned:
        verified_sym, reason = verify_ticker_with_yf(t)
        if verified_sym:
            resolved.append((t, verified_sym, reason))
        else:
            # try SYMBOL_FIX mapping as last attempt
            if t in SYMBOL_FIX:
                try_map = SYMBOL_FIX[t]
                v2, r2 = verify_ticker_with_yf(try_map)
                if v2:
                    resolved.append((t, v2, "symbol_fix_map_fallback"))
                    continue
            unresolved.append((t, reason))
    LOG.info("Verification complete: %d resolved, %d unresolved", len(resolved), len(unresolved))
    if unresolved:
        LOG.debug("Unresolved sample: %s", unresolved[:10])

    # prepare final list for processing (use verified symbol if available)
    final_symbols = []
    for orig, vf, reason in resolved:
        # Use verified symbol (vf)
        final_symbols.append(vf)
    # If no resolved symbols (unlikely), fallback to cleaned list
    if not final_symbols:
        LOG.warning("No verified symbols found; falling back to sanitized list (some may fail)")
        final_symbols = cleaned

    # Connect to supabase (optional)
    supabase_client = connect_supabase()

    # Delete today's existing signals
    today = datetime.now().date().isoformat()
    if supabase_client:
        try:
            LOG.info("Deleting existing signals for date %s", today)
            del_resp = supabase_client.table(SUPABASE_TABLE).delete().eq("signal_date", today).execute()
            LOG.info("Supabase delete response handled")
        except Exception as e:
            LOG.warning("Could not delete existing signals: %s", e)

    total = len(final_symbols)
    LOG.info("Processing %d tickers (workers=%d)", total, MAX_WORKERS)

    results = []
    failed = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(generate_intraday_signal_for_symbol, s, i + 1, total): s for i, s in enumerate(final_symbols)}
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                res = fut.result()
                if res:
                    results.append(res)
                else:
                    failed.append(sym)
            except Exception as e:
                LOG.exception("Error processing %s: %s", sym, e)
                failed.append(sym)

    LOG.info("Completed generation: %d results, %d failed", len(results), len(failed))

    # Upload in batches
    uploaded = 0
    for i in range(0, len(results), BATCH_SIZE):
        batch = results[i: i + BATCH_SIZE]
        inserted = upload_batch_to_supabase(supabase_client, SUPABASE_TABLE, batch)
        uploaded += inserted
        LOG.info("Batch %d uploaded %d/%d", i // BATCH_SIZE + 1, inserted, len(batch))

    elapsed = time.time() - start
    LOG.info("SUMMARY: processed=%d failed=%d uploaded=%d elapsed=%.1fs", len(results), len(failed), uploaded, elapsed)

    # Print top-level summary for CI logs
    print("=" * 60)
    print("INTRADAY SIGNAL GENERATOR v3 SUMMARY")
    print(f"Processed: {len(results)}   Failed: {len(failed)}   Uploaded: {uploaded}")
    print(f"Elapsed: {elapsed:.1f}s")
    print("=" * 60)

    # Exit code: 0 if some processed, else 1
    sys.exit(0 if len(results) > 0 else 1)

if __name__ == "__main__":
    main()
