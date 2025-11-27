#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI TRADING SIGNALS - INTRADAY GENERATOR (15-MIN INTERVALS) - FIXED v3.0
Preserves your improved logic; hardened data handling and Supabase interactions.
"""
import os
import sys
import time
import warnings
import logging
import re
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings("ignore")

import yfinance as yf
import pandas as pd
import numpy as np

# Emoji constants
EMOJI_ROBOT = "🤖"
EMOJI_CAL = "📅"
EMOJI_CHART = "📊"
EMOJI_LINK = "🔗"
EMOJI_CHECK = "✅"
EMOJI_CROSS = "❌"
EMOJI_RUPEE = "₹"
EMOJI_MONEY = "💰"
EMOJI_GREEN = "🟢"
EMOJI_RED = "🔴"
EMOJI_WHITE = "⚪"
EMOJI_CLOCK = "🕐"
EMOJI_WARNING = "⚠️"
EMOJI_ARROW = "→"
EMOJI_ROCKET = "🚀"
EMOJI_FIRE = "🔥"
EMOJI_TARGET = "🎯"

# ================================================================
# CONFIGURATION
# ================================================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
STOCK_LIST = os.getenv("STOCK_LIST", "RELIANCE.NS,TCS.NS,INFY.NS")
# Intraday settings
INTERVAL = "15m"
INTRADAY_PERIOD = "5d"
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20"))
RETRY_DELAY = float(os.getenv("RETRY_DELAY", "0.5"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
# Market hours (IST)
MARKET_OPEN = (9, 15)
MARKET_CLOSE = (15, 30)
# IMPROVED: More conservative indicator periods
RSI_PERIOD = int(os.getenv("RSI_PERIOD", "14"))
MACD_FAST = int(os.getenv("MACD_FAST", "12"))
MACD_SLOW = int(os.getenv("MACD_SLOW", "26"))
MACD_SIGNAL = int(os.getenv("MACD_SIGNAL", "9"))
BB_PERIOD = int(os.getenv("BB_PERIOD", "20"))
VOL_PERIOD = int(os.getenv("VOL_PERIOD", "10"))
# IMPROVED: Adjusted risk management
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "1.5"))
TARGET_PCT = float(os.getenv("TARGET_PCT", "3.0"))
# IMPROVED: Signal quality thresholds
MIN_CONFIDENCE = float(os.getenv("MIN_CONFIDENCE", "60"))
MIN_VOTES_FOR_ACTION = int(os.getenv("MIN_VOTES_FOR_ACTION", "5"))
MIN_STRENGTH_SCORE = int(os.getenv("MIN_STRENGTH_SCORE", "8"))
# Supabase table (keep as 'signals' to match your v2)
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "signals")

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

if not SUPABASE_URL or not SUPABASE_KEY:
    print(f"{EMOJI_CROSS} ERROR: Missing SUPABASE credentials (SUPABASE_URL / SUPABASE_KEY)")
    sys.exit(1)

# ================================================================
# MARKET HOURS CHECK
# ================================================================
def is_market_hours(now: datetime = None):
    if now is None:
        now = datetime.now()
    current_time = (now.hour, now.minute)
    if now.weekday() > 4:
        return False, "Market closed (Weekend)"
    if current_time < MARKET_OPEN:
        return False, f"Market opens at {MARKET_OPEN[0]:02d}:{MARKET_OPEN[1]:02d}"
    elif current_time > MARKET_CLOSE:
        return False, f"Market closed at {MARKET_CLOSE[0]:02d}:{MARKET_CLOSE[1]:02d}"
    return True, "Market is open"

def should_trade_now(now: datetime = None):
    if now is None:
        now = datetime.now()
    current_mins = now.hour * 60 + now.minute
    market_open_mins = MARKET_OPEN[0] * 60 + MARKET_OPEN[1]
    market_close_mins = MARKET_CLOSE[0] * 60 + MARKET_CLOSE[1]
    if current_mins < market_open_mins + 30:
        return False, "Avoiding opening volatility (first 30 min)"
    if current_mins > market_close_mins - 30:
        return False, "Avoiding closing volatility (last 30 min)"
    return True, "Good time to trade"

# ================================================================
# SANITIZE TICKERS
# ================================================================
TICKER_RE = re.compile(r"^[A-Z0-9][A-Z0-9._-]{0,18}(?:\.[A-Z]{1,5})?$")

def sanitize_tickers(raw: str):
    items = []
    for part in raw.split(","):
        t = str(part).strip().strip(" '\"`; :()[]{}<>")
        if not t:
            continue
        tokens = t.split()
        if not tokens:
            continue
        t = tokens[0].upper()
        t = re.sub(r"[^A-Z0-9._-]", "", t)
        if t and TICKER_RE.match(t):
            items.append(t)
    # dedupe preserving order
    seen = set()
    out = []
    for t in items:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out

STOCKS = sanitize_tickers(STOCK_LIST)

# ================================================================
# STARTUP INFO
# ================================================================
print("=" * 70)
print(f"{EMOJI_ROBOT} AI TRADING SIGNALS - INTRADAY GENERATOR v3.0 (FIXED)")
print("=" * 70)
print(f"{EMOJI_CAL} Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}")
print(f"{EMOJI_CHART} Stocks to analyze: {len(STOCKS)}")
print(f"{EMOJI_CHART} Interval: {INTERVAL} candles")
print(f"{EMOJI_LINK} Supabase URL: {SUPABASE_URL[:30]}...")
print(f"⚡ Max workers: {MAX_WORKERS}")
print(f"{EMOJI_TARGET} Stop Loss: {STOP_LOSS_PCT}% | Target: {TARGET_PCT}%")
print(f"🎯 Quality filters: Min confidence {MIN_CONFIDENCE}%, Min votes {MIN_VOTES_FOR_ACTION}/6")
print("=" * 70)
print()

is_open, msg = is_market_hours()
can_trade, trade_msg = should_trade_now()
print(f"📍 Market status: {msg}")
print(f"📍 Trading status: {trade_msg}")
print()

# ================================================================
# SUPABASE CONNECTION
# ================================================================
try:
    from supabase import create_client
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    print(f"{EMOJI_CHECK} Connected to Supabase successfully")
    print()
except Exception as e:
    print(f"{EMOJI_CROSS} Failed to connect to Supabase: {e}")
    sys.exit(1)

# ================================================================
# HELPER FUNCTIONS
# ================================================================
def get_value(series, idx):
    """
    Safely extract single numeric value from a pandas Series-like object.
    idx can be negative index (-1 for last).
    Returns None if value missing or not valid.
    """
    try:
        if series is None:
            return None
        # If user passed a DataFrame column (Series) or numpy array-like
        if isinstance(series, (pd.Series, pd.DataFrame, np.ndarray, list)):
            # convert to Series to use iloc safely
            if isinstance(series, pd.DataFrame):
                series = series.iloc[:, 0]
            ser = pd.Series(series)
            if len(ser) == 0:
                return None
            # convert negative idx
            if idx < 0:
                pos = len(ser) + idx
            else:
                pos = idx
            if pos < 0 or pos >= len(ser):
                return None
            val = ser.iloc[pos]
        else:
            val = series
        if isinstance(val, pd.Series) or isinstance(val, np.ndarray):
            # pick first element if it's a nested structure
            if len(val) == 0:
                return None
            val = val.iloc[0] if isinstance(val, pd.Series) else val[0]
        if pd.isna(val):
            return None
        float_val = float(val)
        if float_val <= 0 or float_val < 0.01:
            # treat extremely small values as invalid (configurable)
            return None
        return float_val
    except (IndexError, ValueError, TypeError) as e:
        return None

def validate_data_quality(data: pd.DataFrame):
    """
    Returns (bool, message). Checks:
     - enough candles
     - too many gaps
     - staleness (last timestamp)
     - extreme moves count
    """
    try:
        if data is None or data.empty:
            return False, "Empty dataset"
        if len(data) < 50:
            return False, f"Insufficient data ({len(data)} candles)"
        # check for gaps in the index assuming a DatetimeIndex
        try:
            idx = pd.DatetimeIndex(data.index)
            # compute diffs
            diffs = idx.to_series().diff()
            expected_diff = pd.Timedelta(minutes=15)
            # count gaps greater than 2 * expected period
            gaps = (diffs > expected_diff * 2).sum()
            if gaps > 3:
                return False, f"Too many data gaps ({int(gaps)})"
        except Exception:
            # if index isn't datetime, skip gap check but warn later if needed
            pass

        # last timestamp staleness check
        last_time = None
        try:
            last_time = pd.Timestamp(data.index[-1])
            # strip timezone if present
            if getattr(last_time, "tzinfo", None) is not None or getattr(last_time, "tz", None) is not None:
                try:
                    last_time = last_time.tz_convert(None)
                except Exception:
                    try:
                        last_time = last_time.tz_localize(None)
                    except Exception:
                        pass
        except Exception:
            last_time = pd.Timestamp.now()

        now = pd.Timestamp.now()
        staleness_mins = (now - last_time).total_seconds() / 60
        if staleness_mins > 30:
            return False, f"Stale data ({staleness_mins:.0f} min old)"

        # basic sanity check on price jumps
        if "Close" in data.columns:
            price_changes = data["Close"].pct_change().abs()
            extreme_moves = (price_changes > 0.10).sum()
            if extreme_moves > 2:
                return False, f"Extreme volatility detected ({int(extreme_moves)} spikes)"
        return True, "Data quality OK"
    except Exception as e:
        return False, f"Data validation error: {e}"

def calculate_intraday_indicators(prices: pd.DataFrame):
    """
    Calculates RSI, MACD, Bollinger Bands, volume average, EMAs and ATR.
    Returns dict of pandas Series.
    """
    close = prices["Close"].astype(float)
    high = prices["High"].astype(float)
    low = prices["Low"].astype(float)
    volume = prices["Volume"].astype(float) if "Volume" in prices.columns else pd.Series([np.nan]*len(prices), index=prices.index)

    # RSI (simple moving average of gains/losses)
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
    macd_histogram = macd - macd_signal

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

    # ATR (true range)
    tr1 = high - low
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(14).mean()

    return {
        "rsi": rsi,
        "macd": macd,
        "macd_signal": macd_signal,
        "macd_histogram": macd_histogram,
        "bb_upper": bb_upper,
        "bb_lower": bb_lower,
        "bb_middle": bb_middle,
        "vol_avg": vol_avg,
        "ema_20": ema_20,
        "ema_50": ema_50,
        "atr": atr,
    }

def fetch_intraday_data(ticker: str, max_retries: int = MAX_RETRIES):
    """
    Fetch intraday history using yfinance with retry and simple validation.
    Returns DataFrame or None.
    """
    for attempt in range(1, max_retries + 1):
        try:
            if attempt > 1:
                time.sleep(RETRY_DELAY)
            stock = yf.Ticker(ticker)
            data = stock.history(period=INTRADAY_PERIOD, interval=INTERVAL, auto_adjust=True)
            # Flatten multi-index columns if present (yfinance sometimes returns a MultiIndex)
            if hasattr(data, "columns") and isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)
            if data is None or data.empty:
                raise ValueError("Empty data")
            if len(data) < 50:
                raise ValueError(f"Only {len(data)} candles")
            if "Close" not in data.columns:
                raise ValueError("Missing Close column")
            valid_closes = data["Close"].dropna()
            if len(valid_closes) < 50:
                raise ValueError(f"Only {len(valid_closes)} valid closes")
            last_close = valid_closes.iloc[-1]
            if pd.isna(last_close) or float(last_close) <= 0:
                raise ValueError(f"Invalid last close: {last_close}")
            # ensure proper dtypes
            data = data.astype({c: "float64" for c in ["Open", "High", "Low", "Close"] if c in data.columns}, errors="ignore")
            return data
        except Exception as e:
            logging.debug(f"Attempt {attempt} fetch {ticker} failed: {e}")
            if attempt == max_retries:
                logging.info(f"{EMOJI_CROSS} {ticker}: Failed to fetch data after {max_retries} attempts ({e})")
                return None
    return None

def determine_trend(ema_20, ema_50, close_price):
    try:
        if ema_20 > ema_50 * 1.002 and close_price > ema_20:
            return "Uptrend"
        elif ema_20 < ema_50 * 0.998 and close_price < ema_20:
            return "Downtrend"
        else:
            return "Sideways"
    except Exception:
        return "Sideways"

def generate_intraday_signal(stock_symbol, stock_num=0, total=0):
    pretty = stock_symbol.replace(".NS", "")
    prefix = f"[{stock_num}/{total}] " if total > 0 else ""
    try:
        data = fetch_intraday_data(stock_symbol)
        if data is None:
            print(f"{prefix}{EMOJI_CROSS} {pretty}: Failed to fetch data")
            return None

        is_valid, quality_msg = validate_data_quality(data)
        if not is_valid:
            print(f"{prefix}{EMOJI_WARNING} {pretty}: {quality_msg}")
            return None

        required_cols = ["Close", "Open", "High", "Low", "Volume"]
        missing = [c for c in required_cols if c not in data.columns]
        if missing:
            print(f"{prefix}{EMOJI_CROSS} {pretty}: Missing columns: {missing}")
            return None

        last_idx = -1
        close_price = get_value(data["Close"], last_idx)
        open_price = get_value(data["Open"], last_idx)
        high_price = get_value(data["High"], last_idx)
        low_price = get_value(data["Low"], last_idx)
        volume = get_value(data["Volume"], last_idx)

        if close_price is None or close_price <= 0:
            print(f"{prefix}{EMOJI_CROSS} {pretty}: Invalid close price")
            return None

        indicators = calculate_intraday_indicators(data)
        rsi_val = get_value(indicators["rsi"], last_idx) or 50.0
        macd_val = get_value(indicators["macd"], last_idx) or 0.0
        macd_sig_val = get_value(indicators["macd_signal"], last_idx) or 0.0
        macd_hist = get_value(indicators["macd_histogram"], last_idx) or 0.0
        bb_up_val = get_value(indicators["bb_upper"], last_idx) or close_price * 1.02
        bb_low_val = get_value(indicators["bb_lower"], last_idx) or close_price * 0.98
        bb_mid_val = get_value(indicators["bb_middle"], last_idx) or close_price
        vol_avg = get_value(indicators["vol_avg"], last_idx)
        ema_20 = get_value(indicators["ema_20"], last_idx) or close_price
        ema_50 = get_value(indicators["ema_50"], last_idx) or close_price
        atr_val = get_value(indicators["atr"], last_idx) or (close_price * 0.02)

        votes = []
        strength_score = 0
        reasons = []

        trend = determine_trend(ema_20, ema_50, close_price)
        if trend == "Uptrend":
            strength_score += 3
            reasons.append("Uptrend")
        elif trend == "Downtrend":
            strength_score += 3
            reasons.append("Downtrend")

        # RSI rules
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

        # MACD rules
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

        # Bollinger Band position
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

        # Volume dynamics
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

        # Candle body shape
        candle_body = (close_price - open_price) if open_price else 0
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

        # Tally votes
        buy_count = votes.count("Buy")
        sell_count = votes.count("Sell")
        hold_count = votes.count("Hold")

        signal = "Hold"
        base_conf = 30.0

        # Decision logic based on votes and trend
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

        # bonus from strength_score (capped)
        bonus = min(max(strength_score, 0) * 2, 25)
        confidence = min(base_conf + bonus, 80)

        # enforce minimum confidence requirement
        if signal != "Hold" and confidence < MIN_CONFIDENCE:
            signal = "Hold"
            confidence = 50.0

        # compute SL/Target depending on side
        stop_loss = None
        target = None
        if signal == "Buy":
            stop_loss = round(close_price * (1 - STOP_LOSS_PCT / 100.0), 2)
            target = round(close_price * (1 + TARGET_PCT / 100.0), 2)
        elif signal == "Sell":
            stop_loss = round(close_price * (1 + STOP_LOSS_PCT / 100.0), 2)
            target = round(close_price * (1 - TARGET_PCT / 100.0), 2)

        risk_reward = None
        if stop_loss is not None and target is not None:
            risk = abs(close_price - stop_loss)
            reward = abs(target - close_price)
            risk_reward = round((reward / risk), 2) if risk > 0 else None

        result = {
            "symbol": pretty,
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
            "reasons": reasons[:6],  # truncate reason list to keep payload small
            "strength_score": int(strength_score),
        }

        emoji = EMOJI_GREEN if signal == "Buy" else EMOJI_RED if signal == "Sell" else EMOJI_WHITE
        sl_info = f"SL:{EMOJI_RUPEE}{stop_loss} T:{EMOJI_RUPEE}{target}" if stop_loss and target else ""
        trend_emoji = "📈" if trend == "Uptrend" else "📉" if trend == "Downtrend" else "➡️"
        reason_str = ", ".join(reasons[:2]) if reasons else ""

        print(f"{prefix}{emoji} {pretty:12s} {signal:4s} ({confidence:.0f}%) @ {EMOJI_RUPEE}{close_price:.2f} {sl_info} {trend_emoji} [{reason_str}]")
        return result

    except Exception as e:
        logging.error(f"{prefix}{EMOJI_CROSS} {pretty}: Error generating signal: {e}")
        return None

def upload_batch(batch_data):
    """
    Insert list of dicts to Supabase. Handles different client return shapes.
    Returns number of rows inserted (best-effort).
    """
    try:
        valid_data = [d for d in batch_data if d and d.get("close_price", 0) > 0]
        if not valid_data:
            print(f"{EMOJI_WARNING} No valid data in batch")
            return 0
        resp = supabase.table(SUPABASE_TABLE).insert(valid_data).execute()
        # supabase-py returned types vary by version: try to detect
        inserted = 0
        try:
            # modern version: object with 'data' attr
            if hasattr(resp, "data") and resp.data:
                inserted = len(resp.data)
            elif isinstance(resp, dict) and "data" in resp and resp["data"]:
                inserted = len(resp["data"])
            else:
                # fallback: assume success for all
                inserted = len(valid_data)
        except Exception:
            inserted = len(valid_data)
        return inserted
    except Exception as e:
        logging.error(f"Batch upload failed: {e}")
        return 0

def main():
    print(f"{EMOJI_ROCKET} Starting intraday signal generation v3.0...\n")
    try:
        today = datetime.now().date().isoformat()
        print(f"{EMOJI_WARNING} Deleting today's old signals (date: {today})...")
        try:
            delete_resp = supabase.table(SUPABASE_TABLE).delete().eq("signal_date", today).execute()
            deleted_count = 0
            if hasattr(delete_resp, "data") and delete_resp.data:
                deleted_count = len(delete_resp.data)
            elif isinstance(delete_resp, dict) and delete_resp.get("data"):
                deleted_count = len(delete_resp.get("data"))
            print(f"{EMOJI_CHECK} Deleted {deleted_count} old signals\n")
        except Exception as e:
            print(f"{EMOJI_WARNING} Could not delete old signals: {e}\n")
    except Exception as e:
        print(f"{EMOJI_WARNING} Pre-cleanup failed: {e}\n")

    start_time = time.time()
    results = []
    failed_tickers = []

    total = len(STOCKS)
    if total == 0:
        print(f"{EMOJI_WARNING} No tickers to process. Set STOCK_LIST environment variable.")
        sys.exit(1)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_stock = {
            executor.submit(generate_intraday_signal, stock, i + 1, total): stock
            for i, stock in enumerate(STOCKS)
        }
        for future in as_completed(future_to_stock):
            stock = future_to_stock[future]
            try:
                signal = future.result()
                if signal:
                    results.append(signal)
                else:
                    failed_tickers.append(stock)
            except Exception as e:
                logging.error(f"Error processing {stock}: {e}")
                failed_tickers.append(stock)

    print(f"\n{EMOJI_ROCKET} Uploading {len(results)} signals...")
    uploaded = 0
    for i in range(0, len(results), BATCH_SIZE):
        batch = results[i : i + BATCH_SIZE]
        count = upload_batch(batch)
        uploaded += count
        print(f" {EMOJI_CHECK} Batch {i // BATCH_SIZE + 1}: {count}/{len(batch)} uploaded")

    elapsed = time.time() - start_time
    success = len(results)
    failed = len(failed_tickers)

    print()
    print("=" * 70)
    print(f"{EMOJI_CHART} INTRADAY SUMMARY (v3.0)")
    print("=" * 70)
    print(f"{EMOJI_CHECK} Successfully processed: {success} stocks")
    print(f"{EMOJI_CROSS} Failed: {failed} stocks")
    print(f"⚡ Total time: {elapsed:.1f}s ({elapsed/max(len(STOCKS),1):.2f}s per stock)")
    upload_rate = 100.0 * uploaded / max(success, 1) if success > 0 else 0.0
    print(f"⚡ Upload rate: {uploaded}/{success} ({upload_rate:.1f}%)")

    if results:
        df = pd.DataFrame(results)
        buy_signals = df[df["signal"] == "Buy"]
        sell_signals = df[df["signal"] == "Sell"]
        hold_signals = df[df["signal"] == "Hold"]
        high_quality_buys = buy_signals[buy_signals["confidence"] > 70]
        high_quality_sells = sell_signals[sell_signals["confidence"] > 70]

        print()
        print(f"{EMOJI_GREEN} Buy signals: {len(buy_signals)} (High quality: {len(high_quality_buys)})")
        print(f"{EMOJI_RED} Sell signals: {len(sell_signals)} (High quality: {len(high_quality_sells)})")
        print(f"{EMOJI_WHITE} Hold signals: {len(hold_signals)}")
        print(f"{EMOJI_CHART} Average confidence: {df['confidence'].mean():.1f}%")

        if len(high_quality_buys) > 0:
            print(f"\n{EMOJI_FIRE} TOP 3 HIGH-QUALITY BUY SIGNALS (>70% confidence):")
            for _, row in high_quality_buys.nlargest(3, "confidence").iterrows():
                rr = f"R:R {row['risk_reward']}" if row["risk_reward"] else ""
                print(f" {EMOJI_GREEN} {row['symbol']:12s} {row['confidence']:.0f}% {EMOJI_RUPEE}{row['close_price']:.2f} → T:{EMOJI_RUPEE}{row['target']} {rr}")
        elif len(buy_signals) > 0:
            print(f"\n{EMOJI_FIRE} TOP 3 BUY SIGNALS:")
            for _, row in buy_signals.nlargest(3, "confidence").iterrows():
                rr = f"R:R {row['risk_reward']}" if row["risk_reward"] else ""
                print(f" {EMOJI_GREEN} {row['symbol']:12s} {row['confidence']:.0f}% {EMOJI_RUPEE}{row['close_price']:.2f} → T:{EMOJI_RUPEE}{row['target']} {rr}")

        if len(high_quality_sells) > 0:
            print(f"\n{EMOJI_FIRE} TOP 3 HIGH-QUALITY SELL SIGNALS (>70% confidence):")
            for _, row in high_quality_sells.nlargest(3, "confidence").iterrows():
                rr = f"R:R {row['risk_reward']}" if row["risk_reward"] else ""
                print(f" {EMOJI_RED} {row['symbol']:12s} {row['confidence']:.0f}% {EMOJI_RUPEE}{row['close_price']:.2f} → T:{EMOJI_RUPEE}{row['target']} {rr}")
        elif len(sell_signals) > 0:
            print(f"\n{EMOJI_FIRE} TOP 3 SELL SIGNALS:")
            for _, row in sell_signals.nlargest(3, "confidence").iterrows():
                rr = f"R:R {row['risk_reward']}" if row["risk_reward"] else ""
                print(f" {EMOJI_RED} {row['symbol']:12s} {row['confidence']:.0f}% {EMOJI_RUPEE}{row['close_price']:.2f} → T:{EMOJI_RUPEE}{row['target']} {rr}")

        valid_rr = df[df["risk_reward"].notna()]["risk_reward"]
        if len(valid_rr) > 0:
            print(f"\n{EMOJI_TARGET} Average Risk:Reward Ratio: {valid_rr.mean():.2f}")

    if failed_tickers:
        print(f"\n{EMOJI_WARNING} Failed tickers: {', '.join([t.replace('.NS', '') for t in failed_tickers[:10]])}")
        if len(failed_tickers) > 10:
            print(f"   ... and {len(failed_tickers) - 10} more")

    print()
    print("=" * 70)
    print(f"{EMOJI_CHECK} INTRADAY SIGNAL GENERATION COMPLETE (v3.0)!")
    print(f"{EMOJI_CLOCK} Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}")
    print(f"📝 Improvements: Better trend filtering, stricter vote requirements, data quality validation, realistic confidence caps")
    print("=" * 70)
    # success exit code if at least one signal generated
    sys.exit(0 if success > 0 else 1)

if __name__ == "__main__":
    main()
