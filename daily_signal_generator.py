#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI TRADING SIGNALS - INTRADAY GENERATOR (15-MIN INTERVALS) - PROFESSIONAL v2.5
Incorporates Liquidity, ATR-based Risk, and Circuit Detection for Real-World Safety.
"""

import os
import sys
import time
import warnings
import logging
import re
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
warnings.filterwarnings('ignore')

import yfinance as yf
import pandas as pd
import numpy as np
import pytz

# Define IST Timezone
IST_TZ = pytz.timezone('Asia/Kolkata')

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
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
STOCK_LIST = os.getenv('STOCK_LIST', 'RELIANCE.NS,TCS.NS,INFY.NS')

# Intraday settings
INTERVAL = '15m'
INTRADAY_PERIOD = '5d'
MAX_WORKERS = 10
BATCH_SIZE = 20
RETRY_DELAY = 0.5
MAX_RETRIES = 3

# Market hours (IST)
MARKET_OPEN = (9, 15)
MARKET_CLOSE = (15, 30)

# IMPROVED: Indicator periods
RSI_PERIOD = 14
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
BB_PERIOD = 20
VOL_PERIOD = 20 # Increased volume period for better average

# CRITICAL FIX: ATR-based risk management
ATR_MULTIPLIER_SL = 1.5  # Stop Loss is 1.5 * ATR away
ATR_MULTIPLIER_TP = 3.0  # Target is 3.0 * ATR away

# CRITICAL FIX: Liquidity and Safety Filters
MIN_PRICE = 50.0            # Minimum closing price
MIN_AVG_VOLUME = 200000     # Minimum average volume over 20 periods
MAX_CIRCUIT_PCT = 0.18      # Skip if movement is > 18% (to avoid near-circuit halts)

# IMPROVED: Signal quality thresholds
MIN_CONFIDENCE = 60
MIN_VOTES_FOR_ACTION = 5
MIN_STRENGTH_SCORE = 8

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

if not SUPABASE_URL or not SUPABASE_KEY:
    print(f"{EMOJI_CROSS} ERROR: Missing SUPABASE credentials")
    sys.exit(1)

# ================================================================
# MARKET HOURS CHECK (NOW USES IST_TZ)
# ================================================================
def is_market_hours():
    now = datetime.now(IST_TZ)
    current_time = (now.hour, now.minute)
    if now.weekday() > 4:
        return False, "Market closed (Weekend)"
    if current_time < MARKET_OPEN:
        return False, f"Market opens at {MARKET_OPEN[0]:02d}:{MARKET_OPEN[1]:02d}"
    elif current_time > MARKET_CLOSE:
        return False, f"Market closed at {MARKET_CLOSE[0]:02d}:{MARKET_CLOSE[1]:02d}"
    return True, "Market is open"

def should_trade_now():
    now = datetime.now(IST_TZ)
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
TICKER_RE = re.compile(r'^[A-Z0-9][A-Z0-9._-]{0,18}(?:\.[A-Z]{1,5})?$')

def sanitize_tickers(raw: str):
    items = []
    for part in raw.split(','):
        t = str(part).strip().strip(" '\"`; :()[]{}<>")
        if not t:
            continue
        tokens = t.split()
        if not tokens:
            continue
        t = tokens[0].upper()
        t = re.sub(r'[^A-Z0-9._-]', '', t)
        if t and TICKER_RE.match(t):
            items.append(t)
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
print(f"{EMOJI_ROBOT} AI TRADING SIGNALS - INTRADAY GENERATOR v2.5 (PROFESSIONAL)")
print("=" * 70)
print(f"{EMOJI_CAL} Date: {datetime.now(IST_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')}")
print(f"{EMOJI_CHART} Stocks to analyze: {len(STOCKS)}")
print(f"{EMOJI_CHART} Interval: {INTERVAL} candles")
print(f"{EMOJI_LINK} Supabase URL: {SUPABASE_URL[:30]}...")
print(f"⚡ Max workers: {MAX_WORKERS}")
print(f"{EMOJI_TARGET} SL/TP (ATR Multipliers): {ATR_MULTIPLIER_SL} / {ATR_MULTIPLIER_TP} (Risk:Reward {ATR_MULTIPLIER_TP/ATR_MULTIPLIER_SL:.1f}:1)")
print(f"🎯 Safety Filters: Min Price {EMOJI_RUPEE}{MIN_PRICE:.0f}, Min Avg Vol {MIN_AVG_VOLUME/1000:.0f}K")
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
    print(f"{EMOJI_CROSS} Failed to connect: {e}")
    sys.exit(1)

# ================================================================
# HELPER FUNCTIONS
# ================================================================
def get_value(series, idx):
    try:
        if series is None or len(series) == 0:
            return None
        val = series.iloc[idx]
        if isinstance(val, pd.Series):
            val = val.iloc[0] if len(val) > 0 else None
        if pd.isna(val):
            return None
        float_val = float(val)
        if float_val <= 0 or float_val < 0.01:
            return None
        return float_val
    except (IndexError, ValueError, TypeError):
        return None

def validate_data_quality(data):
    """Uses timezone-aware timestamps for staleness check and gap detection."""
    if len(data) < 30:
        return False, f"Insufficient data ({len(data)} candles)"
    
    data_index = data.index
    if data_index.tz is None:
        # Localize if somehow yfinance missed the timezone info
        data_index = data_index.tz_localize(IST_TZ)
        data.index = data_index
    
    if len(data) > 1:
        time_diff = data_index.to_series().diff()
        expected_diff = pd.Timedelta(minutes=15)
        gaps = (time_diff > expected_diff * 5).sum()
        if gaps > 10:
            return False, f"Too many data gaps ({gaps})"
    
    last_time = data_index[-1]
    
    # Use timezone-aware 'now' for comparison
    now_aware = pd.Timestamp.now(IST_TZ)
    
    # Calculate difference between two timezone-aware objects
    staleness_mins = (now_aware - last_time).total_seconds() / 60
    if staleness_mins > 1440:
        return False, f"Stale data ({staleness_mins:.0f} min old)"
    
    if 'Close' in data.columns:
        price_changes = data['Close'].pct_change().abs()
        extreme_moves = (price_changes > 0.20).sum()
        if extreme_moves > 5:
            return False, f"Extreme volatility detected ({extreme_moves} spikes)"
    
    return True, "Data quality OK"

def calculate_intraday_indicators(prices):
    close = prices['Close']
    high = prices['High']
    low = prices['Low']
    
    # CRITICAL FIX: Implement Wilder's Smoothing for more accurate RSI
    delta = close.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    
    avg_gain = gain.ewm(com=RSI_PERIOD - 1, adjust=False).mean()
    avg_loss = loss.ewm(com=RSI_PERIOD - 1, adjust=False).mean()
    
    rs = avg_gain / avg_loss.replace(0, 0.0001)
    rsi = 100 - (100 / (1 + rs))
    
    ema_fast = close.ewm(span=MACD_FAST, adjust=False).mean()
    ema_slow = close.ewm(span=MACD_SLOW, adjust=False).mean()
    macd = ema_fast - ema_slow
    macd_signal = macd.ewm(span=MACD_SIGNAL, adjust=False).mean()
    macd_histogram = macd - macd_signal
    
    sma = close.rolling(BB_PERIOD).mean()
    std = close.rolling(BB_PERIOD).std()
    bb_upper = sma + (2 * std)
    bb_lower = sma - (2 * std)
    bb_middle = sma
    
    vol_avg = prices['Volume'].rolling(VOL_PERIOD).mean()
    ema_20 = close.ewm(span=20, adjust=False).mean()
    ema_50 = close.ewm(span=50, adjust=False).mean()
    
    tr1 = high - low
    tr2 = abs(high - close.shift())
    tr3 = abs(low - close.shift())
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.ewm(com=14 - 1, adjust=False).mean() # Wilder's smoothing for ATR
    
    return {
        'rsi': rsi,
        'macd': macd,
        'macd_signal': macd_signal,
        'macd_histogram': macd_histogram,
        'bb_upper': bb_upper,
        'bb_lower': bb_lower,
        'bb_middle': bb_middle,
        'vol_avg': vol_avg,
        'ema_20': ema_20,
        'ema_50': ema_50,
        'atr': atr
    }

def fetch_intraday_data(ticker: str, max_retries=MAX_RETRIES):
    """Fetches data, ensures index is timezone-aware and converted to IST."""
    for attempt in range(1, max_retries + 1):
        try:
            if attempt > 1:
                time.sleep(RETRY_DELAY)
            
            stock = yf.Ticker(ticker)
            data = stock.history(period=INTRADAY_PERIOD, interval=INTERVAL, auto_adjust=True)
            
            if hasattr(data, "columns") and isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)
            
            if data is None or data.empty:
                raise ValueError("Empty data")
            
            if len(data) < 30:
                raise ValueError(f"Only {len(data)} candles")
            
            if 'Close' not in data.columns:
                raise ValueError("Missing Close column")
            
            # Ensure index is in IST
            if data.index.tz is not None:
                data.index = data.index.tz_convert(IST_TZ)
            else:
                data.index = data.index.tz_localize(IST_TZ)

            return data
            
        except Exception as e:
            if attempt == max_retries:
                logging.debug(f"Failed {ticker} after {max_retries} attempts: {e}")
                return None
            else:
                logging.debug(f"Attempt {attempt} failed for {ticker}: {e}")
    return None

def determine_trend(ema_20, ema_50, close_price):
    if ema_20 > ema_50 * 1.002 and close_price > ema_20:
        return 'Uptrend'
    elif ema_20 < ema_50 * 0.998 and close_price < ema_20:
        return 'Downtrend'
    else:
        return 'Sideways'

def generate_intraday_signal(stock_symbol, stock_num=0, total=0):
    pretty = stock_symbol.replace('.NS', '').replace('.BO', '')
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
        
        required_cols = ['Close', 'Open', 'High', 'Low', 'Volume']
        missing = [c for c in required_cols if c not in data.columns]
        if missing:
            print(f"{prefix}{EMOJI_CROSS} {pretty}: Missing columns: {missing}")
            return None
        
        last_idx = -1
        close_price = get_value(data['Close'], last_idx)
        open_price = get_value(data['Open'], last_idx)
        high_price = get_value(data['High'], last_idx)
        low_price = get_value(data['Low'], last_idx)
        volume = get_value(data['Volume'], last_idx)
        
        if close_price is None or close_price <= 0:
            print(f"{prefix}{EMOJI_CROSS} {pretty}: Invalid close price")
            return None

        # CRITICAL FIX 1: Liquidity and Price Filters
        avg_volume_20 = data['Volume'].tail(20).mean()
        if close_price < MIN_PRICE:
            print(f"{prefix}{EMOJI_WARNING} {pretty}: Too cheap ({EMOJI_RUPEE}{close_price:.2f}) - Skipped")
            return None
        if avg_volume_20 < MIN_AVG_VOLUME:
            print(f"{prefix}{EMOJI_WARNING} {pretty}: Illiquid (Avg Vol {avg_volume_20/1000:.0f}K) - Skipped")
            return None

        indicators = calculate_intraday_indicators(data)
        
        rsi_val = get_value(indicators['rsi'], last_idx) or 50.0
        macd_val = get_value(indicators['macd'], last_idx) or 0.0
        macd_sig_val = get_value(indicators['macd_signal'], last_idx) or 0.0
        macd_hist = get_value(indicators['macd_histogram'], last_idx) or 0.0
        bb_up_val = get_value(indicators['bb_upper'], last_idx) or close_price * 1.02
        bb_low_val = get_value(indicators['bb_lower'], last_idx) or close_price * 0.98
        bb_mid_val = get_value(indicators['bb_middle'], last_idx) or close_price
        vol_avg = get_value(indicators['vol_avg'], last_idx)
        ema_20 = get_value(indicators['ema_20'], last_idx) or close_price
        ema_50 = get_value(indicators['ema_50'], last_idx) or close_price
        atr_val = get_value(indicators['atr'], last_idx) or (close_price * 0.005) # Smaller fallback ATR
        
        # CRITICAL FIX 3: Circuit Detection
        if len(data) >= 2 and 'Close' in data.columns:
            prev_close_val = get_value(data['Close'], -2)
            if prev_close_val and prev_close_val > 0:
                daily_move_pct = abs(close_price - prev_close_val) / prev_close_val
                if daily_move_pct > MAX_CIRCUIT_PCT:
                    print(f"{prefix}{EMOJI_WARNING} {pretty}: Near circuit limit ({daily_move_pct*100:.1f}%) - Skipped")
                    return None
        
        votes = []
        strength_score = 0
        reasons = []
        
        trend = determine_trend(ema_20, ema_50, close_price)
        
        if trend == 'Uptrend':
            strength_score += 3
            reasons.append(f"Uptrend")
        elif trend == 'Downtrend':
            strength_score += 3
            reasons.append(f"Downtrend")
        
        if rsi_val < 30:
            votes.append('Buy')
            strength_score += 3
            reasons.append(f"RSI oversold ({rsi_val:.1f})")
        elif rsi_val > 70:
            votes.append('Sell')
            strength_score += 3
            reasons.append(f"RSI overbought ({rsi_val:.1f})")
        elif rsi_val < 40 and trend == 'Uptrend':
            votes.append('Buy')
            strength_score += 1
            reasons.append(f"RSI pullback")
        elif rsi_val > 60 and trend == 'Downtrend':
            votes.append('Sell')
            strength_score += 1
            reasons.append(f"RSI bounce")
        else:
            votes.append('Hold')
        
        if macd_val > macd_sig_val and macd_hist > 0:
            votes.append('Buy')
            strength_score += 2
            reasons.append("MACD bullish")
        elif macd_val < macd_sig_val and macd_hist < 0:
            votes.append('Sell')
            strength_score += 2
            reasons.append("MACD bearish")
        else:
            votes.append('Hold')
        
        bb_width = bb_up_val - bb_low_val
        bb_position = (close_price - bb_low_val) / bb_width if bb_width > 0 else 0.5
        
        if bb_position < 0.15:
            votes.append('Buy')
            strength_score += 2
            reasons.append(f"Near BB lower")
        elif bb_position > 0.85:
            votes.append('Sell')
            strength_score += 2
            reasons.append(f"Near BB upper")
        else:
            votes.append('Hold')
        
        # CRITICAL FIX 4: Improved Volume Confirmation
        if volume and vol_avg:
            # Compare current volume to the trailing 20-period average ending *before* this candle
            vol_avg_prior = data['Volume'].rolling(VOL_PERIOD).mean().iloc[-2] if len(data) >= VOL_PERIOD else vol_avg
            
            vol_ratio = volume / vol_avg_prior if vol_avg_prior > 0 else 0
            
            if vol_ratio > 2.0:
                if close_price > open_price:
                    votes.append('Buy')
                    strength_score += 3
                    reasons.append(f"High vol bullish (Vol Ratio {vol_ratio:.1f})")
                elif close_price < open_price:
                    votes.append('Sell')
                    strength_score += 3
                    reasons.append(f"High vol bearish (Vol Ratio {vol_ratio:.1f})")
                else:
                    votes.append('Hold')
            elif vol_ratio < 0.6:
                votes.append('Hold')
                strength_score -= 2
            else:
                votes.append('Hold')
        else:
            votes.append('Hold')
        
        candle_body = close_price - open_price if open_price else 0
        candle_range = high_price - low_price if (high_price and low_price) else 0
        
        if candle_range > 0:
            body_ratio = abs(candle_body) / candle_range
            
            if candle_body > 0 and body_ratio > 0.7:
                votes.append('Buy')
                strength_score += 2
                reasons.append("Strong bull candle")
            elif candle_body < 0 and body_ratio > 0.7:
                votes.append('Sell')
                strength_score += 2
                reasons.append("Strong bear candle")
            elif body_ratio < 0.1:
                votes.append('Hold')
                strength_score -= 1
            else:
                votes.append('Hold')
        else:
            votes.append('Hold')
        
        buy_count = votes.count('Buy')
        sell_count = votes.count('Sell')
        hold_count = votes.count('Hold')
        
        signal = 'Hold'
        base_conf = 30
        
        if buy_count >= MIN_VOTES_FOR_ACTION and trend != 'Downtrend':
            signal = 'Buy'
            base_conf = (buy_count / 6) * 60
        elif sell_count >= MIN_VOTES_FOR_ACTION and trend != 'Uptrend':
            signal = 'Sell'
            base_conf = (sell_count / 6) * 60
        elif buy_count == 4 and trend == 'Uptrend' and strength_score >= MIN_STRENGTH_SCORE:
            signal = 'Buy'
            base_conf = (buy_count / 6) * 50
        elif sell_count == 4 and trend == 'Downtrend' and strength_score >= MIN_STRENGTH_SCORE:
            signal = 'Sell'
            base_conf = (sell_count / 6) * 50
        else:
            signal = 'Hold'
            base_conf = 30
        
        bonus = min(max(strength_score, 0) * 2, 25)
        confidence = min(base_conf + bonus, 80)
        
        if signal != 'Hold' and confidence < MIN_CONFIDENCE:
            signal = 'Hold'
            confidence = 50
        
        # CRITICAL FIX 2: ATR-based Stop Loss & Target
        if signal == 'Buy':
            stop_loss = round(close_price - (atr_val * ATR_MULTIPLIER_SL), 2)
            target = round(close_price + (atr_val * ATR_MULTIPLIER_TP), 2)
        elif signal == 'Sell':
            stop_loss = round(close_price + (atr_val * ATR_MULTIPLIER_SL), 2)
            target = round(close_price - (atr_val * ATR_MULTIPLIER_TP), 2)
        else:
            stop_loss = None
            target = None
        
        risk_reward = None
        if stop_loss and target:
            risk = abs(close_price - stop_loss)
            reward = abs(target - close_price)
            # Ensure risk is not near zero to prevent division errors
            risk_reward = round(reward / risk, 2) if risk > (close_price * 0.001) else None
        
        now_ist = datetime.now(IST_TZ)
        
        result = {
            'symbol': pretty,
            'signal': signal,
            'confidence': round(float(confidence), 1),
            'close_price': round(float(close_price), 2),
            'rsi': round(float(rsi_val), 1),
            'macd': round(float(macd_val), 3),
            'stop_loss': stop_loss,
            'target': target,
            'risk_reward': risk_reward,
            'buy_votes': int(buy_count),
            'sell_votes': int(sell_count),
            'hold_votes': int(hold_count),
            'signal_date': now_ist.date().isoformat(),
            'signal_time': now_ist.strftime('%H:%M:%S'),
            'interval': INTERVAL
        }
        
        emoji = EMOJI_GREEN if signal == 'Buy' else EMOJI_RED if signal == 'Sell' else EMOJI_WHITE
        sl_info = f"SL:{EMOJI_RUPEE}{stop_loss} T:{EMOJI_RUPEE}{target}" if stop_loss and target else ""
        trend_emoji = "📈" if trend == 'Uptrend' else "📉" if trend == 'Downtrend' else "➡️"
        reason_str = ", ".join(reasons[:2]) if reasons else ""
        
        print(f"{prefix}{emoji} {pretty:12s} {signal:4s} ({confidence:.0f}%) @ {EMOJI_RUPEE}{close_price:.2f} {sl_info} {trend_emoji} [{reason_str}]")
        
        return result
        
    except Exception as e:
        error_msg = str(e)
        if "No data found, symbol may be delisted" in error_msg or "Quote not found" in error_msg:
             logging.warning(f"{prefix}{EMOJI_WARNING} {pretty}: Data fetching failed (Delisted/404)")
        else:
            logging.error(f"{prefix}{EMOJI_CROSS} {pretty}: Error: {e}")
        return None

def upload_batch(batch_data):
    try:
        valid_data = [d for d in batch_data if d and d.get('close_price', 0) > 0]
        if not valid_data:
            print(f"{EMOJI_WARNING} No valid data in batch")
            return 0
        
        resp = supabase.table('signals').insert(valid_data).execute()
        
        if isinstance(resp, dict) and resp.get('error'):
            logging.error(f"Batch upload error: {resp.get('error')}")
            return 0
        
        if hasattr(resp, 'data') and resp.data:
            return len(resp.data)
        
        return len(valid_data)
        
    except Exception as e:
        logging.error(f"Batch upload failed: {e}")
        return 0

def main():
    print(f"{EMOJI_ROCKET} Starting improved intraday signal generation...\n")
    
    try:
        today = datetime.now(IST_TZ).date().isoformat()
        print(f"{EMOJI_WARNING} Deleting today's old signals (date: {today})...")
        delete_resp = supabase.table('signals').delete().eq('signal_date', today).execute()
        
        if hasattr(delete_resp, 'data'):
            print(f"{EMOJI_CHECK} Old signals cleared (Checked against date: {today})\n")
        else:
            print(f"{EMOJI_CHECK} Old signals cleared\n")
    except Exception as e:
        print(f"{EMOJI_WARNING} Could not delete old signals: {e}\n")
    
    start_time = time.time()
    results = []
    failed_tickers = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_stock = {
            executor.submit(generate_intraday_signal, stock, i+1, len(STOCKS)): stock
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
        batch = results[i:i+BATCH_SIZE]
        count = upload_batch(batch)
        uploaded += count
        print(f" {EMOJI_CHECK} Batch {i//BATCH_SIZE + 1}: {count}/{len(batch)} uploaded")
    
    elapsed = time.time() - start_time
    success = len(results)
    failed = len(failed_tickers)
    
    print()
    print("=" * 70)
    print(f"{EMOJI_CHART} INTRADAY SUMMARY (PROFESSIONAL v2.5)")
    print("=" * 70)
    print(f"{EMOJI_CHECK} Successfully processed: {success} stocks")
    print(f"{EMOJI_CROSS} Failed: {failed} stocks")
    print(f"⚡ Total time: {elapsed:.1f}s ({elapsed/len(STOCKS) if len(STOCKS) > 0 else 0:.2f}s per stock)")
    print(f"⚡ Upload rate: {uploaded}/{success} ({100*uploaded/max(success,1):.1f}%)")
    
    if results:
        df = pd.DataFrame(results)
        
        buy_signals = df[df['signal'] == 'Buy']
        sell_signals = df[df['signal'] == 'Sell']
        hold_signals = df[df['signal'] == 'Hold']
        
        high_quality_buys = buy_signals[buy_signals['confidence'] > 70]
        high_quality_sells = sell_signals[sell_signals['confidence'] > 70]
        
        print()
        print(f"{EMOJI_GREEN} Buy signals: {len(buy_signals)} (High quality: {len(high_quality_buys)})")
        print(f"{EMOJI_RED} Sell signals: {len(sell_signals)} (High quality: {len(high_quality_sells)})")
        print(f"{EMOJI_WHITE} Hold signals: {len(hold_signals)}")
        print(f"{EMOJI_CHART} Average confidence: {df['confidence'].mean():.1f}%")
        
        if len(high_quality_buys) > 0:
            print(f"\n{EMOJI_FIRE} TOP 3 HIGH-QUALITY BUY SIGNALS (>70% confidence):")
            for _, row in high_quality_buys.nlargest(3, 'confidence').iterrows():
                rr = f"R:R {row['risk_reward']}" if row['risk_reward'] else ""
                print(f" {EMOJI_GREEN} {row['symbol']:12s} {row['confidence']:.0f}% {EMOJI_RUPEE}{row['close_price']:.2f} → T:{EMOJI_RUPEE}{row['target']} {rr}")
        elif len(buy_signals) > 0:
            print(f"\n{EMOJI_FIRE} TOP 3 BUY SIGNALS:")
            for _, row in buy_signals.nlargest(3, 'confidence').iterrows():
                rr = f"R:R {row['risk_reward']}" if row['risk_reward'] else ""
                print(f" {EMOJI_GREEN} {row['symbol']:12s} {row['confidence']:.0f}% {EMOJI_RUPEE}{row['close_price']:.2f} → T:{EMOJI_RUPEE}{row['target']} {rr}")
        
        if len(high_quality_sells) > 0:
            print(f"\n{EMOJI_FIRE} TOP 3 HIGH-QUALITY SELL SIGNALS (>70% confidence):")
            for _, row in high_quality_sells.nlargest(3, 'confidence').iterrows():
                rr = f"R:R {row['risk_reward']}" if row['risk_reward'] else ""
                print(f" {EMOJI_RED} {row['symbol']:12s} {row['confidence']:.0f}% {EMOJI_RUPEE}{row['close_price']:.2f} → T:{EMOJI_RUPEE}{row['target']} {rr}")
        elif len(sell_signals) > 0:
            print(f"\n{EMOJI_FIRE} TOP 3 SELL SIGNALS:")
            for _, row in sell_signals.nlargest(3, 'confidence').iterrows():
                rr = f"R:R {row['risk_reward']}" if row['risk_reward'] else ""
                print(f" {EMOJI_RED} {row['symbol']:12s} {row['confidence']:.0f}% {EMOJI_RUPEE}{row['close_price']:.2f} → T:{EMOJI_RUPEE}{row['target']} {rr}")
        
        valid_rr = df[df['risk_reward'].notna()]['risk_reward']
        if len(valid_rr) > 0:
            print(f"\n{EMOJI_TARGET} Average Risk:Reward Ratio: {valid_rr.mean():.2f}")
    
    if failed_tickers:
        print(f"\n{EMOJI_WARNING} Failed tickers: {', '.join([t.replace('.NS', '').replace('.BO', '') for t in failed_tickers[:10]])}")
        if len(failed_tickers) > 10:
            print(f"   ... and {len(failed_tickers) - 10} more")
    
    print()
    print("=" * 70)
    print(f"{EMOJI_CHECK} PROFESSIONAL INTRADAY SIGNAL GENERATION COMPLETE!")
    print(f"{EMOJI_CLOCK} Completed at: {datetime.now(IST_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"📝 Improvements: Liquidity filtering, ATR-based risk, Circuit detection, Wilder's RSI.")
    print("=" * 70)
    
    sys.exit(0 if success > 0 else 1)

if __name__ == "__main__":
    try:
        import pytz
    except ImportError:
        print(f"{EMOJI_CROSS} ERROR: The 'pytz' library is required for timezone handling. Please install it using 'pip install pytz'")
        sys.exit(1)
        
    main()
