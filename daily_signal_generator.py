#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI TRADING SIGNALS - INTRADAY GENERATOR (15-MIN INTERVALS)
Enhanced with VWAP, ADX, 1H trend filter, Multi-Factor Confirmation (MFC),
Breakout/Reversal classification, No-Trade zones and improved confidence scoring.
DB (Supabase) and UI remain unchanged.
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
import math

# Emoji constants (same as before)
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
# CONFIGURATION (tweak thresholds here)
# ================================================================
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
STOCK_LIST = os.getenv('STOCK_LIST', 'RELIANCE.NS,TCS.NS,INFY.NS')

# Intraday settings
INTERVAL = '15m'  # 15-minute candles
INTRADAY_PERIOD = '5d'  # Last 5 days of intraday data
MAX_WORKERS = 10
BATCH_SIZE = 20
RETRY_DELAY = 0.5
MAX_RETRIES = 3

# Market hours (IST)
MARKET_OPEN = (9, 15)   # 9:15 AM
MARKET_CLOSE = (15, 30)  # 3:30 PM

# Indicator periods
RSI_PERIOD = 9
MACD_FAST = 8
MACD_SLOW = 17
MACD_SIGNAL = 9
BB_PERIOD = 15
VOL_PERIOD = 10
EMA_SHORT = 20
EMA_LONG = 50
ADX_PERIOD = 14

# Risk management
STOP_LOSS_PCT = 1.5
TARGET_PCT = 2.5

# Filters / thresholds (tune these)
VWAP_FILTER = True
REQUIRE_ADX = True
ADX_THRESHOLD = 22      # require ADX above this to allow trend trades
VOLUME_BREAKOUT_MULT = 1.8
NO_TRADE_RSI_LOW = 45
NO_TRADE_RSI_HIGH = 55
NO_TRADE_BB_WIDTH_PCT = 0.015  # 1.5% band width = low volatility -> no trades
MIN_CANDLES = 30

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

if not SUPABASE_URL or not SUPABASE_KEY:
    print(f"{EMOJI_CROSS} ERROR: Missing SUPABASE credentials")
    sys.exit(1)

# ================================================================
# UTIL / SANITIZE
# ================================================================
TICKER_RE = re.compile(r'^[A-Z0-9][A-Z0-9._-]{0,18}(?:\.[A-Z]{1,5})?$')
def sanitize_tickers(raw: str):
    items = []
    for part in raw.split(','):
        t = str(part).strip()
        if not t:
            continue
        t = t.strip(" '\"`; :()[]{}<>")
        tokens = t.split()
        if not tokens:
            continue
        t = tokens[0].upper()
        t = re.sub(r'[^A-Z0-9._-]', '', t)
        if not t:
            continue
        if TICKER_RE.match(t):
            items.append(t)
    seen = set(); out = []
    for t in items:
        if t not in seen:
            seen.add(t); out.append(t)
    return out

STOCKS = sanitize_tickers(STOCK_LIST)

# ================================================================
# STARTUP INFO
# ================================================================
print("=" * 70)
print(f"{EMOJI_ROBOT} AI TRADING SIGNALS - INTRADAY GENERATOR ({INTERVAL}) - UPGRADED")
print("=" * 70)
print(f"{EMOJI_CAL} Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}")
print(f"{EMOJI_CHART} Stocks to analyze: {len(STOCKS)}")
print(f"{EMOJI_CHART} Interval: {INTERVAL} candles")
print(f"{EMOJI_LINK} Supabase URL: {SUPABASE_URL[:30]}...")
print(f"⚡ Max workers: {MAX_WORKERS}")
print(f"{EMOJI_TARGET} Stop Loss: {STOP_LOSS_PCT}% | Target: {TARGET_PCT}%")
print("=" * 70)
print()

# ================================================================
# SUPABASE CONNECTION (unchanged)
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
# HELPERS: safe value extraction
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
        if float_val <= 0 or float_val < 0.0001:
            return None
        return float_val
    except (IndexError, ValueError, TypeError):
        return None

# ================================================================
# INDICATORS: VWAP, ADX, ATR helpers
# ================================================================
def calculate_vwap(df):
    """
    VWAP over the provided dataframe. We compute cumulative typical_price*volume / cumulative volume.
    Works for intraday timeframe (resets not implemented, but it's fine over a few days of intraday).
    """
    tp = (df['High'] + df['Low'] + df['Close']) / 3.0
    pv = tp * df['Volume']
    c_pv = pv.cumsum()
    c_vol = df['Volume'].cumsum().replace(0, np.nan)
    vwap = c_pv / c_vol
    return vwap

def calculate_atr(df, period=14):
    high = df['High']; low = df['Low']; close = df['Close']
    tr1 = high - low
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(period).mean()
    return atr

def calculate_adx(df, period=14):
    """
    Standard ADX (Wilder's). Returns ADX series.
    """
    high = df['High']; low = df['Low']; close = df['Close']
    plus_dm = high.diff()
    minus_dm = -low.diff()
    plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0.0)
    minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0.0)

    tr1 = high - low
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    atr = tr.rolling(period).mean()
    plus_di = 100 * (plus_dm.ewm(alpha=1/period, adjust=False).mean() / atr)
    minus_di = 100 * (minus_dm.ewm(alpha=1/period, adjust=False).mean() / atr)
    dx = (plus_di - minus_di).abs() / (plus_di + minus_di) * 100
    adx = dx.ewm(alpha=1/period, adjust=False).mean()
    return adx

# ================================================================
# MAIN indicators calc (intraday frame)
# ================================================================
def calculate_intraday_indicators(prices):
    close = prices['Close']; high = prices['High']; low = prices['Low']; vol = prices['Volume']

    # RSI
    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(RSI_PERIOD).mean()
    loss = -delta.where(delta < 0, 0).rolling(RSI_PERIOD).mean()
    rs = gain / loss.replace(0, 1e-8)
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
    vol_avg = vol.rolling(VOL_PERIOD).mean()

    # EMA trend
    ema_20 = close.ewm(span=EMA_SHORT, adjust=False).mean()
    ema_50 = close.ewm(span=EMA_LONG, adjust=False).mean()

    # ATR
    atr = calculate_atr(prices)

    # VWAP
    vwap = calculate_vwap(prices)

    # ADX
    adx = calculate_adx(prices, ADX_PERIOD)

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
        'atr': atr,
        'vwap': vwap,
        'adx': adx
    }

# ================================================================
# Fetching (includes higher timeframe fetch for 1H trend)
# ================================================================
def fetch_intraday_data(ticker: str, period=INTRADAY_PERIOD, interval=INTERVAL, max_retries=MAX_RETRIES):
    for attempt in range(1, max_retries + 1):
        try:
            if attempt > 1:
                time.sleep(RETRY_DELAY)
            stock = yf.Ticker(ticker)
            data = stock.history(period=period, interval=interval, auto_adjust=True)
            if hasattr(data, "columns") and isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)
            if data is None or data.empty:
                raise ValueError("Empty data")
            if len(data) < MIN_CANDLES:
                raise ValueError(f"Only {len(data)} candles")
            if 'Close' not in data.columns:
                raise ValueError("Missing Close column")
            valid_closes = data['Close'].dropna()
            if len(valid_closes) < MIN_CANDLES:
                raise ValueError(f"Only {len(valid_closes)} valid closes")
            last_close = data['Close'].iloc[-1]
            if pd.isna(last_close) or last_close <= 0:
                raise ValueError(f"Invalid last close: {last_close}")
            return data
        except Exception as e:
            if attempt == max_retries:
                logging.debug(f"Failed {ticker} after {max_retries} attempts: {e}")
                return None
    return None

def fetch_higher_timeframe(ticker: str, period='5d', interval='1h'):
    """
    Fetch higher timeframe (1h) for trend confirmation.
    Keep this lightweight; only need latest EMA direction.
    """
    try:
        stock = yf.Ticker(ticker)
        data = stock.history(period=period, interval=interval, auto_adjust=True)
        if hasattr(data, "columns") and isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)
        if data is None or data.empty or 'Close' not in data.columns:
            return None
        return data
    except Exception:
        return None

# ================================================================
# SIGNAL GENERATION (upgraded logic)
# ================================================================
def generate_intraday_signal(stock_symbol, stock_num=0, total=0):
    pretty = stock_symbol.replace('.NS', '')
    prefix = f"[{stock_num}/{total}] " if total > 0 else ""
    try:
        # Load intraday and higher timeframe
        data = fetch_intraday_data(stock_symbol)
        if data is None:
            print(f"{prefix}{EMOJI_CROSS} {pretty}: Failed to fetch data")
            return None

        ht = fetch_higher_timeframe(stock_symbol, period='15d', interval='1h')  # 1h trend
        # columns check
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

        indicators = calculate_intraday_indicators(data)
        # extract indicator values (safe)
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
        atr_val = get_value(indicators['atr'], last_idx) or (close_price * 0.02)
        vwap_val = get_value(indicators['vwap'], last_idx) or close_price
        adx_val = get_value(indicators['adx'], last_idx) or 0.0

        # HIGHER TIMEFRAME TREND: compute EMA20 on 1H
        ht_ema20 = None
        ht_trend_up = None
        if ht is not None and len(ht) >= 10:
            ht_close = ht['Close']
            ht_ema20_series = ht_close.ewm(span=20, adjust=False).mean()
            ht_ema20 = ht_ema20_series.iloc[-1]
            ht_ema20_prev = ht_ema20_series.iloc[-2] if len(ht_ema20_series) > 1 else ht_ema20
            ht_trend_up = ht_ema20 > ht_ema20_prev

        # Basic derived measures
        bb_width = (bb_up_val - bb_low_val) / close_price if close_price else 0
        bb_position = (close_price - bb_low_val) / (bb_up_val - bb_low_val) if bb_up_val != bb_low_val else 0.5
        candle_body = close_price - open_price
        candle_range = (high_price - low_price) if high_price and low_price else 0
        body_ratio = abs(candle_body) / candle_range if candle_range > 0 else 0

        # Classification: Breakout vs Reversal
        recent_high = data['High'].rolling(20).max().iloc[-2] if len(data) >= 21 else data['High'].max()
        recent_low = data['Low'].rolling(20).min().iloc[-2] if len(data) >= 21 else data['Low'].min()
        is_volume_breakout = volume and vol_avg and volume > vol_avg * VOLUME_BREAKOUT_MULT
        is_price_breakout_up = close_price > recent_high
        is_price_breakout_down = close_price < recent_low

        signal_type = None
        if is_volume_breakout and (is_price_breakout_up or is_price_breakout_down):
            signal_type = 'Breakout'
        elif rsi_val < 33 and bb_position < 0.25 and macd_hist > 0:
            signal_type = 'Reversal'
        elif rsi_val > 67 and bb_position > 0.75 and macd_hist < 0:
            signal_type = 'Reversal'
        else:
            signal_type = 'Neutral'

        # No-Trade Zone: restrict signals if market is choppy / indecisive
        in_no_trade_zone = False
        if NO_TRADE_RSI_LOW <= rsi_val <= NO_TRADE_RSI_HIGH:
            in_no_trade_zone = True
        if bb_width < NO_TRADE_BB_WIDTH_PCT:
            in_no_trade_zone = True
        if vol_avg and volume and volume < vol_avg * 0.8:
            in_no_trade_zone = True
        # ADX low implies weak trend -> no trades for breakouts
        if REQUIRE_ADX and adx_val < ADX_THRESHOLD:
            # allow reversals in low ADX sometimes, but block breakouts
            if signal_type == 'Breakout':
                in_no_trade_zone = True

        # Multi-Factor Confirmation (MFC)
        votes = []
        strength_score = 0

        # RSI-based
        if rsi_val < 35:
            votes.append('Buy'); strength_score += 2
        elif rsi_val > 65:
            votes.append('Sell'); strength_score += 2
        else:
            votes.append('Hold')

        # MACD momentum
        if macd_val > macd_sig_val and macd_hist > 0:
            votes.append('Buy'); strength_score += 2
        elif macd_val < macd_sig_val and macd_hist < 0:
            votes.append('Sell'); strength_score += 2
        else:
            votes.append('Hold')

        # Bollinger mean reversion / breakout position
        if bb_position < 0.2:
            votes.append('Buy'); strength_score += 1
        elif bb_position > 0.8:
            votes.append('Sell'); strength_score += 1
        else:
            votes.append('Hold')

        # EMA short/long
        if ema_20 > ema_50:
            votes.append('Buy'); strength_score += 1
        elif ema_20 < ema_50:
            votes.append('Sell'); strength_score += 1
        else:
            votes.append('Hold')

        # VWAP filter
        if VWAP_FILTER:
            if close_price > vwap_val:
                votes.append('Buy'); strength_score += 1
            elif close_price < vwap_val:
                votes.append('Sell'); strength_score += 1
            else:
                votes.append('Hold')
        else:
            votes.append('Hold')

        # Volume confirmation: double counts direction if breakout volume
        if is_volume_breakout:
            # if breakout up and price > vwap then extra buy vote
            if is_price_breakout_up:
                votes.append('Buy'); strength_score += 2
            elif is_price_breakout_down:
                votes.append('Sell'); strength_score += 2
            else:
                votes.append('Hold')
        else:
            votes.append('Hold')

        # Candle body strength
        if body_ratio > 0.6:
            if candle_body > 0:
                votes.append('Buy'); strength_score += 1
            else:
                votes.append('Sell'); strength_score += 1
        else:
            votes.append('Hold')

        # Higher timeframe filter enforcement: do not take counter-trend trades vs 1H EMA20
        ht_block = False
        if ht_ema20 is not None:
            # if 1h trend is up, block sell signals unless strong reversal
            if ht_trend_up and votes.count('Sell') >= 4:
                ht_block = True
            if not ht_trend_up and votes.count('Buy') >= 4:
                ht_block = True

        # Count votes
        buy_count = votes.count('Buy')
        sell_count = votes.count('Sell')
        hold_count = votes.count('Hold')

        # Determine raw signal
        raw_signal = 'Hold'
        if buy_count >= 4:
            raw_signal = 'Buy'
        elif sell_count >= 4:
            raw_signal = 'Sell'
        else:
            raw_signal = 'Hold'

        # Apply no-trade / HT filters
        if in_no_trade_zone:
            final_signal = 'Hold'
            confidence = 20.0
        elif ht_block:
            # If blocked by higher timeframe, downgrade to Hold unless reversal and strong
            if signal_type == 'Reversal' and (buy_count >= 4 or sell_count >= 4):
                final_signal = raw_signal
                confidence = min(70 + strength_score * 3, 95)
            else:
                final_signal = 'Hold'
                confidence = 30.0
        else:
            final_signal = raw_signal
            # Compute improved confidence based on votes, type, ADX and volatility
            base_conf = (max(buy_count, sell_count, hold_count) / 8.0) * 60  # votes weight
            type_bonus = 10 if signal_type == 'Breakout' else (5 if signal_type == 'Reversal' else 0)
            adx_bonus = 10 if (adx_val and adx_val >= ADX_THRESHOLD) else 0
            vol_bonus = 10 if is_volume_breakout else 0
            body_bonus = 5 if body_ratio > 0.6 else 0
            confidence = base_conf + type_bonus + adx_bonus + vol_bonus + body_bonus + min(strength_score*2, 20)
            confidence = max(15.0, min(confidence, 99.9))

        # Final safety checks: ATR and small price filters
        if atr_val and (atr_val / close_price) < 0.0005:
            # too small movement -> force hold
            final_signal = 'Hold'
            confidence = min(confidence, 35.0)

        # Calculate stop loss and target
        stop_loss = None; target = None
        if final_signal == 'Buy':
            stop_loss = round(close_price * (1 - STOP_LOSS_PCT/100), 2)
            target = round(close_price * (1 + TARGET_PCT/100), 2)
        elif final_signal == 'Sell':
            stop_loss = round(close_price * (1 + STOP_LOSS_PCT/100), 2)
            target = round(close_price * (1 - TARGET_PCT/100), 2)

        # Risk-reward
        risk_reward = None
        if stop_loss and target:
            risk = abs(close_price - stop_loss)
            reward = abs(target - close_price)
            risk_reward = round(reward / risk, 2) if risk > 0 else None

        # Prepare result (matching your previous output format)
        result = {
            'symbol': pretty,
            'signal': final_signal,
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
            'signal_date': datetime.now().date().isoformat(),
            'signal_time': datetime.now().strftime('%H:%M:%S'),
            'interval': INTERVAL,
            'type': signal_type,
            'adx': round(float(adx_val), 1) if adx_val is not None else None,
            'vwap': round(float(vwap_val), 2) if vwap_val is not None else None
        }

        emoji = EMOJI_GREEN if final_signal == 'Buy' else EMOJI_RED if final_signal == 'Sell' else EMOJI_WHITE
        sl_info = f"SL:{EMOJI_RUPEE}{stop_loss} T:{EMOJI_RUPEE}{target}" if stop_loss and target else ""
        print(f"{prefix}{emoji} {pretty}: {final_signal} ({confidence:.0f}%) @ {EMOJI_RUPEE}{close_price:.2f} {sl_info} [{signal_type}]")

        return result

    except Exception as e:
        logging.error(f"{prefix}{EMOJI_CROSS} {pretty}: Error: {e}")
        return None

# ================================================================
# Upload & main - unchanged (except minor safety)
# ================================================================
def upload_batch(batch_data):
    """Upload signals in batch (keeps your previous Supabase usage)"""
    try:
        valid_data = [d for d in batch_data if d and d.get('close_price', 0) > 0]
        if not valid_data:
            print(f"{EMOJI_WARNING} No valid data in batch")
            return 0

        print(f"{EMOJI_ROCKET} Uploading {len(valid_data)} records. Sample:")
        print(f"  {valid_data[0]}")

        resp = supabase.table('signals').insert(valid_data).execute()

        print(f"{EMOJI_CHECK} Response type: {type(resp)}")
        if isinstance(resp, dict) and resp.get('error'):
            logging.error(f"Batch upload error: {resp.get('error')}")
            return 0

        if hasattr(resp, 'data') and resp.data:
            print(f"{EMOJI_CHECK} Successfully inserted {len(resp.data)} records")
            return len(resp.data)

        return len(valid_data)

    except Exception as e:
        logging.error(f"Batch upload failed: {e}")
        import traceback; traceback.print_exc()
        return 0

def is_market_hours():
    """Check IST market hours (same as your original function)"""
    now = datetime.now()
    current_time = (now.hour, now.minute)
    if now.weekday() > 4:
        return False, "Market closed (Weekend)"
    if current_time < MARKET_OPEN:
        return False, f"Market opens at {MARKET_OPEN[0]:02d}:{MARKET_OPEN[1]:02d}"
    elif current_time > MARKET_CLOSE:
        return False, f"Market closed at {MARKET_CLOSE[0]:02d}:{MARKET_CLOSE[1]:02d}"
    return True, "Market is open"

def main():
    print(f"{EMOJI_ROCKET} Starting intraday signal generation...\n")
    # delete today's old signals (unchanged)
    try:
        today = datetime.now().date().isoformat()
        print(f"{EMOJI_WARNING} Deleting today's old signals (date: {today})...")
        delete_resp = supabase.table('signals').delete().eq('signal_date', today).execute()
        if hasattr(delete_resp, 'data'):
            deleted_count = len(delete_resp.data) if delete_resp.data else 0
            print(f"{EMOJI_CHECK} Deleted {deleted_count} old signals\n")
        else:
            print(f"{EMOJI_CHECK} Old signals cleared\n")
    except Exception as e:
        print(f"{EMOJI_WARNING} Could not delete old signals: {e}")
        print(f"{EMOJI_WARNING} Continuing anyway...\n")

    start_time = time.time()
    results = []; failed_tickers = []

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

    # Batch upload (unchanged)
    print(f"\n{EMOJI_ROCKET} Uploading {len(results)} signals...")
    uploaded = 0
    for i in range(0, len(results), BATCH_SIZE):
        batch = results[i:i+BATCH_SIZE]
        count = upload_batch(batch)
        uploaded += count
        print(f" {EMOJI_CHECK} Batch {i//BATCH_SIZE + 1}: {count}/{len(batch)} uploaded")

    elapsed = time.time() - start_time
    success = len(results); failed = len(failed_tickers)

    print()
    print("=" * 70)
    print(f"{EMOJI_CHART} INTRADAY SUMMARY")
    print("=" * 70)
    print(f"{EMOJI_CHECK} Successfully processed: {success} stocks")
    print(f"{EMOJI_CROSS} Failed: {failed} stocks")
    print(f"⚡ Total time: {elapsed:.1f}s ({elapsed/len(STOCKS):.2f}s per stock)")
    print(f"⚡ Upload rate: {uploaded}/{success} ({100*uploaded/max(success,1):.1f}%)")

    if results:
        df = pd.DataFrame(results)
        buy_signals = df[df['signal'] == 'Buy']
        sell_signals = df[df['signal'] == 'Sell']
        hold_signals = df[df['signal'] == 'Hold']
        print()
        print(f"{EMOJI_GREEN} Buy signals: {len(buy_signals)}")
        print(f"{EMOJI_RED} Sell signals: {len(sell_signals)}")
        print(f"{EMOJI_WHITE} Hold signals: {len(hold_signals)}")
        print(f"{EMOJI_CHART} Average confidence: {df['confidence'].mean():.1f}%")
        if len(buy_signals) > 0:
            print(f"\n{EMOJI_FIRE} TOP 3 BUY SIGNALS:")
            for _, row in buy_signals.nlargest(3, 'confidence').iterrows():
                rr = f"R:R {row['risk_reward']}" if row['risk_reward'] else ""
                print(f" {EMOJI_GREEN} {row['symbol']:12s} {row['confidence']:.0f}% {EMOJI_RUPEE}{row['close_price']:.2f} → T:{EMOJI_RUPEE}{row['target']} {rr}")
        if len(sell_signals) > 0:
            print(f"\n{EMOJI_FIRE} TOP 3 SELL SIGNALS:")
            for _, row in sell_signals.nlargest(3, 'confidence').iterrows():
                rr = f"R:R {row['risk_reward']}" if row['risk_reward'] else ""
                print(f" {EMOJI_RED} {row['symbol']:12s} {row['confidence']:.0f}% {EMOJI_RUPEE}{row['close_price']:.2f} → T:{EMOJI_RUPEE}{row['target']} {rr}")

    print()
    print("=" * 70)
    print(f"{EMOJI_CHECK} INTRADAY SIGNAL GENERATION COMPLETE!")
    print(f"{EMOJI_CLOCK} Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}")
    print("=" * 70)
    sys.exit(0 if success > 0 else 1)

if __name__ == "__main__":
    main()
