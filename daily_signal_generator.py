#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI TRADING SIGNALS - INTRADAY GENERATOR (15-MIN INTERVALS)
Generates Buy/Sell/Hold signals for Indian stocks during market hours
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
INTERVAL = '15m'  # 15-minute candles
INTRADAY_PERIOD = '5d'  # Last 5 days of intraday data
MAX_WORKERS = 10
BATCH_SIZE = 20
RETRY_DELAY = 0.5
MAX_RETRIES = 3

# Market hours (IST)
MARKET_OPEN = (9, 15)   # 9:15 AM
MARKET_CLOSE = (15, 30)  # 3:30 PM

# Intraday indicator periods (shorter for faster signals)
RSI_PERIOD = 9       # Faster RSI
MACD_FAST = 8
MACD_SLOW = 17
MACD_SIGNAL = 9
BB_PERIOD = 15       # Bollinger Bands
VOL_PERIOD = 10      # Volume average

# Risk management
STOP_LOSS_PCT = 1.5   # 1.5% stop loss
TARGET_PCT = 2.5      # 2.5% target

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

if not SUPABASE_URL or not SUPABASE_KEY:
    print(f"{EMOJI_CROSS} ERROR: Missing SUPABASE credentials")
    sys.exit(1)

# ================================================================
# MARKET HOURS CHECK
# ================================================================
def is_market_hours():
    """Check if current time is within market hours (IST)"""
    now = datetime.now()
    current_time = (now.hour, now.minute)
    
    # Check if it's a weekday (Monday=0, Sunday=6)
    if now.weekday() > 4:
        return False, "Market closed (Weekend)"
    
    # Check market hours
    if current_time < MARKET_OPEN:
        return False, f"Market opens at {MARKET_OPEN[0]:02d}:{MARKET_OPEN[1]:02d}"
    elif current_time > MARKET_CLOSE:
        return False, f"Market closed at {MARKET_CLOSE[0]:02d}:{MARKET_CLOSE[1]:02d}"
    
    return True, "Market is open"

# ================================================================
# SANITIZE TICKERS
# ================================================================
TICKER_RE = re.compile(r'^[A-Z0-9][A-Z0-9._-]{0,18}(?:\.[A-Z]{1,5})?$')

def sanitize_tickers(raw: str):
    items = []
    for part in raw.split(','):
        t = str(part).strip()
        if not t:
            continue
        t = t.strip(" '\"`; :()[]{}<>")
        if not t:
            continue
        tokens = t.split()
        if not tokens:
            continue
        t = tokens[0].upper()
        t = re.sub(r'[^A-Z0-9._-]', '', t)
        if not t:
            continue
        if TICKER_RE.match(t):
            items.append(t)
    
    # Dedupe
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
print(f"{EMOJI_ROBOT} AI TRADING SIGNALS - INTRADAY GENERATOR ({INTERVAL})")
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
    """Extract value safely"""
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

def calculate_intraday_indicators(prices):
    """Calculate intraday technical indicators"""
    close = prices['Close']
    high = prices['High']
    low = prices['Low']
    
    # Fast RSI for intraday
    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(RSI_PERIOD).mean()
    loss = -delta.where(delta < 0, 0).rolling(RSI_PERIOD).mean()
    rs = gain / loss.replace(0, 0.0001)
    rsi = 100 - (100 / (1 + rs))
    
    # Fast MACD
    ema_fast = close.ewm(span=MACD_FAST, adjust=False).mean()
    ema_slow = close.ewm(span=MACD_SLOW, adjust=False).mean()
    macd = ema_fast - ema_slow
    macd_signal = macd.ewm(span=MACD_SIGNAL, adjust=False).mean()
    macd_histogram = macd - macd_signal
    
    # Bollinger Bands (shorter period)
    sma = close.rolling(BB_PERIOD).mean()
    std = close.rolling(BB_PERIOD).std()
    bb_upper = sma + (2 * std)
    bb_lower = sma - (2 * std)
    bb_middle = sma
    
    # Volume
    vol_avg = prices['Volume'].rolling(VOL_PERIOD).mean()
    
    # Moving averages for trend
    ema_20 = close.ewm(span=20, adjust=False).mean()
    ema_50 = close.ewm(span=50, adjust=False).mean()
    
    # ATR for volatility (intraday)
    tr1 = high - low
    tr2 = abs(high - close.shift())
    tr3 = abs(low - close.shift())
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(14).mean()
    
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
    """Fetch intraday data"""
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
            
            valid_closes = data['Close'].dropna()
            if len(valid_closes) < 30:
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

def generate_intraday_signal(stock_symbol, stock_num=0, total=0):
    """Generate intraday trading signal"""
    pretty = stock_symbol.replace('.NS', '')
    prefix = f"[{stock_num}/{total}] " if total > 0 else ""
    
    try:
        # Fetch intraday data
        data = fetch_intraday_data(stock_symbol)
        if data is None:
            print(f"{prefix}{EMOJI_CROSS} {pretty}: Failed to fetch data")
            return None
        
        # Verify columns
        required_cols = ['Close', 'Open', 'High', 'Low', 'Volume']
        missing = [c for c in required_cols if c not in data.columns]
        if missing:
            print(f"{prefix}{EMOJI_CROSS} {pretty}: Missing columns: {missing}")
            return None
        
        # Get current candle values
        last_idx = -1
        close_price = get_value(data['Close'], last_idx)
        open_price = get_value(data['Open'], last_idx)
        high_price = get_value(data['High'], last_idx)
        low_price = get_value(data['Low'], last_idx)
        volume = get_value(data['Volume'], last_idx)
        
        if close_price is None or close_price <= 0:
            print(f"{prefix}{EMOJI_CROSS} {pretty}: Invalid close price")
            return None
        
        # Calculate indicators
        indicators = calculate_intraday_indicators(data)
        
        # Extract indicator values
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
        
        # INTRADAY SIGNAL LOGIC (More aggressive)
        votes = []
        strength_score = 0
        
        # 1. RSI - Oversold/Overbought (intraday thresholds)
        if rsi_val < 35:
            votes.append('Buy')
            strength_score += 2
        elif rsi_val > 65:
            votes.append('Sell')
            strength_score += 2
        elif 45 <= rsi_val <= 55:
            votes.append('Hold')
        else:
            votes.append('Hold')
            strength_score += 1
        
        # 2. MACD - Momentum
        if macd_val > macd_sig_val and macd_hist > 0:
            votes.append('Buy')
            strength_score += 2
        elif macd_val < macd_sig_val and macd_hist < 0:
            votes.append('Sell')
            strength_score += 2
        else:
            votes.append('Hold')
        
        # 3. Bollinger Bands - Mean reversion
        bb_position = (close_price - bb_low_val) / (bb_up_val - bb_low_val) if bb_up_val != bb_low_val else 0.5
        if bb_position < 0.2:  # Near lower band
            votes.append('Buy')
            strength_score += 1
        elif bb_position > 0.8:  # Near upper band
            votes.append('Sell')
            strength_score += 1
        else:
            votes.append('Hold')
        
        # 4. EMA Crossover - Trend
        if ema_20 > ema_50:
            votes.append('Buy')
            strength_score += 1
        elif ema_20 < ema_50:
            votes.append('Sell')
            strength_score += 1
        else:
            votes.append('Hold')
        
        # 5. Volume confirmation
        if volume and vol_avg and volume > vol_avg * 1.5:
            votes.append(votes[-1])  # Amplify last signal
            strength_score += 2
        else:
            votes.append('Hold')
        
        # 6. Price action (candle pattern)
        candle_body = close_price - open_price
        candle_range = high_price - low_price if high_price and low_price else 0
        
        if candle_range > 0:
            body_ratio = abs(candle_body) / candle_range
            if candle_body > 0 and body_ratio > 0.6:  # Strong bullish candle
                votes.append('Buy')
                strength_score += 1
            elif candle_body < 0 and body_ratio > 0.6:  # Strong bearish candle
                votes.append('Sell')
                strength_score += 1
            else:
                votes.append('Hold')
        else:
            votes.append('Hold')
        
        # Count votes
        buy_count = votes.count('Buy')
        sell_count = votes.count('Sell')
        hold_count = votes.count('Hold')
        
        # Determine signal (need 4+ votes for action)
        if buy_count >= 4:
            signal = 'Buy'
            confidence = min((buy_count / 6) * 100 + (strength_score * 2), 100)
        elif sell_count >= 4:
            signal = 'Sell'
            confidence = min((sell_count / 6) * 100 + (strength_score * 2), 100)
        else:
            signal = 'Hold'
            confidence = (max(buy_count, sell_count, hold_count) / 6) * 100
        
        # Calculate stop loss and target
        if signal == 'Buy':
            stop_loss = round(close_price * (1 - STOP_LOSS_PCT/100), 2)
            target = round(close_price * (1 + TARGET_PCT/100), 2)
        elif signal == 'Sell':
            stop_loss = round(close_price * (1 + STOP_LOSS_PCT/100), 2)
            target = round(close_price * (1 - TARGET_PCT/100), 2)
        else:
            stop_loss = None
            target = None
        
        # Risk-reward ratio
        risk_reward = None
        if stop_loss and target:
            risk = abs(close_price - stop_loss)
            reward = abs(target - close_price)
            risk_reward = round(reward / risk, 2) if risk > 0 else None
        
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
            'signal_date': datetime.now().date().isoformat(),
            'signal_time': datetime.now().strftime('%H:%M:%S'),
            'interval': INTERVAL
        }
        
        emoji = EMOJI_GREEN if signal == 'Buy' else EMOJI_RED if signal == 'Sell' else EMOJI_WHITE
        sl_info = f"SL:{EMOJI_RUPEE}{stop_loss} T:{EMOJI_RUPEE}{target}" if stop_loss and target else ""
        print(f"{prefix}{emoji} {pretty}: {signal} ({confidence:.0f}%) @ {EMOJI_RUPEE}{close_price:.2f} {sl_info}")
        
        return result
        
    except Exception as e:
        logging.error(f"{prefix}{EMOJI_CROSS} {pretty}: Error: {e}")
        return None

def upload_batch(batch_data):
    """Upload signals in batch"""
    try:
        valid_data = [d for d in batch_data if d and d.get('close_price', 0) > 0]
        if not valid_data:
            print(f"{EMOJI_WARNING} No valid data in batch")
            return 0
        
        # Debug: Show first record
        print(f"{EMOJI_ROCKET} Uploading {len(valid_data)} records. Sample:")
        print(f"  {valid_data[0]}")
        
        resp = supabase.table('signals').upsert(
            valid_data,
            on_conflict='symbol,signal_date,signal_time'
        ).execute()
        
        # Debug: Show response
        print(f"{EMOJI_CHECK} Response type: {type(resp)}")
        
        if isinstance(resp, dict) and resp.get('error'):
            logging.error(f"Batch upload error: {resp.get('error')}")
            return 0
        
        # Check if response has data
        if hasattr(resp, 'data') and resp.data:
            print(f"{EMOJI_CHECK} Successfully inserted {len(resp.data)} records")
            return len(resp.data)
        
        return len(valid_data)
        
    except Exception as e:
        logging.error(f"Batch upload failed: {e}")
        import traceback
        traceback.print_exc()
        return 0

# ================================================================
# MAIN
# ================================================================
def main():
    print(f"{EMOJI_ROCKET} Starting intraday signal generation...\n")
    
    start_time = time.time()
    results = []
    failed_tickers = []
    
    # Process in parallel
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
    
    # Batch upload
    print(f"\n{EMOJI_ROCKET} Uploading {len(results)} signals...")
    uploaded = 0
    
    for i in range(0, len(results), BATCH_SIZE):
        batch = results[i:i+BATCH_SIZE]
        count = upload_batch(batch)
        uploaded += count
        print(f" {EMOJI_CHECK} Batch {i//BATCH_SIZE + 1}: {count}/{len(batch)} uploaded")
    
    # Summary
    elapsed = time.time() - start_time
    success = len(results)
    failed = len(failed_tickers)
    
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
