#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI TRADING SIGNALS - DAILY GENERATOR (PERFORMANCE OPTIMIZED)
Automatically generates Buy/Sell/Hold signals for Indian stocks
"""

import os
import sys
import time
import warnings
import logging
import re
from random import uniform
from concurrent.futures import ThreadPoolExecutor, as_completed
warnings.filterwarnings('ignore')

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime

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

# ================================================================
# CONFIGURATION
# ================================================================

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
STOCK_LIST = os.getenv('STOCK_LIST', 'RELIANCE.NS,TCS.NS,INFY.NS')

# Performance settings
MAX_WORKERS = 10 # Parallel downloads
BATCH_SIZE = 20 # Upload batch size
RETRY_DELAY = 0.5 # Reduced from exponential backoff
MAX_RETRIES = 3 # Increased to 3 for better success rate

# Basic logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

if not SUPABASE_URL or not SUPABASE_KEY:
    print(f"{EMOJI_CROSS} ERROR: Missing SUPABASE credentials")
    sys.exit(1)

# ================================================================
# SANITIZE / PARSE TICKER LIST
# ================================================================
TICKER_RE = re.compile(r'^[A-Z0-9][A-Z0-9._-]{0,18}(?:\.[A-Z]{1,5})?$')

def sanitize_tickers(raw: str):
    items = []
    for part in raw.split(','):
        t = str(part).strip()
        if not t:
            continue
        t = t.strip(" '\"`;:()[]{}<>")
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
    # Dedupe preserving order
    seen = set()
    out = []
    for t in items:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out

STOCKS = sanitize_tickers(STOCK_LIST)

print("=" * 70)
print(f"{EMOJI_ROBOT} AI TRADING SIGNALS - DAILY GENERATOR (OPTIMIZED)")
print("=" * 70)
print(f"{EMOJI_CAL} Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}")
print(f"{EMOJI_CHART} Stocks to analyze: {len(STOCKS)}")
print(f"{EMOJI_LINK} Supabase URL: {SUPABASE_URL[:30]}...")
print(f"⚡ Max parallel workers: {MAX_WORKERS}")
print(f"⚡ Batch upload size: {BATCH_SIZE}")
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
# OPTIMIZED HELPER FUNCTIONS
# ================================================================

def get_value(series, idx):
    """Fast value extraction with better validation"""
    try:
        if series is None or len(series) == 0:
            return None
        
        val = series.iloc[idx]
        
        # Handle Series within Series
        if isinstance(val, pd.Series):
            val = val.iloc[0] if len(val) > 0 else None
        
        # Convert to float and validate
        if pd.isna(val):
            return None
        
        float_val = float(val)
        
        # Additional validation: reject extremely small or negative prices
        if float_val <= 0 or float_val < 0.01:
            return None
            
        return float_val
    except (IndexError, ValueError, TypeError):
        return None

def calculate_indicators(prices):
    """Calculate all indicators at once to avoid multiple passes"""
    close = prices['Close']
    
    # RSI calculation
    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = -delta.where(delta < 0, 0).rolling(14).mean()
    rs = gain / loss.replace(0, 0.0001)
    rsi = 100 - (100 / (1 + rs))
    
    # MACD calculation
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    macd_signal = macd.ewm(span=9, adjust=False).mean()
    
    # Bollinger Bands
    sma20 = close.rolling(20).mean()
    std20 = close.rolling(20).std()
    bb_upper = sma20 + (2 * std20)
    bb_lower = sma20 - (2 * std20)
    
    # Volume average
    vol_avg = prices['Volume'].rolling(20).mean()
    
    return {
        'rsi': rsi,
        'macd': macd,
        'macd_signal': macd_signal,
        'bb_upper': bb_upper,
        'bb_lower': bb_lower,
        'vol_avg': vol_avg
    }

def fetch_history_fast(ticker: str, max_retries=MAX_RETRIES):
    """Optimized fetch with better error handling"""
    for attempt in range(1, max_retries + 1):
        try:
            if attempt > 1:
                time.sleep(RETRY_DELAY)
            
            # Create a new Ticker object each time to avoid stale sessions
            stock = yf.Ticker(ticker)
            data = stock.history(period='3mo', auto_adjust=True)
            
            # Handle MultiIndex columns
            if hasattr(data, "columns") and isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)
            
            # Validate data
            if data is None or data.empty:
                raise ValueError("Empty data")
            
            if len(data) < 30:
                raise ValueError(f"Only {len(data)} rows")
            
            if 'Close' not in data.columns:
                raise ValueError("Missing Close column")
            
            # Ensure we have valid close prices
            valid_closes = data['Close'].dropna()
            if len(valid_closes) < 30:
                raise ValueError(f"Only {len(valid_closes)} valid closes")
            
            # Check if last close price is valid
            last_close = data['Close'].iloc[-1]
            if pd.isna(last_close) or last_close <= 0:
                raise ValueError(f"Invalid last close: {last_close}")
            
            return data
            
        except Exception as e:
            if attempt == max_retries:
                logging.debug(f"Failed {ticker} after {max_retries} attempts: {e}")
                return None
    return None

def generate_signal(stock_symbol, stock_num=0, total=0):
    """Optimized signal generation with robust error handling"""
    pretty = stock_symbol.replace('.NS', '')
    prefix = f"[{stock_num}/{total}] " if total > 0 else ""
    
    try:
        # Fetch data with retries
        data = fetch_history_fast(stock_symbol)
        if data is None:
            print(f"{prefix}{EMOJI_CROSS} {pretty}: Failed to fetch data")
            return None
        
        # Verify required columns exist
        required_cols = ['Close', 'Open', 'Volume']
        missing = [c for c in required_cols if c not in data.columns]
        if missing:
            print(f"{prefix}{EMOJI_CROSS} {pretty}: Missing columns: {missing}")
            return None
        
        # Get last values with validation
        last_idx = -1
        close_price = get_value(data['Close'], last_idx)
        open_price = get_value(data['Open'], last_idx)
        volume = get_value(data['Volume'], last_idx)
        
        # Strict validation for close price
        if close_price is None:
            print(f"{prefix}{EMOJI_CROSS} {pretty}: Close price is None")
            return None
        
        if close_price <= 0:
            print(f"{prefix}{EMOJI_CROSS} {pretty}: Invalid close price {close_price}")
            return None
        
        # If open price is missing, try previous day
        if open_price is None or open_price <= 0:
            open_price = get_value(data['Open'], -2)
        
        # Calculate all indicators at once
        indicators = calculate_indicators(data)
        
        # Extract indicator values with fallbacks
        rsi_val = get_value(indicators['rsi'], last_idx)
        if rsi_val is None:
            rsi_val = 50.0 # Neutral RSI
        
        macd_val = get_value(indicators['macd'], last_idx)
        if macd_val is None:
            macd_val = 0.0
        
        macd_sig_val = get_value(indicators['macd_signal'], last_idx)
        if macd_sig_val is None:
            macd_sig_val = 0.0
        
        bb_up_val = get_value(indicators['bb_upper'], last_idx)
        if bb_up_val is None:
            bb_up_val = close_price * 1.02 # 2% above
        
        bb_low_val = get_value(indicators['bb_lower'], last_idx)
        if bb_low_val is None:
            bb_low_val = close_price * 0.98 # 2% below
        
        vol_avg = get_value(indicators['vol_avg'], last_idx)
        
        # Fast voting logic
        votes = []
        
        # RSI vote
        if rsi_val < 30:
            votes.append('Buy')
        elif rsi_val > 70:
            votes.append('Sell')
        else:
            votes.append('Hold')
        
        # MACD vote
        votes.append('Buy' if macd_val > macd_sig_val else 'Sell')
        
        # Bollinger vote
        if close_price < bb_low_val:
            votes.append('Buy')
        elif close_price > bb_up_val:
            votes.append('Sell')
        else:
            votes.append('Hold')
        
        # Volume vote (only if we have volume data)
        if volume and vol_avg and volume > vol_avg:
            votes.append(votes[-1]) # Amplify last signal
        else:
            votes.append('Hold')
        
        # Price action vote (only if we have open price)
        if open_price and open_price > 0:
            votes.append('Buy' if close_price > open_price else 'Sell')
        else:
            votes.append('Hold')
        
        # Count votes
        buy_count = votes.count('Buy')
        sell_count = votes.count('Sell')
        hold_count = votes.count('Hold')
        
        # Determine signal
        if buy_count >= 3:
            signal = 'Buy'
            confidence = (buy_count / 5) * 100
        elif sell_count >= 3:
            signal = 'Sell'
            confidence = (sell_count / 5) * 100
        else:
            signal = 'Hold'
            confidence = (max(buy_count, sell_count, hold_count) / 5) * 100
        
        result = {
            'symbol': pretty,
            'signal': signal,
            'confidence': round(float(confidence), 1),
            'close_price': round(float(close_price), 2),
            'rsi': round(float(rsi_val), 1),
            'buy_votes': int(buy_count),
            'sell_votes': int(sell_count),
            'hold_votes': int(hold_count),
            'signal_date': datetime.now().date().isoformat()
        }
        
        print(f"{prefix}{EMOJI_CHECK} {pretty}: {signal} ({confidence:.0f}%) @ {EMOJI_RUPEE}{close_price:.2f}")
        return result
        
    except Exception as e:
        logging.error(f"{prefix}{EMOJI_CROSS} {pretty}: Unexpected error: {e}")
        return None

def upload_batch(batch_data):
    """Upload multiple signals at once"""
    try:
        valid_data = [d for d in batch_data if d and d.get('close_price', 0) > 0]
        if not valid_data:
            return 0
        
        resp = supabase.table('signals').upsert(
            valid_data,
            on_conflict='symbol,signal_date'
        ).execute()
        
        if isinstance(resp, dict) and resp.get('error'):
            logging.error(f"Batch upload error: {resp.get('error')}")
            return 0
        
        return len(valid_data)
        
    except Exception as e:
        logging.error(f"Batch upload failed: {e}")
        return 0

def main():
    print(f"{EMOJI_ROCKET} Starting parallel signal generation...\n")
    
    start_time = time.time()
    results = []
    failed_tickers = []
    
    # Process stocks in parallel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_stock = {
            executor.submit(generate_signal, stock, i+1, len(STOCKS)): stock 
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
    
    # Batch upload results
    print(f"\n{EMOJI_ROCKET} Uploading {len(results)} signals in batches...")
    uploaded = 0
    
    for i in range(0, len(results), BATCH_SIZE):
        batch = results[i:i+BATCH_SIZE]
        count = upload_batch(batch)
        uploaded += count
        print(f" {EMOJI_CHECK} Batch {i//BATCH_SIZE + 1}: {count}/{len(batch)} uploaded")
    
    # Save failed tickers with categories
    if failed_tickers:
        try:
            with open('failed_tickers.txt', 'w') as fh:
                fh.write("# Failed Tickers Report\n")
                fh.write(f"# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                # Known delisted stocks
                delisted = ['MM.NS', 'ABBANK.NS', 'MINDTREE.NS', 'LTI.NS', 
                           'CADILAHC.NS', 'ZOMATO.NS', 'ADANIGAS.NS', 
                           'ADANITRANS.NS', 'BERGER.NS', 'PHOENIX.NS']
                
                data_issues = []
                truly_delisted = []
                
                for t in failed_tickers:
                    if t in delisted:
                        truly_delisted.append(t)
                    else:
                        data_issues.append(t)
                
                if truly_delisted:
                    fh.write("## Delisted/Unavailable Stocks (can be removed from list):\n")
                    for t in truly_delisted:
                        fh.write(f"{t}\n")
                    fh.write("\n")
                
                if data_issues:
                    fh.write("## Temporary Data Issues (may work on retry):\n")
                    for t in data_issues:
                        fh.write(f"{t}\n")
                
            logging.info(f"Wrote {len(failed_tickers)} failed tickers ({len(truly_delisted)} delisted, {len(data_issues)} data issues)")
        except Exception as e:
            logging.error(f"Failed to write file: {e}")
    
    # Summary
    elapsed = time.time() - start_time
    success = len(results)
    failed = len(failed_tickers)
    
    # Categorize failures
    delisted = ['MM.NS', 'ABBANK.NS', 'MINDTREE.NS', 'LTI.NS', 
               'CADILAHC.NS', 'ZOMATO.NS', 'ADANIGAS.NS', 
               'ADANITRANS.NS', 'BERGER.NS', 'PHOENIX.NS']
    
    truly_delisted = [t for t in failed_tickers if t in delisted]
    data_issues = [t for t in failed_tickers if t not in delisted]
    
    print()
    print("=" * 70)
    print(f"{EMOJI_CHART} SUMMARY")
    print("=" * 70)
    print(f"{EMOJI_CHECK} Successfully processed: {success} stocks")
    print(f"{EMOJI_CROSS} Failed: {failed} stocks")
    if truly_delisted:
        print(f" └─ Delisted/Unavailable: {len(truly_delisted)} stocks")
    if data_issues:
        print(f" └─ Temporary data issues: {len(data_issues)} stocks")
    print(f"⚡ Total time: {elapsed:.1f}s ({elapsed/len(STOCKS):.2f}s per stock)")
    print(f"⚡ Upload success rate: {uploaded}/{success} ({100*uploaded/max(success,1):.1f}%)")
    
    if results:
        df = pd.DataFrame(results)
        
        # Check for zero prices
        zero_prices = df[df['close_price'] == 0]
        if len(zero_prices) > 0:
            print(f"\n{EMOJI_WARNING} WARNING: {len(zero_prices)} stocks have zero price!")
        
        print()
        print(f"{EMOJI_GREEN} Buy signals: {len(df[df['signal'] == 'Buy'])}")
        print(f"{EMOJI_RED} Sell signals: {len(df[df['signal'] == 'Sell'])}")
        print(f"{EMOJI_WHITE} Hold signals: {len(df[df['signal'] == 'Hold'])}")
        print(f"{EMOJI_CHART} Average confidence: {df['confidence'].mean():.1f}%")
        print(f"{EMOJI_MONEY} Average price: {EMOJI_RUPEE}{df['close_price'].mean():.2f}")
        
        print(f"\n{EMOJI_MONEY} TOP 3 SIGNALS:")
        for _, row in df.nlargest(3, 'confidence').iterrows():
            emoji = EMOJI_GREEN if row['signal'] == 'Buy' else EMOJI_RED if row['signal'] == 'Sell' else EMOJI_WHITE
            print(f" {emoji} {row['symbol']:12s} {row['signal']:5s} {row['confidence']:.0f}% {EMOJI_RUPEE}{row['close_price']:.2f}")
    
    print()
    print("=" * 70)
    print(f"{EMOJI_CHECK} DAILY SIGNAL GENERATION COMPLETE!")
    print(f"{EMOJI_CLOCK} Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}")
    print("=" * 70)
    
    sys.exit(0 if success > 0 else 1)

if __name__ == "__main__":
    main()
