#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI TRADING SIGNALS - DAILY GENERATOR (PRICE FIX)
Automatically generates Buy/Sell/Hold signals for Indian stocks
"""

import os
import sys
import time
import warnings
warnings.filterwarnings('ignore')

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime

# ================================================================
# CONFIGURATION
# ================================================================

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
STOCK_LIST = os.getenv('STOCK_LIST', 'RELIANCE.NS,TCS.NS,INFY.NS')

if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ ERROR: Missing SUPABASE credentials")
    sys.exit(1)

STOCKS = [s.strip() for s in STOCK_LIST.split(',') if s.strip()]

print("=" * 70)
print("🤖 AI TRADING SIGNALS - DAILY GENERATOR")
print("=" * 70)
print(f"📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}")
print(f"📊 Stocks to analyze: {len(STOCKS)}")
print(f"🔗 Supabase URL: {SUPABASE_URL[:30]}...")
print("=" * 70)
print()

# ================================================================
# SUPABASE CONNECTION
# ================================================================

try:
    from supabase import create_client
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("✅ Connected to Supabase successfully")
    print()
except Exception as e:
    print(f"❌ Failed to connect: {e}")
    sys.exit(1)

# ================================================================
# HELPER FUNCTIONS
# ================================================================

def get_value(series, idx):
    """Safely extract scalar value from pandas Series"""
    try:
        val = series.iloc[idx]
        # Handle MultiIndex columns from yfinance
        if isinstance(val, pd.Series):
            val = val.iloc[0]
        # Convert to Python native type
        if pd.isna(val):
            return None
        return float(val)
    except Exception as e:
        print(f" ⚠️ Error extracting value: {e}")
        return None

# ================================================================
# TECHNICAL INDICATORS
# ================================================================

def calculate_rsi(prices, period=14):
    """RSI Indicator"""
    delta = prices.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss.replace(0, 0.0001)
    return 100 - (100 / (1 + rs))

def calculate_macd(prices):
    """MACD Indicator"""
    ema12 = prices.ewm(span=12, adjust=False).mean()
    ema26 = prices.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    return macd, signal

def calculate_bollinger(prices, period=20):
    """Bollinger Bands"""
    sma = prices.rolling(window=period).mean()
    std = prices.rolling(window=period).std()
    upper = sma + (2 * std)
    lower = sma - (2 * std)
    return upper, sma, lower

# ================================================================
# SIGNAL GENERATION
# ================================================================

def generate_signal(stock_symbol):
    """Generate signal for one stock"""
    
    print(f"📈 Processing: {stock_symbol.replace('.NS', '')}...", end=" ")
    
    try:
        # Download data - force single ticker to avoid MultiIndex
        data = yf.download(
            stock_symbol, 
            period='3mo', 
            interval='1d', 
            progress=False,
            auto_adjust=True, # Auto-adjust for splits/dividends
            actions=False # Don't include corporate actions
        )
        
        if data.empty or len(data) < 30:
            print(f"❌ Not enough data ({len(data)} days)")
            return None
        
        print(f"✅ Got {len(data)} days", end=" → ")
        
        # ============================================================
        # CRITICAL FIX: Access columns correctly
        # yfinance sometimes returns MultiIndex columns
        # ============================================================
        
        # Flatten columns if MultiIndex
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)
        
        # Extract price columns
        close_col = data['Close']
        open_col = data['Open']
        volume_col = data['Volume']
        
        # Get LAST valid index
        last_idx = -1
        
        # Extract prices - with validation
        close_price = get_value(close_col, last_idx)
        open_price = get_value(open_col, last_idx)
        volume = get_value(volume_col, last_idx)
        
        # Validate we got prices
        if close_price is None or close_price <= 0:
            print(f"❌ Invalid price data (close={close_price})")
            return None
        
        print(f"Price: ₹{close_price:.2f}", end=" → ")
        
        # Calculate indicators
        rsi = calculate_rsi(close_col)
        macd, macd_sig = calculate_macd(close_col)
        bb_up, bb_mid, bb_low = calculate_bollinger(close_col)
        
        # Extract indicator values
        rsi_val = get_value(rsi, last_idx) or 50.0
        macd_val = get_value(macd, last_idx) or 0.0
        macd_sig_val = get_value(macd_sig, last_idx) or 0.0
        bb_up_val = get_value(bb_up, last_idx) or close_price
        bb_low_val = get_value(bb_low, last_idx) or close_price
        
        # Volume analysis
        avg_vol = volume_col.rolling(20).mean().iloc[last_idx]
        high_vol = volume > avg_vol if volume and avg_vol else False
        
        # ============================================================
        # VOTING SYSTEM
        # ============================================================
        
        votes = []
        
        # Vote 1: RSI
        if rsi_val < 30:
            votes.append('Buy')
        elif rsi_val > 70:
            votes.append('Sell')
        else:
            votes.append('Hold')
        
        # Vote 2: MACD
        if macd_val > macd_sig_val:
            votes.append('Buy')
        else:
            votes.append('Sell')
        
        # Vote 3: Bollinger Bands
        if close_price < bb_low_val:
            votes.append('Buy')
        elif close_price > bb_up_val:
            votes.append('Sell')
        else:
            votes.append('Hold')
        
        # Vote 4: Volume
        if high_vol and len(votes) > 0:
            votes.append(votes[-1])
        else:
            votes.append('Hold')
        
        # Vote 5: Candlestick
        if close_price > open_price:
            votes.append('Buy')
        else:
            votes.append('Sell')
        
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
        
        print(f"{signal} ({confidence:.0f}%)")
        
        # ============================================================
        # RETURN DATA WITH VALIDATED PRICES
        # ============================================================
        
        result = {
            'symbol': stock_symbol.replace('.NS', ''),
            'signal': signal,
            'confidence': round(float(confidence), 1),
            'close_price': round(float(close_price), 2),
            'rsi': round(float(rsi_val), 1),
            'buy_votes': int(buy_count),
            'sell_votes': int(sell_count),
            'hold_votes': int(hold_count),
            'signal_date': datetime.now().date().isoformat()
        }
        
        # Debug: Print what we're returning
        print(f" 💰 Storing price: ₹{result['close_price']}")
        
        return result
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

# ================================================================
# UPLOAD TO SUPABASE
# ================================================================

def upload_signal(data):
    """Upload signal to database"""
    try:
        # Validate data before upload
        if data['close_price'] <= 0:
            print(f" ⚠️ Invalid price {data['close_price']}, skipping upload")
            return False
        
        response = supabase.table('signals').upsert(
            data, 
            on_conflict='symbol,signal_date'
        ).execute()
        
        print(f" ✅ Uploaded to Supabase (price: ₹{data['close_price']})")
        return True
        
    except Exception as e:
        print(f" ⚠️ Upload failed: {e}")
        return False

# ================================================================
# MAIN
# ================================================================

def main():
    print("🚀 Starting signal generation...\n")
    
    success = 0
    failed = 0
    results = []
    
    for i, stock in enumerate(STOCKS, 1):
        print(f"[{i}/{len(STOCKS)}] ", end="")
        
        signal = generate_signal(stock)
        
        if signal:
            results.append(signal)
            if upload_signal(signal):
                success += 1
            else:
                failed += 1
        else:
            failed += 1
        
        time.sleep(1) # Rate limiting
    
    # Summary
    print()
    print("=" * 70)
    print("📊 SUMMARY")
    print("=" * 70)
    print(f"✅ Successfully processed: {success} stocks")
    print(f"❌ Failed: {failed} stocks")
    
    if results:
        df = pd.DataFrame(results)
        
        # Validate prices
        zero_prices = df[df['close_price'] == 0]
        if len(zero_prices) > 0:
            print(f"\n⚠️ WARNING: {len(zero_prices)} stocks have zero price!")
            for _, row in zero_prices.iterrows():
                print(f" - {row['symbol']}: price = {row['close_price']}")
        
        print()
        print(f"🟢 Buy signals: {len(df[df['signal'] == 'Buy'])}")
        print(f"🔴 Sell signals: {len(df[df['signal'] == 'Sell'])}")
        print(f"⚪ Hold signals: {len(df[df['signal'] == 'Hold'])}")
        print(f"📈 Average confidence: {df['confidence'].mean():.1f}%")
        print(f"💰 Average price: ₹{df['close_price'].mean():.2f}")
        
        # Top 3
        print("\n🏆 TOP 3 SIGNALS:")
        for _, row in df.nlargest(3, 'confidence').iterrows():
            emoji = '🟢' if row['signal'] == 'Buy' else '🔴' if row['signal'] == 'Sell' else '⚪'
            print(f" {emoji} {row['symbol']:12s} {row['signal']:5s} {row['confidence']:.0f}% ₹{row['close_price']:.2f}")
    
    print()
    print("=" * 70)
    print("✅ DAILY SIGNAL GENERATION COMPLETE!")
    print(f"🕐 Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}")
    print("=" * 70)
    
    sys.exit(0 if failed == 0 else 1)

if __name__ == "__main__":
    main()
