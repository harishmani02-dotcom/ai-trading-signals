#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI TRADING SIGNALS - DAILY GENERATOR (FIXED VERSION)
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

def safe_float(value, default=0.0):
    """Safely convert pandas value to float"""
    try:
        if pd.isna(value):
            return default
        return float(value)
    except:
        return default

def safe_bool(value, default=False):
    """Safely convert pandas value to bool"""
    try:
        if pd.isna(value):
            return default
        return bool(value)
    except:
        return default

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
        # Download data
        df = yf.download(stock_symbol, period='3mo', interval='1d', 
                        progress=False, threads=False)
        
        if df.empty or len(df) < 30:
            print(f"❌ Not enough data ({len(df)} days)")
            return None
        
        print(f"✅ Got {len(df)} days", end=" → ")
        
        # Calculate indicators
        rsi = calculate_rsi(df['Close'])
        macd, macd_sig = calculate_macd(df['Close'])
        bb_up, bb_mid, bb_low = calculate_bollinger(df['Close'])
        
        # Get LAST row values - THIS IS THE CRITICAL PART
        last_idx = len(df) - 1
        
        # Extract scalar values using .iloc and convert to Python types
        price = safe_float(df['Close'].iloc[last_idx])
        open_price = safe_float(df['Open'].iloc[last_idx])
        close_price = safe_float(df['Close'].iloc[last_idx])
        
        rsi_val = safe_float(rsi.iloc[last_idx], 50.0)
        macd_val = safe_float(macd.iloc[last_idx], 0.0)
        macd_sig_val = safe_float(macd_sig.iloc[last_idx], 0.0)
        bb_up_val = safe_float(bb_up.iloc[last_idx], price)
        bb_low_val = safe_float(bb_low.iloc[last_idx], price)
        
        # Volume
        vol = safe_float(df['Volume'].iloc[last_idx], 0)
        avg_vol = safe_float(df['Volume'].rolling(20).mean().iloc[last_idx], 0)
        high_vol = vol > avg_vol
        
        # ============================================================
        # VOTING SYSTEM - Using scalar values only!
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
        if price < bb_low_val:
            votes.append('Buy')
        elif price > bb_up_val:
            votes.append('Sell')
        else:
            votes.append('Hold')
        
        # Vote 4: Volume
        if high_vol and len(votes) > 0:
            votes.append(votes[-1]) # Confirm last vote
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
        
        return {
            'symbol': stock_symbol.replace('.NS', ''),
            'signal': signal,
            'confidence': round(confidence, 1),
            'close_price': round(price, 2),
            'rsi': round(rsi_val, 1),
            'buy_votes': buy_count,
            'sell_votes': sell_count,
            'hold_votes': hold_count,
            'signal_date': datetime.now().date().isoformat()
        }
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return None

# ================================================================
# UPLOAD TO SUPABASE
# ================================================================

def upload_signal(data):
    """Upload signal to database"""
    try:
        supabase.table('signals').upsert(
            data, 
            on_conflict='symbol,signal_date'
        ).execute()
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
        print()
        print(f"🟢 Buy signals: {len(df[df['signal'] == 'Buy'])}")
        print(f"🔴 Sell signals: {len(df[df['signal'] == 'Sell'])}")
        print(f"⚪ Hold signals: {len(df[df['signal'] == 'Hold'])}")
        print(f"📈 Average confidence: {df['confidence'].mean():.1f}%")
        
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
