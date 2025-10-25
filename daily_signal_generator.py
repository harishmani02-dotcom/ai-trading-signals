#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
AI TRADING SIGNALS - DAILY GENERATOR
Automatically generates Buy/Sell/Hold signals for Indian stocks
Runs daily at 6 PM IST via GitHub Actions
Uploads results to Supabase database
"""

import os
import sys
import time
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# ================================================================
# CONFIGURATION
# ================================================================

# Get configuration from environment variables (GitHub Secrets)
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
STOCK_LIST = os.getenv('STOCK_LIST', 'RELIANCE.NS,TCS.NS,INFY.NS,HDFCBANK.NS,ITC.NS')

# Validate environment variables
if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ ERROR: Missing SUPABASE_URL or SUPABASE_KEY environment variables")
    print("Please set them in GitHub Secrets")
    sys.exit(1)

# Parse stock list
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
    from supabase import create_client, Client
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("✅ Connected to Supabase successfully")
    print()
except Exception as e:
    print(f"❌ ERROR: Failed to connect to Supabase: {str(e)}")
    sys.exit(1)

# ================================================================
# TECHNICAL INDICATOR FUNCTIONS
# ================================================================

def calculate_rsi(prices, period=14):
    """Calculate Relative Strength Index"""
    changes = prices.diff()
    gains = changes.where(changes > 0, 0)
    losses = -changes.where(changes < 0, 0)
    
    avg_gain = gains.rolling(window=period).mean()
    avg_loss = losses.rolling(window=period).mean()
    avg_loss = avg_loss.replace(0, 0.0001)  # Avoid division by zero
    
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def calculate_macd(prices):
    """Calculate MACD and Signal Line"""
    ema_12 = prices.ewm(span=12, adjust=False).mean()
    ema_26 = prices.ewm(span=26, adjust=False).mean()
    macd = ema_12 - ema_26
    signal_line = macd.ewm(span=9, adjust=False).mean()
    return macd, signal_line

def calculate_bollinger_bands(prices, period=20):
    """Calculate Bollinger Bands"""
    sma = prices.rolling(window=period).mean()
    std = prices.rolling(window=period).std()
    upper_band = sma + (2 * std)
    lower_band = sma - (2 * std)
    return upper_band, sma, lower_band

def calculate_volume_signal(volume, period=20):
    """Check if volume is higher than average"""
    avg_volume = volume.rolling(window=period).mean()
    return volume > avg_volume

def is_bullish_candle(open_price, close_price):
    """Check if candlestick is bullish"""
    return close_price > open_price

# ================================================================
# SIGNAL GENERATION
# ================================================================

def generate_signal_for_stock(stock_symbol):
    """
    Generate Buy/Sell/Hold signal for one stock
    Returns: dict with signal data or None if failed
    """
    
    print(f"📈 Processing: {stock_symbol.replace('.NS', '')}...", end=" ")
    
    try:
        # Download 90 days of data (need history for indicators)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=90)
        
        data = yf.download(
            stock_symbol, 
            start=start_date, 
            end=end_date, 
            interval='1d', 
            progress=False,
            threads=False
        )
        
        if data.empty or len(data) < 50:
            print(f"❌ Insufficient data ({len(data)} days)")
            return None
        
        # Calculate indicators
        rsi = calculate_rsi(data['Close'])
        macd, macd_signal = calculate_macd(data['Close'])
        bb_upper, bb_middle, bb_lower = calculate_bollinger_bands(data['Close'])
        volume_high = calculate_volume_signal(data['Volume'])
        
        # Get latest values
        idx = -1
        current_price = float(data['Close'].iloc[idx])
        current_rsi = float(rsi.iloc[idx])
        current_macd = float(macd.iloc[idx])
        current_macd_signal = float(macd_signal.iloc[idx])
        current_bb_upper = float(bb_upper.iloc[idx])
        current_bb_lower = float(bb_lower.iloc[idx])
        current_volume_high = bool(volume_high.iloc[idx])
        current_open = float(data['Open'].iloc[idx])
        current_close = float(data['Close'].iloc[idx])
        
        # Voting system
        votes = []
        
        # Vote 1: RSI
        if current_rsi < 30:
            votes.append('Buy')
        elif current_rsi > 70:
            votes.append('Sell')
        else:
            votes.append('Hold')
        
        # Vote 2: MACD
        if current_macd > current_macd_signal:
            votes.append('Buy')
        else:
            votes.append('Sell')
        
        # Vote 3: Bollinger Bands
        if current_price < current_bb_lower:
            votes.append('Buy')
        elif current_price > current_bb_upper:
            votes.append('Sell')
        else:
            votes.append('Hold')
        
        # Vote 4: Volume
        if current_volume_high:
            votes.append(votes[-1])  # Confirm trend
        else:
            votes.append('Hold')
        
        # Vote 5: Candlestick
        if is_bullish_candle(current_open, current_close):
            votes.append('Buy')
        else:
            votes.append('Sell')
        
        # Count votes
        buy_votes = votes.count('Buy')
        sell_votes = votes.count('Sell')
        hold_votes = votes.count('Hold')
        
        # Determine final signal
        if buy_votes >= 3:
            final_signal = 'Buy'
            confidence = (buy_votes / 5) * 100
        elif sell_votes >= 3:
            final_signal = 'Sell'
            confidence = (sell_votes / 5) * 100
        else:
            final_signal = 'Hold'
            confidence = (max(buy_votes, sell_votes, hold_votes) / 5) * 100
        
        print(f"✅ {final_signal} ({confidence:.0f}%)")
        
        return {
            'symbol': stock_symbol.replace('.NS', ''),
            'signal': final_signal,
            'confidence': round(confidence, 1),
            'close_price': round(current_price, 2),
            'rsi': round(current_rsi, 1),
            'buy_votes': buy_votes,
            'sell_votes': sell_votes,
            'hold_votes': hold_votes,
            'signal_date': datetime.now().date().isoformat()
        }
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return None

# ================================================================
# SUPABASE UPLOAD
# ================================================================

def upload_to_supabase(signal_data):
    """
    Upload signal to Supabase database
    Uses upsert to avoid duplicates
    """
    try:
        response = supabase.table('signals').upsert(
            signal_data,
            on_conflict='symbol,signal_date'
        ).execute()
        
        return True
    except Exception as e:
        print(f"   ⚠️  Failed to upload to Supabase: {str(e)}")
        return False

# ================================================================
# MAIN EXECUTION
# ================================================================

def main():
    """Main function - orchestrates the entire process"""
    
    print("🚀 Starting signal generation...\n")
    
    successful = 0
    failed = 0
    all_signals = []
    
    # Process each stock
    for i, stock in enumerate(STOCKS, 1):
        print(f"[{i}/{len(STOCKS)}] ", end="")
        
        signal_data = generate_signal_for_stock(stock)
        
        if signal_data:
            all_signals.append(signal_data)
            
            # Upload to Supabase
            if upload_to_supabase(signal_data):
                successful += 1
            else:
                failed += 1
        else:
            failed += 1
        
        # Small delay to avoid rate limiting
        time.sleep(1)
    
    # Summary
    print()
    print("=" * 70)
    print("📊 SUMMARY")
    print("=" * 70)
    print(f"✅ Successfully processed: {successful} stocks")
    print(f"❌ Failed: {failed} stocks")
    
    if all_signals:
        signals_df = pd.DataFrame(all_signals)
        print()
        print("🟢 Buy signals:", len(signals_df[signals_df['signal'] == 'Buy']))
        print("🔴 Sell signals:", len(signals_df[signals_df['signal'] == 'Sell']))
        print("⚪ Hold signals:", len(signals_df[signals_df['signal'] == 'Hold']))
        print()
        print("📈 Average confidence:", round(signals_df['confidence'].mean(), 1), "%")
        
        # Show top signals
        print()
        print("🏆 TOP 3 SIGNALS (Highest Confidence):")
        top_signals = signals_df.nlargest(3, 'confidence')
        for _, row in top_signals.iterrows():
            emoji = '🟢' if row['signal'] == 'Buy' else '🔴' if row['signal'] == 'Sell' else '⚪'
            print(f"   {emoji} {row['symbol']:12s} {row['signal']:5s} {row['confidence']:.0f}%  ₹{row['close_price']:.2f}")
    
    print()
    print("=" * 70)
    print("✅ DAILY SIGNAL GENERATION COMPLETE!")
    print(f"🕐 Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}")
    print("=" * 70)
    
    # Exit with appropriate code
    sys.exit(0 if failed == 0 else 1)

# ================================================================
# ENTRY POINT
# ================================================================

if __name__ == "__main__":
    main()
