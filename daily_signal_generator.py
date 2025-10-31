# python
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
import logging
import re
from random import uniform
warnings.filterwarnings('ignore')

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime

# Emoji constants (use these in f-strings)
EMOJI_ROBOT   = "🤖"
EMOJI_CAL     = "📅"
EMOJI_CHART   = "📊"
EMOJI_LINK    = "🔗"
EMOJI_CHECK   = "✅"
EMOJI_CROSS   = "❌"
EMOJI_RUPEE   = "₹"
EMOJI_MONEY   = "💰"
EMOJI_GREEN   = "🟢"
EMOJI_RED     = "🔴"
EMOJI_WHITE   = "⚪"
EMOJI_CLOCK   = "🕐"
EMOJI_WARNING = "⚠️"
EMOJI_ARROW   = "→"
EMOJI_ROCKET  = "🚀"

# ================================================================
# CONFIGURATION
# ================================================================

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
STOCK_LIST = os.getenv('STOCK_LIST', 'RELIANCE.NS,TCS.NS,INFY.NS')

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
# Ticker pattern: e.g. RELIANCE.NS, INFY, AAPL, etc.
TICKER_RE = re.compile(r'^[A-Z0-9][A-Z0-9._-]{0,18}(?:\.[A-Z]{1,5})?$')

def sanitize_tickers(raw: str):
    items = []
    for part in raw.split(','):
        t = str(part).strip()
        if not t:
            continue
        # remove surrounding quotes and common punctuation
        t = t.strip(" '\"`;:()[]{}<>")
        # if removing punctuation made it empty, skip
        if not t:
            logging.debug(f"Skipped empty token after strip: {part!r}")
            continue
        # split by whitespace and use first token (if any)
        tokens = t.split()
        if not tokens:
            logging.debug(f"No tokens found after split: {part!r}")
            continue
        t = tokens[0].upper()
        # remove any characters that aren't allowed in tickers
        t = re.sub(r'[^A-Z0-9._-]', '', t)
        if not t:
            logging.debug(f"Empty after removing invalid chars: {part!r}")
            continue
        if TICKER_RE.match(t):
            items.append(t)
        else:
            logging.debug(f"Filtered invalid ticker token: {part!r} -> {t!r}")
    # dedupe preserving order
    seen = set()
    out = []
    for t in items:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out

STOCKS = sanitize_tickers(STOCK_LIST)

print("=" * 70)
print(f"{EMOJI_ROBOT} AI TRADING SIGNALS - DAILY GENERATOR")
print("=" * 70)
print(f"{EMOJI_CAL} Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}")
print(f"{EMOJI_CHART} Stocks to analyze: {len(STOCKS)}")
print(f"{EMOJI_LINK} Supabase URL: {SUPABASE_URL[:30]}...")
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

# (Remaining code unchanged from prior working version)
def get_value(series, idx):
    try:
        val = series.iloc[idx]
        if isinstance(val, pd.Series):
            for v in val:
                if not pd.isna(v):
                    val = v
                    break
            else:
                return None
        if pd.isna(val):
            return None
        return float(val)
    except Exception as e:
        logging.debug(f"Error extracting value: {e}")
        return None

def calculate_rsi(prices, period=14):
    delta = prices.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss.replace(0, 0.0001)
    return 100 - (100 / (1 + rs))

def calculate_macd(prices):
    ema12 = prices.ewm(span=12, adjust=False).mean()
    ema26 = prices.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    return macd, signal

def calculate_bollinger(prices, period=20):
    sma = prices.rolling(window=period).mean()
    std = prices.rolling(window=period).std()
    upper = sma + (2 * std)
    lower = sma - (2 * std)
    return upper, sma, lower

def fetch_history_with_retries(ticker: str, period='3mo', interval='1d', max_retries=3, min_days=30, pause_base=0.6):
    for attempt in range(1, max_retries + 1):
        try:
            logging.info(f"Fetching {ticker} (attempt {attempt})")
            data = yf.download(
                ticker,
                period=period,
                interval=interval,
                progress=False,
                auto_adjust=True,
                actions=False,
            )
            if hasattr(data, "columns") and isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)
            if data is None or data.empty:
                raise ValueError("Empty history returned")
            if 'Close' not in data.columns:
                raise ValueError("Close column missing")
            valid_closes = data['Close'].dropna()
            if len(valid_closes) < min_days:
                raise ValueError(f"Not enough valid price rows: {len(valid_closes)}")
            return data
        except Exception as e:
            logging.warning(f"Failed fetch for {ticker}: {e}")
            if attempt < max_retries:
                sleep_time = pause_base * (2 ** (attempt - 1)) + uniform(0, 0.5)
                logging.info(f"Sleeping {sleep_time:.2f}s before retry")
                time.sleep(sleep_time)
            else:
                logging.error(f"Giving up on {ticker} after {max_retries} attempts")
                return None

def generate_signal(stock_symbol):
    pretty = stock_symbol.replace('.NS', '')
    print(f"{EMOJI_CHART} Processing: {pretty}...", end=" ")
    try:
        data = fetch_history_with_retries(stock_symbol, period='3mo', interval='1d', max_retries=3, min_days=30)
        if data is None:
            print(f"{EMOJI_CROSS} Not enough data or fetch failed")
            return None

        print(f"{EMOJI_CHECK} Got {len(data)} days", end=f" {EMOJI_ARROW} ")

        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)

        if not all(c in data.columns for c in ('Close', 'Open', 'Volume')):
            print(f"{EMOJI_CROSS} Missing required columns")
            return None

        close_col = data['Close']
        open_col = data['Open']
        volume_col = data['Volume']

        last_idx = -1
        close_price = get_value(close_col, last_idx)
        open_price = get_value(open_col, last_idx)
        volume = get_value(volume_col, last_idx)

        if close_price is None or close_price <= 0:
            print(f"{EMOJI_CROSS} Invalid price data (close={close_price})")
            return None

        print(f"Price: {EMOJI_RUPEE}{close_price:.2f}", end=f" {EMOJI_ARROW} ")

        rsi = calculate_rsi(close_col)
        macd, macd_sig = calculate_macd(close_col)
        bb_up, bb_mid, bb_low = calculate_bollinger(close_col)

        rsi_val = get_value(rsi, last_idx) or 50.0
        macd_val = get_value(macd, last_idx) or 0.0
        macd_sig_val = get_value(macd_sig, last_idx) or 0.0
        bb_up_val = get_value(bb_up, last_idx) or close_price
        bb_low_val = get_value(bb_low, last_idx) or close_price

        try:
            avg_vol = float(volume_col.rolling(20).mean().iloc[last_idx])
        except Exception:
            avg_vol = None
        high_vol = (volume is not None and avg_vol is not None and volume > avg_vol)

        votes = []

        if rsi_val < 30:
            votes.append('Buy')
        elif rsi_val > 70:
            votes.append('Sell')
        else:
            votes.append('Hold')

        votes.append('Buy' if macd_val > macd_sig_val else 'Sell')

        if close_price < bb_low_val:
            votes.append('Buy')
        elif close_price > bb_up_val:
            votes.append('Sell')
        else:
            votes.append('Hold')

        if high_vol and votes:
            votes.append(votes[-1])
        else:
            votes.append('Hold')

        if open_price is not None and close_price > open_price:
            votes.append('Buy')
        else:
            votes.append('Sell')

        buy_count = votes.count('Buy')
        sell_count = votes.count('Sell')
        hold_count = votes.count('Hold')

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

        print(f"{EMOJI_MONEY} Storing price: {EMOJI_RUPEE}{result['close_price']}")

        return result

    except Exception as e:
        logging.exception(f"{EMOJI_CROSS} Error processing {stock_symbol}: {e}")
        return None

def upload_signal(data):
    try:
        if data is None:
            return False
        if data.get('close_price') is None or data['close_price'] <= 0:
            logging.warning(f"{EMOJI_WARNING} Invalid price {data.get('close_price')}, skipping upload")
            return False

        resp = supabase.table('signals').upsert(
            data,
            on_conflict='symbol,signal_date'
        ).execute()

        if isinstance(resp, dict) and resp.get('error'):
            logging.error(f"{EMOJI_WARNING} Supabase error: {resp.get('error')}")
            return False

        print(f" {EMOJI_CHECK} Uploaded to Supabase (price: {EMOJI_RUPEE}{data['close_price']})")
        return True

    except Exception as e:
        logging.exception(f" {EMOJI_WARNING} Upload failed: {e}")
        return False

def main():
    print(f"{EMOJI_ROCKET} Starting signal generation...n")

    success = 0
    failed = 0
    results = []
    failed_tickers = []

    for i, stock in enumerate(STOCKS, 1):
        print(f"[{i}/{len(STOCKS)}] ", end="")
        signal = generate_signal(stock)

        if signal:
            results.append(signal)
            ok = upload_signal(signal)
            if ok:
                success += 1
            else:
                failed += 1
                failed_tickers.append(stock)
        else:
            failed += 1
            failed_tickers.append(stock)

        time.sleep(0.8 + uniform(0, 0.4))

    if failed_tickers:
        try:
            with open('failed_tickers.txt', 'w') as fh:
                for t in failed_tickers:
                    fh.write(f"{t}\n")
            logging.info(f"Wrote {len(failed_tickers)} failed tickers to failed_tickers.txt")
        except Exception:
            logging.exception("Failed to write failed_tickers.txt")

    print()
    print("=" * 70)
    print(f"{EMOJI_CHART} SUMMARY")
    print("=" * 70)
    print(f"{EMOJI_CHECK} Successfully processed: {success} stocks")
    print(f"{EMOJI_CROSS} Failed: {failed} stocks")

    if results:
        df = pd.DataFrame(results)
        zero_prices = df[df['close_price'] == 0]
        if len(zero_prices) > 0:
            print(f"\n{EMOJI_WARNING} WARNING: {len(zero_prices)} stocks have zero price!")
            for _, row in zero_prices.iterrows():
                print(f" - {row['symbol']}: price = {row['close_price']}")

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
