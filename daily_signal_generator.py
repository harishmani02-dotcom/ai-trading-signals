#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Finspark AI v6.4.2 FIXED - Upload & Syntax bugs resolved"""

import os
import sys
import time
import warnings
import logging
import re
import random
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz

warnings.filterwarnings('ignore')

import yfinance as yf
import pandas as pd
import numpy as np
from supabase import create_client

# -------------------------
# Configuration (env/defaults)
# -------------------------
SUPABASE_URL = os.getenv('SUPABASE_URL', '')
SUPABASE_KEY = os.getenv('SUPABASE_KEY', '')
STOCK_LIST = os.getenv('STOCK_LIST', 'RELIANCE.NS,TCS.NS,INFY.NS,HDFCBANK.NS,ICICIBANK.NS')
TABLE_NAME = os.getenv("TABLE_NAME", "signals")
CLEAR_TODAY = os.getenv("CLEAR_TODAY", "false").lower() in ("1", "true", "yes")
MAX_SIGNALS = int(os.getenv("MAX_SIGNALS", "150"))
MIN_CONF_TO_UPLOAD = float(os.getenv("MIN_CONF_TO_UPLOAD", "60"))
MIN_RR_TO_SHOW = float(os.getenv("MIN_RR", "1.8"))
INTERVAL = '15m'
INTRADAY_PERIOD = '5d'
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20"))
RETRY_DELAY = float(os.getenv("RETRY_DELAY", "0.5"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
IST = pytz.timezone("Asia/Kolkata")
MARKET_OPEN = (9, 15)
MARKET_CLOSE = (15, 30)
RSI_PERIOD = 14
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
BB_PERIOD = 20
VOL_PERIOD = 10
SL_ATR_MULT = float(os.getenv("SL_ATR_MULT", "1.2"))
TP_ATR_MULT = float(os.getenv("TP_ATR_MULT", "2.4"))
MAX_CIRCUIT_PCT = float(os.getenv("MAX_CIRCUIT_PCT", "0.18"))  # keep as fraction (0.18 => 18%)
EMA_TREND_BUFFER = float(os.getenv("EMA_TREND_BUFFER", "0.002"))
MIN_CONFIDENCE = 60
MIN_VOTES_FOR_ACTION = int(os.getenv("MIN_VOTES", "5"))
MIN_STRENGTH_SCORE = int(os.getenv("MIN_STRENGTH", "8"))
VOL_SPIKE_MULT = float(os.getenv("VOL_SPIKE_MULT", "3.0"))
MAX_INTRADAY_GAPS = int(os.getenv("MAX_INTRADAY_GAPS", "10"))

START_TS = time.time()
LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("FinsparkV6.4.2")

if not SUPABASE_URL or not SUPABASE_KEY:
    logger.critical("❌ Missing SUPABASE credentials")
    sys.exit(1)

TICKER_RE = re.compile(r'^[A-Z0-9][A-Z0-9._-]{0,18}(?:\.[A-Z]{1,5})?$')


def sanitize_tickers(raw):
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

print("=" * 70)
print("🏆 FINSPARK AI v6.4.2 FIXED - CTO PATCH")
print("=" * 70)
print(f"📅 {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}")
print(f"📊 Stocks: {len(STOCKS)} | Workers: {MAX_WORKERS} | Table: {TABLE_NAME}")
print(f"🎯 SL: {SL_ATR_MULT}x | TP: {TP_ATR_MULT}x | Vol: {VOL_SPIKE_MULT}x")
print(f"⚙ Max intraday gaps allowed: {MAX_INTRADAY_GAPS}")
print("=" * 70)


def is_market_open():
    now = datetime.now(IST)
    if now.weekday() > 4:
        return False, "Weekend"
    current_time = (now.hour, now.minute)
    is_open = (MARKET_OPEN <= current_time < MARKET_CLOSE)
    return is_open, "Open" if is_open else "Closed"


market_open, status = is_market_open()
print(f"📍 Market: {status}\n")

# Supabase client
try:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    logger.info("✅ Supabase connected")
except Exception as e:
    logger.critical(f"❌ Supabase failed: {e}")
    sys.exit(1)


# ----- Utilities -----
def safe_iloc(series, idx, fallback=np.nan):
    if not isinstance(series, pd.Series) or series.empty:
        return fallback
    try:
        if idx < 0 and abs(idx) > len(series):
            return fallback
        if idx >= 0 and idx >= len(series):
            return fallback
        val = series.iloc[idx]
        return val if pd.notna(val) else fallback
    except Exception:
        return fallback


def get_value(series, idx):
    val = safe_iloc(series, idx)
    if val is None or pd.isna(val):
        return None
    try:
        float_val = float(val)
        return float_val if float_val > 0.0 else None
    except Exception:
        return None


# ----- Data quality & indicators -----
def validate_data_quality(data):
    # Enough candles
    if len(data) < 50:
        return False, f"Insufficient ({len(data)})"

    # Intraday gaps (very coarse)
    if len(data) > 1:
        time_diff = data.index.to_series().diff()
        expected_diff = pd.Timedelta(minutes=15)
        intraday_gaps = (time_diff > expected_diff * 2).sum()
        if intraday_gaps > MAX_INTRADAY_GAPS:
            return False, f"Gaps ({intraday_gaps})"

    # Staleness (allow slightly more headroom)
    last_time = data.index[-1]
    try:
        if last_time.tzinfo is None:
            # yfinance naive -> treat as UTC then convert
            last_time = last_time.tz_localize('UTC').tz_convert(IST)
        else:
            last_time = last_time.tz_convert(IST)
    except Exception:
        # conservative fallback
        last_time = pd.Timestamp(last_time).tz_localize('UTC').tz_convert(IST)

    now = pd.Timestamp.now(tz=IST)
    staleness_mins = (now - last_time).total_seconds() / 60
    if staleness_mins > 60:  # allow up to 60m (safer for occasional yfinance delay)
        return False, f"Stale ({staleness_mins:.0f}m)"

    # extreme moves check (loosened)
    if 'Close' in data.columns:
        price_changes = data['Close'].pct_change().abs()
        extreme_moves = (price_changes > 0.10).sum()
        if extreme_moves > 8:
            return False, f"Volatility ({extreme_moves})"

    return True, "OK"


def calculate_indicators(df):
    close = pd.to_numeric(df['Close'], errors='coerce').astype(float)
    high = pd.to_numeric(df['High'], errors='coerce').astype(float)
    low = pd.to_numeric(df['Low'], errors='coerce').astype(float)

    tr1 = high - low
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1 / 14, adjust=False).mean()

    delta = close.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    ma_up = up.ewm(alpha=1 / RSI_PERIOD, adjust=False).mean()
    ma_down = down.ewm(alpha=1 / RSI_PERIOD, adjust=False).mean()
    ma_down = ma_down.replace(0, 1e-10)
    rs = ma_up / ma_down
    rs = rs.clip(lower=0.0001, upper=1000)
    rsi = 100 - (100 / (1 + rs))

    ema_fast = close.ewm(span=MACD_FAST, adjust=False).mean()
    ema_slow = close.ewm(span=MACD_SLOW, adjust=False).mean()
    macd = ema_fast - ema_slow
    macd_signal = macd.ewm(span=MACD_SIGNAL, adjust=False).mean()
    macd_hist = macd - macd_signal

    sma = close.rolling(BB_PERIOD).mean()
    std = close.rolling(BB_PERIOD).std()
    bb_upper = sma + (2 * std)
    bb_lower = sma - (2 * std)

    vol_avg = df['Volume'].rolling(VOL_PERIOD + 1).mean().shift(1)  # use prior average to detect spikes better
    ema_20 = close.ewm(span=20, adjust=False).mean()
    ema_50 = close.ewm(span=50, adjust=False).mean()

    return {
        'rsi': rsi,
        'atr': atr,
        'macd_hist': macd_hist,
        'bb_upper': bb_upper,
        'bb_lower': bb_lower,
        'vol_avg': vol_avg,
        'ema_20': ema_20,
        'ema_50': ema_50
    }


# ----- Fetch with exponential backoff -----
def fetch_data(ticker, max_retries=MAX_RETRIES):
    for attempt in range(1, max_retries + 1):
        try:
            if attempt > 1:
                delay = RETRY_DELAY * (2 ** (attempt - 2)) + random.random() * 0.2
                time.sleep(delay)
            stock = yf.Ticker(ticker)
            data = stock.history(period=INTRADAY_PERIOD, interval=INTERVAL, auto_adjust=True)
            if hasattr(data, "columns") and isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)
            if data is None or data.empty:
                raise ValueError("Empty")
            data = data.sort_index()
            if len(data) < 50:
                raise ValueError(f"{len(data)}")
            if 'Close' not in data.columns:
                raise ValueError("No Close")
            valid_closes = data['Close'].dropna()
            if len(valid_closes) < 50:
                raise ValueError(f"{len(valid_closes)} valid")
            last_close = data['Close'].iloc[-1]
            if pd.isna(last_close) or last_close <= 0:
                raise ValueError("Invalid")
            return data
        except Exception as e:
            logger.debug(f"{ticker}: attempt {attempt} failed: {e}")
            if attempt == max_retries:
                logger.debug(f"{ticker} failed permanently after {max_retries} attempts: {e}")
                return None
    return None


def determine_trend(ema_20, ema_50, close_price):
    # use buffer as fraction (e.g. 0.002 => 0.2%)
    if ema_20 > ema_50 * (1 + EMA_TREND_BUFFER) and close_price > ema_20:
        return 'Uptrend'
    elif ema_20 < ema_50 * (1 - EMA_TREND_BUFFER) and close_price < ema_20:
        return 'Downtrend'
    return 'Sideways'


def get_tier(confidence, risk_reward):
    try:
        if confidence >= 85 and risk_reward and risk_reward >= 2.5:
            return "S"
        if confidence >= 78 and risk_reward and risk_reward >= 2.0:
            return "A"
        if confidence >= 70:
            return "B"
    except Exception:
        pass
    return "C"


# ----- Signal generation -----
def generate_signal(stock_symbol, stock_num=0, total=0):
    pretty = stock_symbol.replace('.NS', '').upper()
    prefix = f"[{stock_num}/{total}] " if total > 0 else ""
    try:
        data = fetch_data(stock_symbol)
        if data is None:
            logger.debug(f"{prefix}❌ {pretty}: Fetch failed")
            return None

        is_valid, quality_msg = validate_data_quality(data)
        if not is_valid:
            logger.debug(f"{prefix}⚠️ {pretty}: {quality_msg}")
            return None

        last_idx = -1
        close_price = get_value(data['Close'], last_idx)
        open_price = get_value(data['Open'], last_idx)
        high_price = get_value(data['High'], last_idx)
        low_price = get_value(data['Low'], last_idx)
        volume = get_value(data['Volume'], last_idx)
        prev_close = get_value(data['Close'], -2)

        if close_price is None or close_price <= 0:
            logger.debug(f"{prefix}❌ {pretty}: Invalid price")
            return None

        # circuit check vs previous close (fix)
        if prev_close and prev_close > 0:
            circuit_pct = abs(close_price - prev_close) / prev_close
            if circuit_pct > MAX_CIRCUIT_PCT:
                logger.debug(f"{prefix}⚠️ {pretty}: Circuit ({circuit_pct:.1%})")
                return None

        indicators = calculate_indicators(data)
        rsi_val = get_value(indicators['rsi'], last_idx) or 50.0
        macd_hist = get_value(indicators['macd_hist'], last_idx)
        macd_hist = float(macd_hist) if macd_hist is not None else 0.0
        bb_up_val = get_value(indicators['bb_upper'], last_idx) or (close_price * 1.02)
        bb_low_val = get_value(indicators['bb_lower'], last_idx) or (close_price * 0.98)
        vol_avg = get_value(indicators['vol_avg'], last_idx)
        ema_20 = get_value(indicators['ema_20'], last_idx) or close_price
        ema_50 = get_value(indicators['ema_50'], last_idx) or close_price
        atr_val = get_value(indicators['atr'], last_idx)
        if atr_val is None or atr_val <= 0:
            atr_val = max(close_price * 0.002, 0.1)

        # votes
        buy_votes = sell_votes = hold_votes = 0
        strength_score = 0
        reasons = []

        trend = determine_trend(ema_20, ema_50, close_price)
        if trend == 'Uptrend':
            buy_votes += 1
            strength_score += 3
            reasons.append("Uptrend")
        elif trend == 'Downtrend':
            sell_votes += 1
            strength_score += 3
            reasons.append("Downtrend")

        # RSI rules
        if rsi_val < 30:
            buy_votes += 1
            strength_score += 3
            reasons.append(f"RSI({rsi_val:.1f})")
        elif rsi_val > 70:
            sell_votes += 1
            strength_score += 3
            reasons.append(f"RSI({rsi_val:.1f})")
        elif rsi_val < 40 and trend == 'Uptrend':
            buy_votes += 1
            strength_score += 1
        elif rsi_val > 60 and trend == 'Downtrend':
            sell_votes += 1
            strength_score += 1
        else:
            hold_votes += 1

        # MACD
        if macd_hist > 0:
            buy_votes += 1
            strength_score += 2
            reasons.append("MACD+")
        elif macd_hist < 0:
            sell_votes += 1
            strength_score += 2
            reasons.append("MACD-")
        else:
            hold_votes += 1

        # Bollinger position
        if pd.isna(bb_up_val) or pd.isna(bb_low_val):
            hold_votes += 1
        else:
            bb_width = bb_up_val - bb_low_val
            if bb_width <= 0:
                hold_votes += 1
            else:
                bb_position = (close_price - bb_low_val) / bb_width
                if bb_position < 0.15:
                    buy_votes += 1
                    strength_score += 2
                    reasons.append("BB_Low")
                elif bb_position > 0.85:
                    sell_votes += 1
                    strength_score += 2
                    reasons.append("BB_High")
                else:
                    hold_votes += 1

        # Volume spike (use vol_avg computed as prior average)
        if volume and vol_avg and vol_avg > 0:
            vol_ratio = volume / vol_avg
            if vol_ratio >= VOL_SPIKE_MULT:
                if close_price > open_price:
                    buy_votes += 1
                    strength_score += 3
                    reasons.append(f"Vol({vol_ratio:.1f}x)")
                elif close_price < open_price:
                    sell_votes += 1
                    strength_score += 3
                    reasons.append("VolDump")
                else:
                    hold_votes += 1
            elif vol_ratio < 0.6:
                hold_votes += 1
                strength_score -= 2
            else:
                hold_votes += 1
        else:
            hold_votes += 1

        # Candle body
        candle_body = (close_price - open_price) if open_price else 0
        candle_range = (high_price - low_price) if (high_price and low_price) else 0
        if candle_range > 0:
            body_ratio = abs(candle_body) / candle_range
            if candle_body > 0 and body_ratio > 0.7:
                buy_votes += 1
                strength_score += 2
            elif candle_body < 0 and body_ratio > 0.7:
                sell_votes += 1
                strength_score += 2
            elif body_ratio < 0.1:
                hold_votes += 1
                strength_score -= 1
            else:
                hold_votes += 1
        else:
            hold_votes += 1

        # Decision
        signal = 'Hold'
        base_conf = 40.0

        if buy_votes >= MIN_VOTES_FOR_ACTION and trend != 'Downtrend':
            signal = 'Buy'
            base_conf += min(strength_score, 50) + (buy_votes * 5)
        elif sell_votes >= MIN_VOTES_FOR_ACTION and trend != 'Uptrend':
            signal = 'Sell'
            base_conf += min(strength_score, 50) + (sell_votes * 5)
        elif buy_votes == 4 and trend == 'Uptrend' and strength_score >= MIN_STRENGTH_SCORE:
            signal = 'Buy'
            base_conf += min(strength_score, 40) + (buy_votes * 4)
        elif sell_votes == 4 and trend == 'Downtrend' and strength_score >= MIN_STRENGTH_SCORE:
            signal = 'Sell'
            base_conf += min(strength_score, 40) + (sell_votes * 4)
        else:
            signal = 'Hold'
            hold_votes = max(1, hold_votes)

        confidence = min(base_conf, 95.0)
        if signal != 'Hold' and confidence < MIN_CONFIDENCE:
            signal = 'Hold'
            confidence = 50.0

        sl = tp = rr = None
        if signal != 'Hold' and atr_val > 0:
            risk = atr_val * SL_ATR_MULT
            reward = atr_val * TP_ATR_MULT
            if signal == 'Buy':
                sl = round(close_price - risk, 2)
                tp = round(close_price + reward, 2)
            else:
                sl = round(close_price + risk, 2)
                tp = round(close_price - reward, 2)
            rr = round(reward / risk, 2) if risk > 0 else None

        now = datetime.now(IST)
        result = {
            'symbol': pretty,
            'interval': INTERVAL,
            'signal': signal,
            'confidence': round(float(confidence), 1),
            'close_price': round(float(close_price), 2),
            'stop_loss': sl,
            'target': tp,
            'risk_reward': rr,
            'signal_date': now.date().isoformat(),
            'signal_time': now.strftime('%H:%M:%S'),
            'rsi': round(float(rsi_val), 2),
            'buy_votes': buy_votes,
            'sell_votes': sell_votes,
            'hold_votes': hold_votes,
            'macd': round(float(macd_hist), 4)
        }

        tier = get_tier(confidence, rr)
        emoji = "🟢" if signal == 'Buy' else "🔴" if signal == 'Sell' else "⚪"
        tier_emoji = "🏆" if tier == "S" else "🥇" if tier == "A" else "🥈" if tier == "B" else ""
        sl_info = f"SL:₹{sl} T:₹{tp}" if (sl is not None and tp is not None) else ""
        rr_info = f"R:R{rr}" if rr is not None else ""
        trend_emoji = "📈" if trend == 'Uptrend' else "📉" if trend == 'Downtrend' else "➡️"
        reason_str = ", ".join(reasons[:2]) if reasons else ""
        logger.info(f"{prefix}{emoji} {pretty:12s} {signal:4s} ({confidence:.0f}%) {tier_emoji}{tier} @ ₹{close_price:.2f} {sl_info} {rr_info} {trend_emoji} [{reason_str}]")

        return result

    except Exception as e:
        logger.error(f"{prefix}❌ {pretty}: {e}")
        return None


# ----- Uploading -----
def upload_signals(signals, table_name):
    try:
        if not signals:
            return 0

        seen = set()
        unique_signals = []
        for s in signals:
            if s.get("confidence", 0) < MIN_CONF_TO_UPLOAD:
                continue
            key = (s["symbol"], s["interval"], s["signal_date"], s["signal_time"])
            if key not in seen:
                seen.add(key)
                unique_signals.append(s)

        if not unique_signals:
            return 0

        uploaded = 0
        for i in range(0, len(unique_signals), BATCH_SIZE):
            batch = unique_signals[i:i + BATCH_SIZE]
            try:
                resp = supabase.table(table_name).insert(batch).execute()
                # resp might be PostgrestResponse object or dict-like
                data = None
                # prefer attribute access
                data = getattr(resp, 'data', None)
                if data is None and isinstance(resp, dict):
                    data = resp.get('data')
                # check for errors both ways
                error = None
                if isinstance(resp, dict):
                    error = resp.get('error')
                else:
                    error = getattr(resp, 'error', None)
                if error:
                    logger.error(f"Supabase error: {error}")
                    continue
                count = len(data) if data else len(batch)
                uploaded += count
                logger.info(f"✅ Batch {i // BATCH_SIZE + 1}: {count}/{len(batch)}")
            except Exception as e:
                logger.error(f"Batch {i // BATCH_SIZE + 1} failed: {e}")
                continue

        return uploaded

    except Exception as e:
        logger.error(f"Upload failed: {e}")
        return 0


# ----- Main -----
def main():
    logger.info("🚀 Starting v6.4.2 FIXED...")
    market_open, status = is_market_open()
    if not market_open:
        logger.info(f"⏸ Market closed ({status}). Exiting.")
        sys.exit(0)

    if CLEAR_TODAY:
        try:
            today = datetime.now(IST).date().isoformat()
            logger.info(f"⚠️ Deleting old signals ({today})...")
            delete_resp = supabase.table(TABLE_NAME).delete().eq('signal_date', today).execute()
            deleted_count = getattr(delete_resp, 'data', None)
            deleted_count = len(deleted_count) if deleted_count else 0
            logger.info(f"✅ Deleted {deleted_count}")
        except Exception as e:
            logger.warning(f"⚠️ Delete failed: {e}")

    results = []
    failed_tickers = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_stock = {executor.submit(generate_signal, stock, i + 1, len(STOCKS)): stock for i, stock in enumerate(STOCKS)}
        for future in as_completed(future_to_stock):
            stock = future_to_stock[future]
            try:
                signal = future.result()
                if signal:
                    results.append(signal)
                else:
                    failed_tickers.append(stock)
            except Exception as e:
                logger.error(f"{stock}: {e}")
                failed_tickers.append(stock)

    if results:
        results_sorted = sorted(results, key=lambda r: r.get("confidence", 0), reverse=True)
        top_signals = results_sorted[:MAX_SIGNALS]
    else:
        top_signals = []

    logger.info(f"🚀 Uploading {len(top_signals)} signals...")
    uploaded = upload_signals(top_signals, TABLE_NAME)

    elapsed = time.time() - START_TS
    success = len(results)
    failed = len(failed_tickers)

    print("\n" + "=" * 70)
    print("📊 FINSPARK v6.4.2 FINAL SUMMARY")
    print("=" * 70)
    print(f"✅ Processed: {success} | ❌ Failed: {failed}")
    print(f"⚡ Time: {elapsed:.1f}s ({elapsed / len(STOCKS):.2f}s/stock)")
    print(f"⚡ Uploaded: {uploaded}/{success} ({100 * uploaded / max(success, 1):.1f}%)")

    if results:
        df = pd.DataFrame(results)
        buy_signals = df[df['signal'] == 'Buy']
        sell_signals = df[df['signal'] == 'Sell']
        hold_signals = df[df['signal'] == 'Hold']

        hq_buys = buy_signals[(buy_signals['confidence'] > 70) & (buy_signals['risk_reward'] >= MIN_RR_TO_SHOW)]
        hq_sells = sell_signals[(sell_signals['confidence'] > 70) & (sell_signals['risk_reward'] >= MIN_RR_TO_SHOW)]

        s_tier = df[(df['confidence'] >= 85) & (df['risk_reward'] >= 2.5)]
        a_tier = df[(df['confidence'] >= 78) & (df['risk_reward'] >= 2.0) & (df['confidence'] < 85)]

        print()
        print(f"🟢 Buy: {len(buy_signals)} (HQ: {len(hq_buys)}, R:R≥{MIN_RR_TO_SHOW})")
        print(f"🔴 Sell: {len(sell_signals)} (HQ: {len(hq_sells)}, R:R≥{MIN_RR_TO_SHOW})")
        print(f"⚪ Hold: {len(hold_signals)}")
        print(f"📊 Avg confidence: {df['confidence'].mean():.1f}%")
        print(f"🏆 S-Tier: {len(s_tier)} | 🥇 A-Tier: {len(a_tier)}")

        if len(s_tier) > 0:
            print("\n🏆 S-TIER ELITE(Conf≥85%,R:R≥2.5):")
            for _, row in s_tier.nlargest(5, 'confidence').iterrows():
                emoji = "🟢" if row['signal'] == 'Buy' else "🔴"
                print(f" {emoji} {row['symbol']:12s} {row['confidence']:.0f}% ₹{row['close_price']:.2f} → T:₹{row['target']} R:R{row['risk_reward']}")

        if len(a_tier) > 0:
            print("\n🥇 A-TIER(Conf≥78%,R:R≥2.0):")
            for _, row in a_tier.nlargest(3, 'confidence').iterrows():
                emoji = "🟢" if row['signal'] == 'Buy' else "🔴"
                print(f" {emoji} {row['symbol']:12s} {row['confidence']:.0f}% ₹{row['close_price']:.2f} → T:₹{row['target']} R:R{row['risk_reward']}")
        elif len(hq_buys) > 0:
            print(f"\n🔥 TOP BUY(Conf>70%,R:R≥{MIN_RR_TO_SHOW}):")
            for _, row in hq_buys.nlargest(3, 'confidence').iterrows():
                print(f" 🟢 {row['symbol']:12s} {row['confidence']:.0f}% ₹{row['close_price']:.2f} → T:₹{row['target']} R:R{row['risk_reward']}")

        if len(hq_sells) > 0:
            print(f"\n🔥 TOP SELL(Conf>70%,R:R≥{MIN_RR_TO_SHOW}):")
            for _, row in hq_sells.nlargest(3, 'confidence').iterrows():
                print(f" 🔴 {row['symbol']:12s} {row['confidence']:.0f}% ₹{row['close_price']:.2f} → T:₹{row['target']} R:R{row['risk_reward']}")

        valid_rr = df[df['risk_reward'].notna()]['risk_reward']
        if len(valid_rr) > 0:
            print(f"\n🎯 Avg R:R: {valid_rr.mean():.2f}")

    if failed_tickers:
        print(f"\n⚠️ Failed: {', '.join([t.replace('.NS', '') for t in failed_tickers[:10]])}")
        if len(failed_tickers) > 10:
            print(f"   +{len(failed_tickers) - 10} more")

    print("\n" + "=" * 70)
    print("✅ v6.4.2 FIXED COMPLETE!")
    print(f"🕐 {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}")
    print("🏆 Wilder's RSI/ATR | Gap-Proof | S-Tier | Production-ready")
    print("=" * 70)

    sys.exit(0 if success > 0 else 1)


if __name__ == "__main__":
    main()
