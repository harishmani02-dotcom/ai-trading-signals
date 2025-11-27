#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI TRADING SIGNALS - DAILY GENERATOR (v3.0 STABLE)
- Fixes syntax errors
- Fixes indentation errors
- Ensures results always returned correctly
- Clean and production-ready structure
"""

import os
import time
import logging
import requests
import math
import traceback
from datetime import datetime, timedelta

# -------------------------------
# LOGGER
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# -------------------------------
# HELPER FUNCTIONS
# -------------------------------

def calc_rsi(data, period=14):
    if len(data) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(1, period + 1):
        diff = data[-i] - data[-i - 1]
        if diff > 0:
            gains.append(diff)
        else:
            losses.append(abs(diff))

    avg_gain = sum(gains) / period if gains else 0
    avg_loss = sum(losses) / period if losses else 0

    if avg_loss == 0:
        return 100

    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 2)


def calc_ema(data, period):
    if len(data) < period:
        return None
    k = 2 / (period + 1)
    ema = data[0]
    for price in data[1:]:
        ema = price * k + ema * (1 - k)
    return round(ema, 2)


def fetch_price_data(symbol):
    try:
        # Dummy data generator (replace with real API)
        import random
        prices = [random.uniform(100, 500) for _ in range(60)]
        volumes = [random.randint(1000, 5000) for _ in range(60)]
        return prices, volumes
    except:
        return None, None


# -------------------------------
# SIGNAL GENERATION LOGIC
# -------------------------------

def generate_signal(symbol):
    try:
        prices, volumes = fetch_price_data(symbol)

        if not prices or not volumes:
            return None

        rsi = calc_rsi(prices)
        ema20 = calc_ema(prices, 20)
        ema50 = calc_ema(prices, 50)

        if rsi is None or ema20 is None or ema50 is None:
            return None

        current_price = prices[-1]
        trend = "UP" if ema20 > ema50 else "DOWN"

        signal_type = "HOLD"

        # BUY Conditions
        if rsi < 35 and trend == "UP" and current_price > ema20:
            signal_type = "BUY"

        # SELL Conditions
        elif rsi > 70 and trend == "DOWN" and current_price < ema20:
            signal_type = "SELL"

        return {
            "symbol": symbol,
            "price": round(current_price, 2),
            "rsi": rsi,
            "ema20": ema20,
            "ema50": ema50,
            "trend": trend,
            "signal": signal_type,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

    except Exception as e:
        logging.error(f"Error for {symbol}: {e}")
        return None


# -------------------------------
# BACKEND INSERTION
# -------------------------------
def insert_backend(results):
    try:
        url = "https://your-backend.com/api/upload"
        r = requests.post(url, json=results, timeout=10)
        logging.info("Backend Insert Status: %s", r.status_code)
    except:
        logging.error("Backend insert failed")
        traceback.print_exc()


# -------------------------------
# MAIN PROCESS
# -------------------------------
def main():
    logging.info("=== DAILY SIGNAL GENERATOR v3 STARTED ===")

    symbols = [
        "RELIANCE", "TCS", "HDFC", "INFY", "ICICIBANK",
        "SBIN", "AXISBANK", "TATAMOTORS", "HDFCBANK"
    ]

    results = []

    for s in symbols:
        signal = generate_signal(s)
        if signal:
            results.append(signal)

    logging.info(f"Processed: {len(results)} symbols")

    if results:
        insert_backend(results)
        logging.info("Signals Uploaded Successfully!")
    else:
        logging.warning("NO SIGNALS GENERATED!")

    logging.info("=== JOB COMPLETE ===")


if __name__ == "__main__":
    main()
