#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Finspark AI v6.4.2 FINAL - ENHANCED PRECISION
Enhanced signal generation with EMA angle/momentum filter, MACD divergence,
stricter volume confirmation, and optimized R:R (2.5:1)
"""
import os,sys,time,warnings,logging,re,random
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor,as_completed
warnings.filterwarnings('ignore')
import yfinance as yf
import pandas as pd
import numpy as np
from supabase import create_client

# Configuration
SUPABASE_URL=os.getenv('SUPABASE_URL','')
SUPABASE_KEY=os.getenv('SUPABASE_KEY','')
STOCK_LIST=os.getenv('STOCK_LIST','RELIANCE.NS,TCS.NS,INFY.NS,HDFCBANK.NS,ICICIBANK.NS')
INTERVAL='15m'
INTRADAY_PERIOD='5d'
MAX_WORKERS=int(os.getenv("MAX_WORKERS","10"))
BATCH_SIZE=20
RETRY_DELAY=0.5
MAX_RETRIES=3
MARKET_OPEN=(9,15)
MARKET_CLOSE=(15,30)
RSI_PERIOD=14
MACD_FAST=12
MACD_SLOW=26
MACD_SIGNAL=9
BB_PERIOD=20
VOL_PERIOD=10
SL_ATR_MULT=float(os.getenv("SL_ATR_MULT","1.2"))
TP_ATR_MULT=float(os.getenv("TP_ATR_MULT","3.0"))
MAX_CIRCUIT_PCT=float(os.getenv("MAX_CIRCUIT_PCT","0.18"))
EMA_TREND_BUFFER=float(os.getenv("EMA_TREND_BUFFER","0.002"))
EMA_ANGLE_THRESHOLD=float(os.getenv("EMA_ANGLE_THRESHOLD","0.003"))
MIN_CONFIDENCE=70
MIN_VOTES_FOR_ACTION=int(os.getenv("MIN_VOTES","5"))
MIN_STRENGTH_SCORE=int(os.getenv("MIN_STRENGTH","10"))
VOL_SPIKE_MULT=float(os.getenv("VOL_SPIKE_MULT","3.0"))
MAX_INTRADAY_GAPS=int(os.getenv("MAX_INTRADAY_GAPS","10"))
MIN_RR_TO_SHOW=float(os.getenv("MIN_RR","2.0"))

logging.basicConfig(level=logging.INFO,format="%(asctime)s %(levelname)s %(message)s")
logger=logging.getLogger("FinsparkV6.4.2")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ ERROR: Missing SUPABASE credentials")
    sys.exit(1)

TICKER_RE=re.compile(r'^[A-Z0-9][A-Z0-9._-]{0,18}(?:\.[A-Z]{1,5})?$')

def sanitize_tickers(raw):
    items=[]
    for part in raw.split(','):
        t=str(part).strip().strip(" '\"`; :()[]{}<>")
        if not t:continue
        tokens=t.split()
        if not tokens:continue
        t=tokens[0].upper()
        t=re.sub(r'[^A-Z0-9._-]','',t)
        if t and TICKER_RE.match(t):items.append(t)
    seen=set();out=[]
    for t in items:
        if t not in seen:seen.add(t);out.append(t)
    return out

STOCKS=sanitize_tickers(STOCK_LIST)

print("="*70)
print("🏆 FINSPARK AI v6.4.2 FINAL - ENHANCED PRECISION")
print("="*70)
print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}")
print(f"📊 Stocks:{len(STOCKS)}|Workers:{MAX_WORKERS}")
print(f"🎯 SL:{SL_ATR_MULT}x|TP:{TP_ATR_MULT}x|R:R:{TP_ATR_MULT/SL_ATR_MULT:.1f}:1")
print(f"🏅 MinConf:{MIN_CONFIDENCE}%|MinR:R:{MIN_RR_TO_SHOW}|MaxGaps:{MAX_INTRADAY_GAPS}")
print(f"🔥 EMA Angle|MACD Divergence|Strict Volume")
print("="*70)

def is_market_hours():
    now=datetime.now()
    if now.weekday()>4:return False,"Weekend"
    current_time=(now.hour,now.minute)
    is_open=(MARKET_OPEN<=current_time<MARKET_CLOSE)
    return is_open,"Open"if is_open else"Closed"

is_open,status=is_market_hours()
print(f"📍 Market:{status}\n")

try:
    supabase=create_client(SUPABASE_URL,SUPABASE_KEY)
    logger.info("✅ Supabase connected")
except Exception as e:
    logger.critical(f"❌ Supabase failed:{e}")
    sys.exit(1)

def get_value(series,idx):
    try:
        if series is None or len(series)==0:return None
        val=series.iloc[idx]
        if isinstance(val,pd.Series):val=val.iloc[0]if len(val)>0 else None
        if pd.isna(val):return None
        float_val=float(val)
        return float_val if float_val>0.01 else None
    except:return None

def validate_data_quality(data):
    if len(data)<50:return False,f"Insufficient({len(data)})"
    if len(data)>1:
        time_diff=data.index.to_series().diff()
        expected_diff=pd.Timedelta(minutes=15)
        gaps=(time_diff>expected_diff*2).sum()
        if gaps>MAX_INTRADAY_GAPS:return False,f"Gaps({gaps})"
    last_time=data.index[-1]
    if hasattr(last_time,'tz_localize'):
        last_time=last_time.tz_localize(None)if last_time.tz is None else last_time
    now=pd.Timestamp.now()
    staleness_mins=(now-last_time).total_seconds()/60
    if staleness_mins>30:return False,f"Stale({staleness_mins:.0f}m)"
    if 'Close'in data.columns:
        price_changes=data['Close'].pct_change().abs()
        extreme_moves=(price_changes>0.10).sum()
        if extreme_moves>5:return False,"Volatility"
    return True,"OK"

def calculate_indicators(df):
    close=pd.to_numeric(df['Close'],errors='coerce').astype(float)
    high=pd.to_numeric(df['High'],errors='coerce').astype(float)
    low=pd.to_numeric(df['Low'],errors='coerce').astype(float)
    delta=close.diff()
    gain=delta.where(delta>0,0).rolling(RSI_PERIOD).mean()
    loss=-delta.where(delta<0,0).rolling(RSI_PERIOD).mean()
    rs=gain/loss.replace(0,0.0001)
    rsi=100-(100/(1+rs))
    ema_fast=close.ewm(span=MACD_FAST,adjust=False).mean()
    ema_slow=close.ewm(span=MACD_SLOW,adjust=False).mean()
    macd=ema_fast-ema_slow
    macd_signal=macd.ewm(span=MACD_SIGNAL,adjust=False).mean()
    macd_hist=macd-macd_signal
    sma=close.rolling(BB_PERIOD).mean()
    std=close.rolling(BB_PERIOD).std()
    bb_upper=sma+(2*std);bb_lower=sma-(2*std)
    vol_avg=df['Volume'].rolling(VOL_PERIOD).mean()
    ema_20=close.ewm(span=20,adjust=False).mean()
    ema_50=close.ewm(span=50,adjust=False).mean()
    tr1=high-low;tr2=abs(high-close.shift());tr3=abs(low-close.shift())
    tr=pd.concat([tr1,tr2,tr3],axis=1).max(axis=1)
    atr=tr.rolling(14).mean()
    return{'rsi':rsi,'atr':atr,'macd_hist':macd_hist,'bb_upper':bb_upper,'bb_lower':bb_lower,'vol_avg':vol_avg,'ema_20':ema_20,'ema_50':ema_50,'close':close,'macd':macd,'macd_signal':macd_signal}

def fetch_data(ticker,max_retries=MAX_RETRIES):
    for attempt in range(1,max_retries+1):
        try:
            if attempt>1:time.sleep(RETRY_DELAY)
            stock=yf.Ticker(ticker)
            data=stock.history(period=INTRADAY_PERIOD,interval=INTERVAL,auto_adjust=True)
            if hasattr(data,"columns")and isinstance(data.columns,pd.MultiIndex):
                data.columns=data.columns.get_level_values(0)
            if data is None or data.empty:raise ValueError("Empty")
            data=data.sort_index()
            if len(data)<50:raise ValueError(f"{len(data)}")
            if 'Close'not in data.columns:raise ValueError("No Close")
            valid_closes=data['Close'].dropna()
            if len(valid_closes)<50:raise ValueError(f"{len(valid_closes)} valid")
            last_close=data['Close'].iloc[-1]
            if pd.isna(last_close)or last_close<=0:raise ValueError("Invalid")
            return data
        except Exception as e:
            if attempt==max_retries:logger.debug(f"{ticker} failed:{e}");return None
    return None

def determine_trend_with_momentum(ema_20_series,ema_50,close_price):
    try:
        ema_20_current=ema_20_series.iloc[-1]if len(ema_20_series)>0 else None
        ema_20_prev=ema_20_series.iloc[-5]if len(ema_20_series)>=5 else None
        if pd.isna(ema_20_current)or pd.isna(ema_20_prev)or pd.isna(ema_50):return'Sideways'
        ema_slope=(ema_20_current-ema_20_prev)/ema_20_prev if ema_20_prev!=0 else 0
        if ema_20_current>ema_50*(1+EMA_TREND_BUFFER)and close_price>ema_20_current and ema_slope>EMA_ANGLE_THRESHOLD:return'Uptrend'
        elif ema_20_current<ema_50*(1-EMA_TREND_BUFFER)and close_price<ema_20_current and ema_slope<-EMA_ANGLE_THRESHOLD:return'Downtrend'
        return'Sideways'
    except:return'Sideways'

def detect_macd_divergence(close_series,macd_hist_series):
    if len(close_series)<10 or len(macd_hist_series)<10:return None
    try:
        price_last=close_series.iloc[-1];price_prev=close_series.iloc[-5]
        macd_last=macd_hist_series.iloc[-1];macd_prev=macd_hist_series.iloc[-5]
        if pd.isna(price_last)or pd.isna(price_prev)or pd.isna(macd_last)or pd.isna(macd_prev):return None
        if price_last<price_prev and macd_last>macd_prev:return'Bullish'
        if price_last>price_prev and macd_last<macd_prev:return'Bearish'
    except:pass
    return None

def get_tier(confidence,risk_reward):
    if confidence>=85 and risk_reward and risk_reward>=2.5:return"S"
    if confidence>=78 and risk_reward and risk_reward>=2.0:return"A"
    if confidence>=70:return"B"
    return"C"

def generate_signal(stock_symbol,stock_num=0,total=0):
    pretty=stock_symbol.replace('.NS','').upper()
    prefix=f"[{stock_num}/{total}] "if total>0 else""
    try:
        data=fetch_data(stock_symbol)
        if data is None:logger.debug(f"{prefix}❌ {pretty}:Fetch failed");return None
        is_valid,quality_msg=validate_data_quality(data)
        if not is_valid:logger.debug(f"{prefix}⚠️ {pretty}:{quality_msg}");return None
        last_idx=-1
        close_price=get_value(data['Close'],last_idx)
        open_price=get_value(data['Open'],last_idx)
        high_price=get_value(data['High'],last_idx)
        low_price=get_value(data['Low'],last_idx)
        volume=get_value(data['Volume'],last_idx)
        if close_price is None or close_price<=0:logger.debug(f"{prefix}❌ {pretty}:Invalid price");return None
        if open_price:
            circuit_pct=abs(close_price-open_price)/open_price
            if circuit_pct>MAX_CIRCUIT_PCT:logger.debug(f"{prefix}⚠️ {pretty}:Circuit({circuit_pct:.1%})");return None
        indicators=calculate_indicators(data)
        rsi_val=get_value(indicators['rsi'],last_idx)or 50.0
        macd_hist=get_value(indicators['macd_hist'],last_idx)or 0.0
        bb_up_val=get_value(indicators['bb_upper'],last_idx)or close_price*1.02
        bb_low_val=get_value(indicators['bb_lower'],last_idx)or close_price*0.98
        vol_avg=get_value(indicators['vol_avg'],last_idx)
        ema_20=get_value(indicators['ema_20'],last_idx)or close_price
        ema_50=get_value(indicators['ema_50'],last_idx)or close_price
        atr_val=get_value(indicators['atr'],last_idx)
        if atr_val is None or atr_val<=0:atr_val=max(close_price*0.002,0.1)
        buy_votes=0;sell_votes=0;hold_votes=0;strength_score=0;reasons=[]
        trend=determine_trend_with_momentum(indicators['ema_20'],ema_50,close_price)
        if trend=='Uptrend':buy_votes+=1;strength_score+=4;reasons.append("Momentum↑")
        elif trend=='Downtrend':sell_votes+=1;strength_score+=4;reasons.append("Momentum↓")
        divergence=detect_macd_divergence(indicators['close'],indicators['macd_hist'])
        if divergence=='Bullish':buy_votes+=1;strength_score+=4;reasons.append("MACD_Div+")
        elif divergence=='Bearish':sell_votes+=1;strength_score+=4;reasons.append("MACD_Div-")
        if rsi_val<30:buy_votes+=1;strength_score+=3;reasons.append(f"RSI({rsi_val:.1f})")
        elif rsi_val>70:sell_votes+=1;strength_score+=3;reasons.append(f"RSI({rsi_val:.1f})")
        elif rsi_val<40 and trend=='Uptrend':buy_votes+=1;strength_score+=1
        elif rsi_val>60 and trend=='Downtrend':sell_votes+=1;strength_score+=1
        else:hold_votes+=1
        if macd_hist>0:buy_votes+=1;strength_score+=2;reasons.append("MACD+")
        elif macd_hist<0:sell_votes+=1;strength_score+=2;reasons.append("MACD-")
        else:hold_votes+=1
        bb_width=max(bb_up_val-bb_low_val,1e-8)
        bb_position=(close_price-bb_low_val)/bb_width
        if bb_position<0.15:buy_votes+=1;strength_score+=2;reasons.append("BB_Low")
        elif bb_position>0.85:sell_votes+=1;strength_score+=2;reasons.append("BB_High")
        else:hold_votes+=1
        if volume and vol_avg:
            vol_ratio=volume/vol_avg
            candle_range=high_price-low_price if(high_price and low_price)else 0
            if vol_ratio>=VOL_SPIKE_MULT and candle_range>0:
                close_position=(close_price-low_price)/candle_range if candle_range>0 else 0.5
                if close_price>open_price and close_position>=0.75:buy_votes+=1;strength_score+=4;reasons.append(f"Vol+({vol_ratio:.1f}x)")
                elif close_price<open_price and close_position<=0.25:sell_votes+=1;strength_score+=4;reasons.append(f"Vol-({vol_ratio:.1f}x)")
                else:hold_votes+=1
            elif vol_ratio<0.6:hold_votes+=1;strength_score-=2
            else:hold_votes+=1
        else:hold_votes+=1
        candle_body=close_price-open_price if open_price else 0
        candle_range=high_price-low_price if(high_price and low_price)else 0
        if candle_range>0:
            body_ratio=abs(candle_body)/candle_range
            if candle_body>0 and body_ratio>0.7:buy_votes+=1;strength_score+=2
            elif candle_body<0 and body_ratio>0.7:sell_votes+=1;strength_score+=2
            elif body_ratio<0.1:hold_votes+=1;strength_score-=1
            else:hold_votes+=1
        else:hold_votes+=1
        signal='Hold';base_conf=40
        if buy_votes>=MIN_VOTES_FOR_ACTION and trend!='Downtrend':signal='Buy';base_conf+=min(strength_score,55)+(buy_votes*6)
        elif sell_votes>=MIN_VOTES_FOR_ACTION and trend!='Uptrend':signal='Sell';base_conf+=min(strength_score,55)+(sell_votes*6)
        elif buy_votes==4 and trend=='Uptrend'and strength_score>=MIN_STRENGTH_SCORE:signal='Buy';base_conf+=min(strength_score,45)+(buy_votes*5)
        elif sell_votes==4 and trend=='Downtrend'and strength_score>=MIN_STRENGTH_SCORE:signal='Sell';base_conf+=min(strength_score,45)+(sell_votes*5)
        else:signal='Hold';hold_votes=max(1,hold_votes)
        confidence=min(base_conf,95.0)
        if signal!='Hold'and confidence<MIN_CONFIDENCE:signal='Hold';confidence=50
        sl=tp=rr=None
        if signal!='Hold'and atr_val>0:
            sl_mult=SL_ATR_MULT if trend in('Uptrend','Downtrend')else SL_ATR_MULT*1.2
            tp_mult=TP_ATR_MULT if trend in('Uptrend','Downtrend')else TP_ATR_MULT*0.9
            risk=atr_val*sl_mult;reward=atr_val*tp_mult
            if signal=='Buy':sl=round(close_price-risk,2);tp=round(close_price+reward,2)
            else:sl=round(close_price+risk,2);tp=round(close_price-reward,2)
            rr=round(reward/risk,2)if risk>0 else None
        now=datetime.now()
        result={'symbol':pretty,'interval':INTERVAL,'signal':signal,'confidence':round(float(confidence),1),'close_price':round(float(close_price),2),'stop_loss':sl,'target':tp,'risk_reward':rr,'signal_date':now.date().isoformat(),'signal_time':now.strftime('%H:%M:%S'),'rsi':round(float(rsi_val),2),'buy_votes':buy_votes,'sell_votes':sell_votes,'hold_votes':hold_votes,'macd':round(float(macd_hist),4)}
        tier=get_tier(confidence,rr)
        emoji="🟢"if signal=='Buy'else"🔴"if signal=='Sell'else"⚪"
        tier_emoji="🏆"if tier=="S"else"🥇"if tier=="A"else"🥈"if tier=="B"else""
        sl_info=f"SL:₹{sl} T:₹{tp}"if(sl and tp)else""
        rr_info=f"R:R{rr}"if rr else""
        trend_emoji="📈"if trend=='Uptrend'else"📉"if trend=='Downtrend'else"➡️"
        reason_str=", ".join(reasons[:2])if reasons else""
        logger.info(f"{prefix}{emoji} {pretty:12s} {signal:4s}({confidence:.0f}%){tier_emoji}{tier} @₹{close_price:.2f} {sl_info} {rr_info} {trend_emoji}[{reason_str}]")
        return result
    except Exception as e:
        logger.error(f"{prefix}❌ {pretty}:{e}")
        return None

def upload_batch(batch_data):
    try:
        valid_data=[d for d in batch_data if d and d.get('close_price',0)>0]
        if not valid_data:return 0
        resp=supabase.table('signals').insert(valid_data).execute()
        if isinstance(resp,dict)and resp.get('error'):
            logger.error(f"Batch error:{resp.get('error')}")
            return 0
        if hasattr(resp,'data')and resp.data:return len(resp.data)
        return len(valid_data)
    except Exception as e:
        logger.error(f"Batch upload failed:{e}")
        return 0

def main():
    logger.info("🚀 Starting v6.4.2 FINAL...")
    try:
        today=datetime.now().date().isoformat()
        logger.info(f"⚠️ Deleting today's signals({today})...")
        delete_resp=supabase.table('signals').delete().eq('signal_date',today).execute()
        deleted_count=len(delete_resp.data)if hasattr(delete_resp,'data')and delete_resp.data else 0
        logger.info(f"✅ Deleted {deleted_count}")
    except Exception as e:
        logger.warning(f"⚠️ Delete failed:{e}")
    start_time=time.time()
    results=[];failed_tickers=[]
    with ThreadPoolExecutor(max_workers=MAX_WORKERS)as executor:
        future_to_stock={executor.submit(generate_signal,stock,i+1,len(STOCKS)):stock for i,stock in enumerate(STOCKS)}
        for future in as_completed(future_to_stock):
            stock=future_to_stock[future]
            try:
                signal=future.result()
                if signal:results.append(signal)
                else:failed_tickers.append(stock)
            except Exception as e:
                logger.error(f"{stock}:{e}")
                failed_tickers.append(stock)
    print(f"\n🚀 Uploading {len(results)} signals...")
    uploaded=0
    for i in range(0,len(results),BATCH_SIZE):
        batch=results[i:i+BATCH_SIZE]
        count=upload_batch(batch)
        uploaded+=count
        print(f" ✅ Batch {i//BATCH_SIZE+1}: {count}/{len(batch)} uploaded")
    elapsed=time.time()-start_time
    success=len(results);failed=len(failed_tickers)
    print("\n"+"="*70)
    print("📊 FINSPARK v6.4.2 FINAL SUMMARY")
    print("="*70)
    print(f"✅ Processed:{success}|❌ Failed:{failed}")
    print(f"⚡ Time:{elapsed:.1f}s({elapsed/len(STOCKS):.2f}s/stock)")
    print(f"⚡ Uploaded:{uploaded}/{success}({100*uploaded/max(success,1):.1f}%)")
    if results:
        df=pd.DataFrame(results)
        buy_signals=df[df['signal']=='Buy']
        sell_signals=df[df['signal']=='Sell']
        hold_signals=df[df['signal']=='Hold']
        hq_buys=buy_signals[(buy_signals['confidence']>70)&(buy_signals['risk_reward']>=MIN_RR_TO_SHOW)]
        hq_sells=sell_signals[(sell_signals['confidence']>70)&(sell_signals['risk_reward']>=MIN_RR_TO_SHOW)]
        s_tier=df[(df['confidence']>=85)&(df['risk_reward']>=2.5)]
        a_tier=df[(df['confidence']>=78)&(df['risk_reward']>=2.0)&(df['confidence']<85)]
        print()
        print(f"🟢 Buy:{len(buy_signals)}(HQ:{len(hq_buys)},R:R≥{MIN_RR_TO_SHOW})")
        print(f"🔴 Sell:{len(sell_signals)}(HQ:{len(hq_sells)},R:R≥{MIN_RR_TO_SHOW})")
        print(f"⚪ Hold:{len(hold_signals)}")
        print(f"📊 Avg confidence:{df['confidence'].mean():.1f}%")
        print(f"🏆 S-Tier:{len(s_tier)}|🥇 A-Tier:{len(a_tier)}")
        if len(s_tier)>0:
            print("\n🏆 S-TIER ELITE(Conf≥85%,R:R≥2.5):")
            for _,row in s_tier.nlargest(5,'confidence').iterrows():
                emoji="🟢"if row['signal']=='Buy'else"🔴"
                print(f" {emoji} {row['symbol']:12s}{row['confidence']:.0f}% ₹{row['close_price']:.2f}→T:₹{row['target']} R:R{row['risk_reward']}")
        if len(a_tier)>0:
            print("\n🥇 A-TIER(Conf≥78%,R:R≥2.0):")
            for _,row in a_tier.nlargest(3,'confidence').iterrows():
                emoji="🟢"if row['signal']=='Buy'else"🔴"
                print(f" {emoji} {row['symbol']:12s}{row['confidence']:.0f}% ₹{row['close_price']:.2f}→T:₹{row['target']} R:R{row['risk_reward']}")
        elif len(hq_buys)>0:
            print("\n🟢 HIGH-QUALITY BUYS(Conf≥70%,R:R≥2.0):")
            for _,row in hq_buys.nlargest(3,'confidence').iterrows():
                print(f" 🟢 {row['symbol']:12s}{row['confidence']:.0f}% ₹{row['close_price']:.2f}→T:₹{row['target']} R:R{row['risk_reward']}")
        elif len(hq_sells)>0:
            print("\n🔴 HIGH-QUALITY SELLS(Conf≥70%,R:R≥2.0):")
            for _,row in hq_sells.nlargest(3,'confidence').iterrows():
                print(f" 🔴 {row['symbol']:12s}{row['confidence']:.0f}% ₹{row['close_price']:.2f}→T:₹{row['target']} R:R{row['risk_reward']}")
    else:
        print("\n⚠️ No signals generated")
    if failed_tickers:
        print(f"\n❌ Failed tickers: {len(failed_tickers)}")
        if len(failed_tickers)<=10:
            print(f"   {', '.join([t.replace('.NS','') for t in failed_tickers])}")
    print("="*70)
    print(f"✅ Finspark v6.4.2 FINAL completed successfully")
    print(f"⏱️  Total runtime: {elapsed:.1f}s")
    print("="*70)
    sys.exit(0 if success>0 else 1)

if __name__=="__main__":
    main()
