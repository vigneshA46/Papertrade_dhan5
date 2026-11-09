import time
import pytz
import requests
from datetime import datetime, timedelta, time as dtime
from dotenv import load_dotenv
import os
from dhanhq import DhanContext, dhanhq
from dhan_token import get_access_token
from candle_builder import FiveMinuteCandleBuilder
from find_security import load_fno_master, find_option_security
import threading
from signal_emitter import emit_signal
from dispatcher import subscribe
from queue import Queue
import asyncio
from find_instrument import FindInstrument


# =========================================================
# STRATEGY 7 — NIFTY SUPERTREND + EMA OPTION BUYING
# ---------------------------------------------------------
# Signal / tracking : NIFTY spot index (13), 5-min candles
# Indicators        : EMA 5, EMA 10, Supertrend (10, 1.5)  [TradingView formula]
# Marked line       : close of the 09:15-09:20 index candle
#
# CE entry (first CE of the day): close > marked line, EMA5 > EMA10, Supertrend BUY
# CE entry (later CE entries)   : EMA5 > EMA10, Supertrend BUY
# PE entry                      : mirror (close < marked first time, EMA5 < EMA10, ST SELL)
#
# Strike  : at entry, 200 points ITM (CE = ATM - 200, PE = ATM + 200)
# Lots    : 2 on the first entry of a side, +2 on every next entry of that side
# Exits   : trade pnl >= ₹3,000 -> book half, rest on Supertrend change
#           Supertrend change before ₹3,000 -> exit all
# Day     : +₹10,000 / -₹10,000 (realized + running, both sides, every tick)
#           -> exit all, no more trades. Square-off at 15:14.
# =========================================================


# =========================
# CONFIG
# =========================

load_dotenv()

STRATEGY_NAME = "NIFTY_SUPERTREND_EMA_BUYING"

# 🔥 PUT THE NEW STRATEGY UUID HERE (deployments / logs / telemetry use this)
COMMON_ID = "122884d2-8de3-44df-b5e5-06f214d04a99"
strategy_id = COMMON_ID

SYMBOL = "NIFTY"

TRADE_LOG_URL = "https://algoapi.dreamintraders.in/api/paperlogger/event"
EVENT_LOG_URL = "https://algoapi.dreamintraders.in/api/paperlogger/paperlogger"
TELEMETRY_URL = "https://algoapi.dreamintraders.in/api/telemetry"
TRADELEG_URL = "https://algoapi.dreamintraders.in/api/tradelegs/create"
DEPLOYMENTS_URL = f"https://algoapi.dreamintraders.in/api/deployments/today/{strategy_id}"

IST = pytz.timezone("Asia/Kolkata")

MARK_TIME    = dtime(9, 20)     # 09:15 candle is complete -> mark its close
TRADE_END    = dtime(15, 14)    # square-off, no entries after this

INDEX_TOKEN  = "13"
CANDLE_MIN   = 5

EMA_FAST     = 5
EMA_SLOW     = 10
ST_PERIOD    = 10
ST_MULT      = 1.5
HISTORY_DAYS = 7                # calendar days of 5-min history for indicator warm-up

ITM_POINTS   = 200
STRIKE_STEP  = 50
LTP_BAND     = 20               # strikes each side of 09:20 ATM kept in the LTP cache

BASE_LOT     = 2
LOT_STEP     = 2
LOTSIZE      = 65

HALF_BOOK_RS = 3000             # trade pnl -> book half
DAY_TARGET   = 10000
DAY_STOPLOSS = -10000

PENDING_FILL_SECONDS = 20       # wait this long for the first tick of a new strike
USERS_REFRESH_SECONDS = 30

today = datetime.now(IST).strftime("%Y-%m-%d")


# =========================
# LOGIN
# =========================

client_id = os.getenv("CLIENT_ID")
access_token = get_access_token()

dhan_context = DhanContext(client_id, access_token)
dhan = dhanhq(dhan_context)
fno_df = load_fno_master()
finder = FindInstrument()

state_lock = threading.RLock()


# =========================
# ASYNC BRIDGE (signals)
# =========================

loop = asyncio.new_event_loop()

def start_loop():
    asyncio.set_event_loop(loop)
    loop.run_forever()

threading.Thread(target=start_loop, daemon=True).start()

def _signal_done(fut):
    try:
        exc = fut.exception()
        if exc:
            print("❌ SIGNAL EMIT FAILED:", exc)
    except Exception as e:
        print("❌ SIGNAL FUTURE ERROR:", e)

def run_async(coro):
    try:
        if asyncio.iscoroutine(coro):
            f = asyncio.run_coroutine_threadsafe(coro, loop)
            f.add_done_callback(_signal_done)
        else:
            print("❌ Not coroutine:", coro)
    except Exception as e:
        print("WS error: ", e)


# =========================
# HTTP LOG QUEUE (non-blocking)
# =========================

http_queue = Queue()

def http_worker():
    while True:
        url, payload = http_queue.get()
        try:
            requests.post(url, json=payload, timeout=3)
        except Exception as e:
            print("LOG POST ERROR:", url, e)
        finally:
            http_queue.task_done()

threading.Thread(target=http_worker, daemon=True).start()


# =========================
# DEPLOYMENTS / USERS (cached)
# =========================

users_cache = {"users": {}}

def get_today_deployments():
    try:
        response = requests.get(DEPLOYMENTS_URL, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print("API Error:", e)
        return None

def group_users_by_broker(deployments):
    grouped = {}
    if not deployments:
        return grouped
    for d in deployments:
        if d.get("type") == "paper":
            continue
        broker = d.get("broker_name")
        if not broker:
            continue
        grouped.setdefault(broker, []).append(d)
    return grouped

def refresh_users():
    deployments = get_today_deployments()
    if deployments is not None:
        users_cache["users"] = group_users_by_broker(deployments)

def users_refresher():
    while True:
        time.sleep(USERS_REFRESH_SECONDS)
        refresh_users()

def get_users():
    return users_cache["users"]


# =========================
# LOGGING HELPERS
# =========================

def logtradeleg(strategyid, leg, symbol, strike_price, date, token):
    payload = {
        "strategy_id": strategyid,
        "leg": leg,
        "symbol": symbol,
        "strike_price": strike_price,
        "date": date,
        "token": str(token)
    }
    try:
        response = requests.post(TRADELEG_URL, json=payload, timeout=5)
        if response.status_code in (200, 201):
            print("✅ Trade leg logged successfully")
            return response.json()
        print(f"❌ Failed to log trade leg: {response.status_code}")
        print(response.text)
    except Exception as e:
        print(f"⚠️ Error while calling API: {e}")
    return None

def log_event(leg_name, token, action, price, remark=""):
    payload = {
        "run_id": COMMON_ID,
        "strategy_id": COMMON_ID,
        "leg_name": leg_name,
        "token": int(token),
        "symbol": SYMBOL,
        "action": action,
        "price": price,
        "log_type": "TRADE_EVENT",
        "remark": remark
    }
    http_queue.put((EVENT_LOG_URL, payload))

def log_trade_event(event_type, leg_name, token, symbol, side, lot, price, reason, pnl, cum_pnl):
    payload = {
        "run_id": COMMON_ID,
        "strategy_id": COMMON_ID,
        "trade_id": COMMON_ID,
        "event_type": event_type,
        "leg_name": leg_name,
        "token": int(token),
        "symbol": symbol,
        "side": side,
        "lots": lot,
        "quantity": lot * LOTSIZE,
        "price": price,
        "reason": reason,
        "deployed_by": COMMON_ID,
        "pnl": str(pnl),
        "cum_pnl": str(cum_pnl)
    }
    http_queue.put((TRADE_LOG_URL, payload))


# =========================
# SIGNAL PAYLOAD
# =========================

def build_payload(leg, side, reason, event_type, price, lots, pnl, cum_pnl):
    name = leg["name"]
    strike = int(float(leg["strike"]))
    dhan_row = leg["dhan_row"]
    angel_row = leg["angel_row"]

    expiry_date = dhan_row["SM_EXPIRY_DATE"]
    day = expiry_date.strftime("%d")
    month = expiry_date.strftime("%b").upper()
    year = expiry_date.strftime("%y")

    symbol = f"NIFTY{day}{month}{year}{strike}{name}"
    expiry = expiry_date.strftime("%Y-%m-%d")

    print(f"📦 PAYLOAD {symbol} | {side} | {event_type} | {reason} | price {price} | lots {lots}")

    return {
        "strategy_id": COMMON_ID,
        "users": get_users(),
        "option": name,
        "side": side,
        "quantity": lots * LOTSIZE,
        "security_id": leg["token"],
        "token": int(angel_row["token"]),
        "event_type": event_type,
        "leg_name": name,
        "symbol": str(symbol),
        "exchange": "NFO",
        "expiry": expiry,
        "strike": str(strike),
        "price": price,
        "pnl": pnl,
        "cum_pnl": cum_pnl,
        "zebusymbol": "NIFTY",
        "is_ce": name == "CE",
        "is_fno": True,
        "antsymbol": "NIFTY",
        "reason": reason
    }


# =========================
# TELEMETRY
# =========================

telemetry = {
    "strategy_id": COMMON_ID,
    "run_id": COMMON_ID,
    "status": "ACTIVE",
    "pnl": 0.0,
    "pnl_percentage": 0.0,
    "ce_ltp": 0.0,
    "pe_ltp": 0.0,
    "ce_pnl": 0.0,
    "pe_pnl": 0.0
}

NUMERIC_KEYS = ["pnl", "ce_pnl", "pe_pnl", "ce_ltp", "pe_ltp", "pnl_percentage"]

def safe_number(x):
    try:
        return float(x)
    except Exception:
        return 0.0

def telemetry_broadcaster():
    while True:
        try:
            payload = telemetry.copy()
            payload = {k: safe_number(v) if k in NUMERIC_KEYS else v for k, v in payload.items()}
            res = requests.post(TELEMETRY_URL, json=payload, timeout=0.5)
            if res.status_code != 200:
                print("Telemetry failed:", res.status_code)
        except Exception as e:
            print("Telemetry error:", e)
        time.sleep(1)

threading.Thread(target=telemetry_broadcaster, daemon=True).start()


# =========================
# INDICATORS
# =========================

def ema_series(values, period):
    """EMA list aligned with values (None until enough data). Seeded with SMA."""
    out = [None] * len(values)
    if len(values) < period:
        return out
    k = 2 / (period + 1)
    ema = sum(values[:period]) / period
    out[period - 1] = ema
    for i in range(period, len(values)):
        ema = (values[i] - ema) * k + ema
        out[i] = ema
    return out

def atr_rma(candles, period):
    """Wilder ATR (RMA of True Range) — same as TradingView ta.atr()."""
    trs = []
    for i, c in enumerate(candles):
        if i == 0:
            tr = c["high"] - c["low"]
        else:
            pc = candles[i - 1]["close"]
            tr = max(c["high"] - c["low"], abs(c["high"] - pc), abs(c["low"] - pc))
        trs.append(tr)

    out = [None] * len(candles)
    if len(trs) < period:
        return out
    atr = sum(trs[:period]) / period
    out[period - 1] = atr
    for i in range(period, len(trs)):
        atr = (atr * (period - 1) + trs[i]) / period
        out[i] = atr
    return out

def supertrend_series(candles, period=ST_PERIOD, mult=ST_MULT):
    """
    TradingView ta.supertrend logic.
    Returns list of (supertrend_value, trend) with trend +1 = BUY (line below price),
    -1 = SELL (line above price). None until ATR is available.
    """
    atr = atr_rma(candles, period)
    out = [None] * len(candles)

    lower_prev = upper_prev = None
    trend = None

    for i, c in enumerate(candles):
        if atr[i] is None:
            continue

        hl2 = (c["high"] + c["low"]) / 2
        upper = hl2 + mult * atr[i]
        lower = hl2 - mult * atr[i]

        if lower_prev is not None:
            prev_close = candles[i - 1]["close"]
            # bands only tighten in the trend's favour unless the previous close broke them
            if not (lower > lower_prev or prev_close < lower_prev):
                lower = lower_prev
            if not (upper < upper_prev or prev_close > upper_prev):
                upper = upper_prev

        if trend is None:
            trend = -1                      # TradingView seeds the first bar as down-trend
        elif trend == -1:
            trend = 1 if c["close"] > upper else -1
        else:
            trend = -1 if c["close"] < lower else 1

        out[i] = (lower if trend == 1 else upper, trend)
        lower_prev, upper_prev = lower, upper

    return out

def compute_indicators():
    """Recompute EMA5 / EMA10 / Supertrend on all stored candles (a few hundred — cheap)."""
    candles = index["candles"]
    closes = [c["close"] for c in candles]
    e5 = ema_series(closes, EMA_FAST)
    e10 = ema_series(closes, EMA_SLOW)
    st = supertrend_series(candles)

    index["ema5"] = e5[-1] if e5 else None
    index["ema10"] = e10[-1] if e10 else None
    if st and st[-1] is not None:
        index["st_value"], index["trend"] = st[-1]
    else:
        index["st_value"], index["trend"] = None, None


# =========================
# INDEX HISTORY
# =========================

def floor_bucket(dt):
    minute_of_day = dt.hour * 60 + dt.minute
    m = (minute_of_day // CANDLE_MIN) * CANDLE_MIN
    return dt.replace(hour=m // 60, minute=m % 60, second=0, microsecond=0)

def fetch_index_history():
    """Completed NIFTY 5-min candles for the last HISTORY_DAYS calendar days."""
    now = datetime.now(IST)
    from_d = (now - timedelta(days=HISTORY_DAYS)).strftime("%Y-%m-%d")
    to_d = now.strftime("%Y-%m-%d")

    try:
        res = dhan.intraday_minute_data(
            security_id=INDEX_TOKEN,
            exchange_segment="IDX_I",
            instrument_type="INDEX",
            from_date=from_d,
            to_date=to_d,
            interval=CANDLE_MIN
        )
    except Exception as e:
        print("❌ Index history error:", e)
        return None

    data = res.get("data") if isinstance(res, dict) else None
    if not isinstance(data, dict) or not data.get("timestamp"):
        print(f"❌ Index history failed: status={res.get('status') if isinstance(res, dict) else None} "
              f"remarks={res.get('remarks') if isinstance(res, dict) else res}")
        return None

    candles = []
    for i, ts in enumerate(data["timestamp"]):
        start = datetime.fromtimestamp(ts, IST)
        if start + timedelta(minutes=CANDLE_MIN) > now:
            continue                     # still forming
        candles.append({
            "start": start,
            "open": float(data["open"][i]),
            "high": float(data["high"][i]),
            "low": float(data["low"][i]),
            "close": float(data["close"][i]),
        })
    candles.sort(key=lambda c: c["start"])
    return candles


# =========================
# STATE
# =========================

index = {
    "builder": FiveMinuteCandleBuilder(),
    "ltp": 0.0,
    "candles": [],
    "ema5": None,
    "ema10": None,
    "st_value": None,
    "trend": None,
    "marked": None,
    "first_tick": None,
}

def init_leg(name):
    return {
        "name": name,
        "token": None,
        "strike": None,
        "dhan_row": None,
        "angel_row": None,
        "position": False,
        "entry_price": None,
        "entry_time": None,
        "open_lots": 0,
        "half_booked": False,
        "next_lots": BASE_LOT,     # 2, 4, 6 ...
        "entries": 0,
        "pnl": 0.0,                # realized for the day (incl. half bookings)
        "ltp": 0.0,
        "pending": None,           # waiting for first tick of a strike not yet in cache
    }

legs = {"CE": init_leg("CE"), "PE": init_leg("PE")}
ltp_cache = {}            # option security_id -> ltp
option_tokens = set()     # option tokens this strategy listens to
ready = False
day_closed = False


# =========================
# PNL / TELEMETRY
# =========================

def running_pnl(leg):
    if not leg["position"]:
        return 0.0
    return (leg["ltp"] - leg["entry_price"]) * LOTSIZE * leg["open_lots"]

def realized_total():
    return legs["CE"]["pnl"] + legs["PE"]["pnl"]

def total_mtm():
    return realized_total() + running_pnl(legs["CE"]) + running_pnl(legs["PE"])

def update_telemetry():
    ce, pe = legs["CE"], legs["PE"]
    telemetry["ce_ltp"] = float(ce["ltp"] or 0)
    telemetry["pe_ltp"] = float(pe["ltp"] or 0)
    telemetry["ce_pnl"] = ce["pnl"] + running_pnl(ce)
    telemetry["pe_pnl"] = pe["pnl"] + running_pnl(pe)
    telemetry["pnl"] = telemetry["ce_pnl"] + telemetry["pe_pnl"]

    if day_closed:
        telemetry["status"] = "CLOSED"
    elif ce["position"] or pe["position"]:
        telemetry["status"] = "RUNNING"
    else:
        telemetry["status"] = "ACTIVE"


# =========================
# STRIKES
# =========================

def itm_strike(side, spot):
    atm = int(round(spot / STRIKE_STEP) * STRIKE_STEP)
    return atm - ITM_POINTS if side == "CE" else atm + ITM_POINTS

def resolve_rows(side, strike):
    dhan_row = find_option_security(fno_df, strike, side, today, "NIFTY")
    angel_row = finder.get_option("NIFTY", int(strike), side)

    try:
        d_exp = dhan_row["SM_EXPIRY_DATE"].strftime("%Y-%m-%d")
        a_exp = str(angel_row["expiry"])[:10]
        if d_exp != a_exp:
            print(f"🚨 ANGEL EXPIRY MISMATCH {side} {int(strike)} -> Dhan {d_exp} | Angel {a_exp} "
                  f"(token {angel_row['token']}) — Angel users would get the WRONG contract")
    except Exception as e:
        print("Expiry check error:", e)

    return dhan_row, angel_row

def watch_option(token):
    token = str(token)
    if token not in option_tokens:
        option_tokens.add(token)
        subscribe(token, on_tick)


# =========================
# ORDER EVENTS
# =========================

def open_trade(leg, price, reason):
    lots = leg["next_lots"]
    leg["position"] = True
    leg["entry_price"] = price
    leg["entry_time"] = datetime.now(IST).isoformat()
    leg["open_lots"] = lots
    leg["half_booked"] = False
    leg["ltp"] = price
    leg["entries"] += 1
    leg["next_lots"] += LOT_STEP

    print(f"🟢 BUY {leg['name']} {int(leg['strike'])} @ {price} | lots {lots} | {reason} | "
          f"next {leg['name']} entry -> {leg['next_lots']} lots")

    run_async(emit_signal(build_payload(leg, "BUY", "entry", "ENTRY", price, lots,
                                        leg["pnl"], realized_total())))
    log_trade_event("ENTRY", leg["name"], leg["token"], SYMBOL, "BUY",
                    lots, price, reason, leg["pnl"], realized_total())
    log_event(f"{leg['name']} BUY", leg["token"], "ENTRY_EXECUTED", price, reason)

def close_lots(leg, price, lots, reason):
    """Exit `lots` of the open trade (half booking or full exit)."""
    trade_pnl = (price - leg["entry_price"]) * LOTSIZE * lots
    leg["pnl"] += trade_pnl
    leg["open_lots"] -= lots

    print(f"🔴 EXIT {lots} lot(s) {leg['name']} {int(leg['strike'])} @ {price} | {reason} | "
          f"pnl {trade_pnl:.2f} | side {leg['pnl']:.2f} | total {realized_total():.2f} | "
          f"open lots {leg['open_lots']}")

    run_async(emit_signal(build_payload(leg, "SELL", "exit", "EXIT", price, lots,
                                        trade_pnl, realized_total())))
    log_trade_event("EXIT", leg["name"], leg["token"], SYMBOL, "SELL",
                    lots, price, reason, leg["pnl"], realized_total())
    log_event(f"{leg['name']} SELL", leg["token"], "EXIT_EXECUTED", price, reason)

    if leg["open_lots"] <= 0:
        leg["position"] = False
        leg["entry_price"] = None
        leg["entry_time"] = None
        leg["open_lots"] = 0
        leg["half_booked"] = False

def square_off_all(reason):
    global day_closed
    if day_closed:
        return
    for leg in legs.values():
        leg["pending"] = None
        if leg["position"]:
            close_lots(leg, leg["ltp"], leg["open_lots"], reason)
    day_closed = True
    telemetry["status"] = "CLOSED"
    print(f"🛑 DAY CLOSED -> {reason} | day pnl {realized_total():.2f}")

def check_time_exit():
    if datetime.now(IST).time() >= TRADE_END:
        square_off_all("TIME EXIT 15:14")
        return True
    return False

def check_day_limits():
    if day_closed:
        return
    total = total_mtm()
    if total >= DAY_TARGET:
        print(f"🏁 DAY TARGET HIT ({total:.2f}) — EXIT ALL")
        square_off_all("DAY TARGET +10000")
    elif total <= DAY_STOPLOSS:
        print(f"🚨 DAY STOPLOSS HIT ({total:.2f}) — EXIT ALL")
        square_off_all("DAY STOPLOSS -10000")

def check_half_booking(leg):
    if not leg["position"] or leg["half_booked"] or leg["open_lots"] < 2:
        return
    trade_pnl = running_pnl(leg)
    if trade_pnl >= HALF_BOOK_RS:
        half = leg["open_lots"] // 2
        leg["half_booked"] = True
        close_lots(leg, leg["ltp"], half,
                   f"Trade pnl {trade_pnl:.2f} >= {HALF_BOOK_RS} -> book half")


# =========================
# ENTRY (with strike resolution)
# =========================

def request_entry(leg, reason):
    """Pick the 200-ITM strike now and buy at its latest LTP (or its first tick)."""
    spot = index["ltp"] or index["candles"][-1]["close"]
    strike = itm_strike(leg["name"], spot)

    try:
        dhan_row, angel_row = resolve_rows(leg["name"], strike)
    except Exception as e:
        print(f"❌ {leg['name']} {strike} contract lookup failed — entry skipped:", e)
        return

    token = str(dhan_row["SECURITY_ID"])
    leg["token"] = token
    leg["strike"] = strike
    leg["dhan_row"] = dhan_row
    leg["angel_row"] = angel_row

    watch_option(token)
    logtradeleg_async(leg["name"], strike, token)

    ltp = ltp_cache.get(token)
    if ltp:
        open_trade(leg, ltp, f"{reason} | spot {spot} | {ITM_POINTS} ITM {strike}")
    else:
        leg["pending"] = {"reason": f"{reason} | spot {spot} | {ITM_POINTS} ITM {strike}",
                          "since": time.time()}
        print(f"⏳ {leg['name']} {strike} [{token}] no LTP yet — buying on its first tick")

def logtradeleg_async(side, strike, token):
    threading.Thread(
        target=logtradeleg,
        args=(COMMON_ID, side, f"NIFTY {side} {strike}", str(strike), str(today), token),
        daemon=True
    ).start()


# =========================
# INDEX ENGINE (5-min candle close)
# =========================

def on_index_candle():
    compute_indicators()
    c = index["candles"][-1]
    e5, e10, trend = index["ema5"], index["ema10"], index["trend"]

    trend_txt = "BUY" if trend == 1 else "SELL" if trend == -1 else None
    print(f"🕯 NIFTY 5m {c['start'].strftime('%H:%M')} O {c['open']} H {c['high']} L {c['low']} "
          f"C {c['close']} | EMA5 {e5 and round(e5, 2)} | EMA10 {e10 and round(e10, 2)} | "
          f"ST {index['st_value'] and round(index['st_value'], 2)} {trend_txt} | marked {index['marked']}")

    if day_closed or None in (e5, e10, trend):
        return

    ce, pe = legs["CE"], legs["PE"]

    # ---- EXITS: Supertrend change ----
    if ce["position"] and trend == -1:
        close_lots(ce, ce["ltp"], ce["open_lots"], "Supertrend changed to SELL")
    if pe["position"] and trend == 1:
        close_lots(pe, pe["ltp"], pe["open_lots"], "Supertrend changed to BUY")

    if datetime.now(IST).time() >= TRADE_END:
        return

    close = c["close"]
    marked = index["marked"]

    # ---- CE ENTRY ----
    if not ce["position"] and ce["pending"] is None:
        first = ce["entries"] == 0
        ok = e5 > e10 and trend == 1 and (not first or close > marked)
        if ok:
            why = (f"close {close} > marked {marked}, " if first else "") + \
                  f"EMA5 {e5:.2f} > EMA10 {e10:.2f}, ST BUY"
            request_entry(ce, why)

    # ---- PE ENTRY ----
    if not pe["position"] and pe["pending"] is None:
        first = pe["entries"] == 0
        ok = e5 < e10 and trend == -1 and (not first or close < marked)
        if ok:
            why = (f"close {close} < marked {marked}, " if first else "") + \
                  f"EMA5 {e5:.2f} < EMA10 {e10:.2f}, ST SELL"
            request_entry(pe, why)

def completed_bucket_start(now=None):
    """The live builder emits a candle on the first tick of the next bucket."""
    now = now or datetime.now(IST)
    return floor_bucket(now) - timedelta(minutes=CANDLE_MIN)

def on_index_tick(msg, ltp):
    now = datetime.now(IST)
    if index["first_tick"] is None:
        index["first_tick"] = now

    index["ltp"] = ltp
    candle = index["builder"].process_tick(msg)

    if check_time_exit():
        return

    if not candle or not ready:
        return

    start = completed_bucket_start(now)
    last = index["candles"][-1]["start"] if index["candles"] else None

    if last is not None and start <= last:
        return                                     # already have it from history

    if start < index["first_tick"] - timedelta(seconds=1):
        # feed started inside this candle -> live candle is partial; take it from history
        print("⚠️ Live 5m candle is partial (feed started mid-candle) — reloading history")
        hist = fetch_index_history()
        if hist:
            index["candles"] = hist
            on_index_candle()
        return

    index["candles"].append({
        "start": start,
        "open": float(candle["open"]),
        "high": float(candle["high"]),
        "low": float(candle["low"]),
        "close": float(candle["close"]),
    })
    index["candles"] = index["candles"][-2000:]
    on_index_candle()


# =========================
# OPTION ENGINE (ticks)
# =========================

def on_option_tick(token, ltp):
    ltp_cache[token] = ltp

    for leg in legs.values():
        if leg["token"] != token:
            continue

        leg["ltp"] = ltp

        # first tick of a freshly picked strike -> complete the entry
        if leg["pending"] is not None and not leg["position"] and not day_closed:
            p = leg["pending"]
            leg["pending"] = None
            open_trade(leg, ltp, p["reason"])

        if check_time_exit():
            return

        check_half_booking(leg)

def expire_pending():
    for leg in legs.values():
        p = leg["pending"]
        if p and time.time() - p["since"] > PENDING_FILL_SECONDS:
            leg["pending"] = None
            print(f"⚠️ {leg['name']} {leg['strike']} [{leg['token']}] got no tick in "
                  f"{PENDING_FILL_SECONDS}s — entry cancelled (token probably not in the feed)")


# =========================
# CALLBACKS
# =========================

def on_message(msg):
    if msg.get("type") != "Quote Data":
        return

    token = str(msg["security_id"])
    ltp = float(msg.get("LTP") or 0)
    if ltp <= 0:
        return

    with state_lock:
        if token == INDEX_TOKEN:
            on_index_tick(msg, ltp)
        elif token in option_tokens:
            on_option_tick(token, ltp)
        else:
            return

        expire_pending()
        update_telemetry()
        check_day_limits()


def on_tick(token, msg):
    t = str(token)
    if t != INDEX_TOKEN and t not in option_tokens:
        return
    try:
        on_message(msg)
    except Exception as e:
        print("SUPERTREND STRATEGY ERROR:", e)


# =========================
# SETUP (background — never blocks main.py)
# =========================

def setup():
    global ready

    try:
        print(f"\n🚀 {STRATEGY_NAME} STARTED\n")
        refresh_users()
        threading.Thread(target=users_refresher, daemon=True).start()

        # ---- wait for the 09:15 candle to complete ----
        while datetime.now(IST).time() < MARK_TIME:
            time.sleep(1)
        time.sleep(5)

        # ---- history + marked line (retry until today's 09:15 candle is there) ----
        candles, marked = None, None
        for attempt in range(30):
            candles = fetch_index_history()
            if candles:
                today_d = datetime.now(IST).date()
                c915 = [c for c in candles
                        if c["start"].date() == today_d and c["start"].strftime("%H:%M") == "09:15"]
                if c915:
                    marked = c915[0]["close"]
                    break
            print(f"⏳ waiting for today's 09:15 candle in history (attempt {attempt + 1})")
            time.sleep(4)

        if marked is None:
            print("❌ Could not get the 09:15 candle — strategy will not trade today")
            return

        # ---- option LTP cache: strikes around the current ATM ----
        spot = candles[-1]["close"]
        atm = int(round(spot / STRIKE_STEP) * STRIKE_STEP)
        cached = 0
        for i in range(-LTP_BAND, LTP_BAND + 1):
            strike = atm + i * STRIKE_STEP
            for side in ("CE", "PE"):
                try:
                    row = find_option_security(fno_df, strike, side, today, "NIFTY")
                    if row is not None:
                        watch_option(row["SECURITY_ID"])
                        cached += 1
                except Exception:
                    pass

        with state_lock:
            index["candles"] = candles
            index["marked"] = marked
            compute_indicators()
            ready = True

        print(f"📍 MARKED LINE (09:15 close): {marked}")
        print(f"📈 Warm-up: {len(candles)} candles | EMA5 {index['ema5']:.2f} | "
              f"EMA10 {index['ema10']:.2f} | ST {index['st_value']:.2f} "
              f"{'BUY' if index['trend'] == 1 else 'SELL'}")
        print(f"📡 Watching {cached} option tokens around ATM {atm} for entry prices")
        print("✅ Ready — signals on every 5-min NIFTY candle close")

    except Exception as e:
        import traceback
        print("❌ SUPERTREND STRATEGY setup FAILED:", e)
        traceback.print_exc()


# =====================
# START FEED
# =====================

subscribe(INDEX_TOKEN, on_tick)
threading.Thread(target=setup, daemon=True).start()