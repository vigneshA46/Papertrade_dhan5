import time
import pytz
import requests
import pandas as pd
from datetime import datetime, timedelta, time as dtime
from dotenv import load_dotenv
import os
from dhanhq import DhanContext, dhanhq
from dhan_token import get_access_token
from find_security import load_fno_master, find_option_security
import threading
from signal_emitter import emit_signal
from dispatcher import subscribe
from queue import Queue
import asyncio
from find_instrument import FindInstrument
import option_chain_manager
from vwap_engine import VWAPManager

try:
    from candle_builder import FiveMinuteCandleBuilder
except ImportError:
    from vwap_engine import FiveMinuteCandleBuilder


# =========================================================
# STRATEGY 3 — NIFTY VWAP + PIVOT OPTION BUYING (low budget)
# ---------------------------------------------------------
# Signal  : current-month NIFTY FUTURES, 5-min candles
# Execute : options, 1 lot (65 qty) always, no lot increase
# Lines   : P (traditional pivot from futures prev-day H/L/C) + futures VWAP
#
#   CE BUY  : 5m close > P  and close > VWAP
#   CE EXIT : 5m close < VWAP or close < P
#   PE BUY  : 5m close < P  and close < VWAP
#   PE EXIT : 5m close > VWAP or close > P
#   Re-entry allowed whenever the condition is true again on a candle close.
#   Reversal on the same candle allowed (CE exit + PE entry together).
#
# Strikes : 09:20 option chain -> CE & PE with LTP nearest 200, fixed all day
# Day MTM : +3000 or -3000 (realized + running, both legs) on any tick
#           -> exit all, no more trades today. Time exit 15:14.
# =========================================================


# =========================
# CONFIG
# =========================

load_dotenv()

STRATEGY_NAME = "NIFTY_VWAP_PIVOT_BUYING"

# 🔥 PUT THE NEW STRATEGY UUID HERE (deployments / logs / telemetry use this)
COMMON_ID = "4737f626-a73f-480c-ba20-462e53a9e7d8"
strategy_id = COMMON_ID

SYMBOL = "NIFTY"

TRADE_LOG_URL = "https://algoapi.dreamintraders.in/api/paperlogger/event"
EVENT_LOG_URL = "https://algoapi.dreamintraders.in/api/paperlogger/paperlogger"
TELEMETRY_URL = "https://algoapi.dreamintraders.in/api/telemetry"
TRADELEG_URL = "https://algoapi.dreamintraders.in/api/tradelegs/create"
DEPLOYMENTS_URL = f"https://algoapi.dreamintraders.in/api/deployments/today/{strategy_id}"

IST = pytz.timezone("Asia/Kolkata")

STRIKE_TIME = dtime(9, 20)   # option chain call + strike selection
TRADE_END   = dtime(15, 14)  # time exit, no entries after this

NIFTY_SECURITY_ID = 13
TARGET_LTP        = 200      # strike selection: premium nearest to this

LOT              = 1         # fixed, never increased
LOTSIZE          = 65
MTM_TARGET       = 3000      # +₹3000 -> exit all, stop for the day
MTM_STOPLOSS     = -3000     # -₹3000 -> exit all, stop for the day

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
vwap_manager = VWAPManager()

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

def build_payload(leg, side, reason, event_type, price, pnl, cum_pnl):
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

    print(f"📦 PAYLOAD {symbol} | {side} | {event_type} | {reason} | price {price} | lot {LOT}")

    return {
        "strategy_id": COMMON_ID,
        "users": get_users(),
        "option": name,
        "side": side,
        "quantity": LOT * LOTSIZE,
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
# MARKET HELPERS
# =========================

def wait_until(t):
    while datetime.now(IST).time() < t:
        time.sleep(1)

def in_trade_window():
    now = datetime.now(IST).time()
    return STRIKE_TIME <= now < TRADE_END


# =========================
# FUTURES + PIVOT
# =========================

def get_current_month_fut():
    futs = fno_df[
        (fno_df["INSTRUMENT"] == "FUTIDX") &
        (fno_df["UNDERLYING_SYMBOL"] == SYMBOL)
    ].copy()
    futs["SM_EXPIRY_DATE"] = pd.to_datetime(futs["SM_EXPIRY_DATE"])
    futs = futs[futs["SM_EXPIRY_DATE"] >= pd.Timestamp(today)]
    return futs.sort_values("SM_EXPIRY_DATE").iloc[0]

def get_prev_day_ohlc(security_id, exchange_segment, instrument_type):
    """Previous trading day's H/L/C (Dhan daily candles)."""
    to_d = datetime.now(IST).date()
    from_d = to_d - timedelta(days=10)

    try:
        res = dhan.historical_daily_data(
            security_id=str(security_id),
            exchange_segment=exchange_segment,
            instrument_type=instrument_type,
            from_date=from_d.strftime("%Y-%m-%d"),
            to_date=to_d.strftime("%Y-%m-%d")
        )
    except Exception as e:
        print("❌ Daily data error:", security_id, e)
        return None

    data = res.get("data") if isinstance(res, dict) else None
    if not isinstance(data, dict):
        print(f"❌ Daily data failed {security_id}: status={res.get('status') if isinstance(res, dict) else None} "
              f"remarks={res.get('remarks') if isinstance(res, dict) else res}")
        return None

    timestamps = data.get("timestamp", []) or []

    last_idx = None
    for i, ts in enumerate(timestamps):
        if datetime.fromtimestamp(ts, IST).date() < to_d:
            last_idx = i

    if last_idx is None:
        print("❌ No previous day candle for", security_id)
        return None

    return {
        "date": datetime.fromtimestamp(timestamps[last_idx], IST).strftime("%Y-%m-%d"),
        "high": float(data["high"][last_idx]),
        "low": float(data["low"][last_idx]),
        "close": float(data["close"][last_idx]),
    }


# =========================
# OPTION CHAIN / STRIKES
# =========================

def get_expiry():
    try:
        res = dhan.expiry_list(
            under_security_id=NIFTY_SECURITY_ID,
            under_exchange_segment="IDX_I"
        )
        return res["data"]["data"][0]
    except Exception as e:
        print("❌ Expiry list error:", e)
        return None

def get_fresh_chain(expiry):
    """LIVE option chain from Dhan; shared cached chain only as fallback."""
    if expiry:
        try:
            res = dhan.option_chain(
                under_security_id=NIFTY_SECURITY_ID,
                under_exchange_segment="IDX_I",
                expiry=str(expiry)
            )
            oc = res["data"]["data"]["oc"]
            if oc:
                return oc
            print("⚠️ Live option chain empty:", res.get("remarks"))
        except Exception as e:
            print("⚠️ Live option chain error:", e)

    print("⚠️ Using CACHED option chain (LTPs may be stale)")
    return option_chain_manager.get_option_chain()["data"]["data"]["oc"]

def select_strike(side, oc):
    """Strike whose LTP is nearest to TARGET_LTP."""
    key = side.lower()
    rows = []
    for strike, strike_data in oc.items():
        if key not in strike_data:
            continue
        ltp = float(strike_data[key].get("last_price") or 0)
        if ltp > 0:
            rows.append((float(strike), ltp, str(strike_data[key]["security_id"])))

    if not rows:
        print(f"❌ No {side} strikes with LTP in option chain")
        return None

    strike, ltp, sid = min(rows, key=lambda r: abs(r[1] - TARGET_LTP))
    return {"strike": strike, "ltp": ltp, "security_id": sid}

def resolve_rows(side, strike):
    dhan_row = find_option_security(fno_df, strike, side, today, "NIFTY")
    angel_row = finder.get_option("NIFTY", int(strike), side)

    # Angel token must be for the SAME expiry we trade on Dhan
    try:
        d_exp = dhan_row["SM_EXPIRY_DATE"].strftime("%Y-%m-%d")
        a_exp = str(angel_row["expiry"])[:10]
        if d_exp != a_exp:
            print(f"🚨 ANGEL EXPIRY MISMATCH {side} {int(strike)} -> Dhan {d_exp} | Angel {a_exp} "
                  f"(token {angel_row['token']}) — Angel users would get the WRONG contract")
    except Exception as e:
        print("Expiry check error:", e)

    return dhan_row, angel_row


# =========================
# STATE
# =========================

fut_row = get_current_month_fut()
FUT_TOKEN = str(fut_row["SECURITY_ID"])

fut = {
    "token": FUT_TOKEN,
    "expiry": fut_row["SM_EXPIRY_DATE"].strftime("%Y-%m-%d"),
    "builder": FiveMinuteCandleBuilder(),
    "ltp": 0.0,
    "vwap": None,
    "P": None,
    "first_tick": None,
    "skip_first_candle": False,
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
        "pnl": 0.0,          # realized leg pnl for the day
        "ltp": 0.0,
    }

legs = {"CE": init_leg("CE"), "PE": init_leg("PE")}
token_map = {}      # option security_id -> "CE"/"PE"
options_ready = False
day_closed = False


# =========================
# PNL / TELEMETRY
# =========================

def running_pnl(leg):
    if not leg["position"]:
        return 0.0
    return (leg["ltp"] - leg["entry_price"]) * LOTSIZE * LOT

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
# ORDER EVENTS
# =========================

def enter_position(leg, price, reason):
    leg["position"] = True
    leg["entry_price"] = price
    leg["entry_time"] = datetime.now(IST).isoformat()

    print(f"🟢 BUY {leg['name']} {int(leg['strike'])} @ {price} | lot {LOT} | {reason}")

    run_async(emit_signal(build_payload(leg, "BUY", "entry", "ENTRY", price, leg["pnl"], realized_total())))

    log_trade_event("ENTRY", leg["name"], leg["token"], SYMBOL, "BUY",
                    LOT, price, reason, leg["pnl"], realized_total())
    log_event(f"{leg['name']} BUY", leg["token"], "ENTRY_EXECUTED", price, reason)

def exit_position(leg, price, reason):
    trade_pnl = (price - leg["entry_price"]) * LOTSIZE * LOT
    leg["pnl"] += trade_pnl

    print(f"🔴 EXIT {leg['name']} {int(leg['strike'])} @ {price} | {reason} | "
          f"trade {trade_pnl:.2f} | leg {leg['pnl']:.2f} | total {realized_total():.2f}")

    run_async(emit_signal(build_payload(leg, "SELL", "exit", "EXIT", price, trade_pnl, realized_total())))

    log_trade_event("EXIT", leg["name"], leg["token"], SYMBOL, "SELL",
                    LOT, price, reason, leg["pnl"], realized_total())
    log_event(f"{leg['name']} SELL", leg["token"], "EXIT_EXECUTED", price, reason)

    leg["position"] = False
    leg["entry_price"] = None
    leg["entry_time"] = None

def square_off_all(reason):
    global day_closed
    if day_closed:
        return
    for leg in legs.values():
        if leg["position"]:
            exit_position(leg, leg["ltp"], reason)
    day_closed = True
    telemetry["status"] = "CLOSED"
    print(f"🛑 DAY CLOSED -> {reason} | final pnl {realized_total():.2f}")

def check_time_exit():
    if datetime.now(IST).time() >= TRADE_END:
        square_off_all("TIME EXIT")
        return True
    return False

def check_mtm():
    if day_closed:
        return
    total = total_mtm()
    if total >= MTM_TARGET:
        print(f"🏁 MTM TARGET HIT ({total:.2f}) — EXIT ALL, DONE FOR THE DAY")
        square_off_all("MTM TARGET")
    elif total <= MTM_STOPLOSS:
        print(f"🚨 MTM STOPLOSS HIT ({total:.2f}) — EXIT ALL, DONE FOR THE DAY")
        square_off_all("MTM STOPLOSS")


# =========================
# FUTURES ENGINE (5-min signals)
# =========================

def on_fut_candle(candle, vwap):
    close = candle["close"]
    P = fut["P"]
    print(f"🕯 FUT 5m {candle} | P {P} | VWAP {vwap}")

    if fut["skip_first_candle"]:
        fut["skip_first_candle"] = False
        print("   ⏭ first 5m candle was partial (feed started mid-candle) — skipped")
        return

    if day_closed:
        return
    if P is None or vwap is None:
        print("   ⚠️ P / VWAP not ready — candle ignored")
        return

    ce, pe = legs["CE"], legs["PE"]

    # ---- EXITS first (allows reversal on the same candle) ----
    if ce["position"] and (close < vwap or close < P):
        why = "VWAP" if close < vwap else "P"
        exit_position(ce, ce["ltp"], f"FUT close {close} below {why} (VWAP {vwap:.2f} | P {P:.2f})")

    if pe["position"] and (close > vwap or close > P):
        why = "VWAP" if close > vwap else "P"
        exit_position(pe, pe["ltp"], f"FUT close {close} above {why} (VWAP {vwap:.2f} | P {P:.2f})")

    # ---- ENTRIES ----
    if not options_ready or not in_trade_window():
        return

    if not ce["position"] and close > P and close > vwap:
        if ce["ltp"] > 0:
            enter_position(ce, ce["ltp"], f"FUT close {close} > P {P:.2f} & VWAP {vwap:.2f}")
        else:
            print("   ⚠️ CE signal but no CE LTP yet — skipped")

    if not pe["position"] and close < P and close < vwap:
        if pe["ltp"] > 0:
            enter_position(pe, pe["ltp"], f"FUT close {close} < P {P:.2f} & VWAP {vwap:.2f}")
        else:
            print("   ⚠️ PE signal but no PE LTP yet — skipped")

def on_fut_tick(msg, ltp):
    now = datetime.now(IST)

    # first 5m candle is partial if the feed starts after its first seconds
    if fut["first_tick"] is None:
        fut["first_tick"] = now
        fut["skip_first_candle"] = (now.minute % 5 != 0) or (now.second > 30)
        print(f"📡 FUT first tick {now.strftime('%H:%M:%S')} | partial first candle: {fut['skip_first_candle']}")

    prev_vwap = fut["vwap"]   # VWAP as of the end of the previous candle
    try:
        _, vwap = vwap_manager.on_tick(msg)
        if vwap is not None:
            fut["vwap"] = float(vwap)
    except Exception as e:
        print("VWAP error:", e)

    fut["ltp"] = ltp
    candle = fut["builder"].process_tick(msg)

    if check_time_exit():
        return

    if candle:
        on_fut_candle(candle, prev_vwap if prev_vwap is not None else fut["vwap"])


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
        if token == FUT_TOKEN:
            on_fut_tick(msg, ltp)
        else:
            name = token_map.get(token)
            if not name:
                return
            legs[name]["ltp"] = ltp
            check_time_exit()

        update_telemetry()
        check_mtm()


def on_tick(token, msg):
    t = str(token)
    if t != FUT_TOKEN and t not in token_map:
        return
    on_message(msg)


# =========================
# SETUP (background — never blocks main.py's feed)
# =========================

def setup():
    global options_ready

    print(f"\n🚀 {STRATEGY_NAME} STARTED | FUT {FUT_TOKEN} (expiry {fut['expiry']})\n")

    refresh_users()
    threading.Thread(target=users_refresher, daemon=True).start()

    # ---- P from futures previous day ----
    ohlc = None
    while ohlc is None:
        ohlc = get_prev_day_ohlc(FUT_TOKEN, "NSE_FNO", "FUTIDX")
        if ohlc is None:
            time.sleep(5)

    P = (ohlc["high"] + ohlc["low"] + ohlc["close"]) / 3
    with state_lock:
        fut["P"] = round(P, 2)
    print(f"📈 FUT PrevDay {ohlc} -> P {fut['P']}")

    # ---- 09:20 strike selection ----
    print(f"⏳ Waiting for {STRIKE_TIME.strftime('%H:%M')} strike selection...")
    wait_until(STRIKE_TIME)

    expiry = get_expiry()
    print("📅 Option expiry:", expiry)

    selected = {}
    for side in ("CE", "PE"):
        sel = None
        for attempt in range(20):
            try:
                sel = select_strike(side, get_fresh_chain(expiry))
            except Exception as e:
                print(f"❌ {side} selection error:", e)
            if sel:
                break
            time.sleep(3.1)
        if not sel:
            print(f"❌ Could not select {side} strike — strategy will not trade today")
            return
        selected[side] = sel

    for side, sel in selected.items():
        dhan_row, angel_row = resolve_rows(side, sel["strike"])
        with state_lock:
            leg = legs[side]
            leg["token"] = sel["security_id"]
            leg["strike"] = sel["strike"]
            leg["dhan_row"] = dhan_row
            leg["angel_row"] = angel_row
            leg["ltp"] = sel["ltp"]
            token_map[leg["token"]] = side

        subscribe(leg["token"], on_tick)
        logtradeleg(COMMON_ID, side, f"NIFTY {side} {sel['strike']}",
                    str(sel["strike"]), str(today), leg["token"])
        print(f"📌 {side} {int(sel['strike'])} [{leg['token']}] LTP {sel['ltp']}")

    with state_lock:
        options_ready = True
        update_telemetry()
    print("✅ Options ready — trading on 5m FUT candle closes")


# =====================
# START FEED
# =====================

# futures subscribed right away so VWAP + 5m candles build from the first tick
subscribe(FUT_TOKEN, on_tick)
threading.Thread(target=setup, daemon=True).start()