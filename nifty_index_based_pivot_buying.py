import time
import bisect
import pytz
import requests
from datetime import datetime, timedelta, time as dtime
from dotenv import load_dotenv
import os
from dhanhq import DhanContext, dhanhq
from dhan_token import get_access_token
from candle_builder import OneMinuteCandleBuilder
from find_security import load_fno_master, find_option_security
import threading
from signal_emitter import emit_signal
from dispatcher import subscribe
from queue import Queue
import asyncio
from find_instrument import FindInstrument
import option_chain_manager

try:
    from dispatcher import unsubscribe
except ImportError:
    unsubscribe = None


# =========================================================
# STRATEGY
# ---------------------------------------------------------
# Entries / candle SL  -> NIFTY INDEX chart (spot, security 13)
# Orders / PnL / target / option SL -> OPTIONS (CE & PE)
#
# - Index traditional pivots from previous-day H/L/C of NIFTY spot
# - Index tick touches a pivot -> armed for BOTH CE and PE (flat sides)
# - Index 1-min candle close on armed pivot:
#       CE BUY : close > pivot, avg > pivot, avg < close
#       PE BUY : close < pivot, avg < pivot, avg > close
# - Exits:
#       CE : index candle close BELOW entry pivot   (SL)
#       PE : index candle close ABOVE entry pivot   (SL)
#       any: option tick <= entry - 15              (SL)
#       any: cumulative SIDE target (realized + running, across strikes)
# - Lot 2, +1 after SL (max 15), back to 2 after target
# - Fresh index pivot touch needed after every exit
# - Strike change (flat only): option > 280 or < 170 -> walk strikes
#   until 180 < LTP < 280. Armed index pivot stays armed.
# - Overall MTM -13000 -> exit all, no more trades. Time exit 15:14.
# =========================================================


# =========================
# CONFIG
# =========================

load_dotenv()

STRATEGY_NAME = "NIFTY_OPTION_BUYING_PIVOT_INDEX_BASED"

# 🔥 PUT THE NEW STRATEGY UUID HERE (deployments / logs / telemetry use this)
COMMON_ID = "d60a5587-5081-40d5-af6b-476c9a7f13bb"
strategy_id = "d60a5587-5081-40d5-af6b-476c9a7f13bb"

SYMBOL = "NIFTY"

TRADE_LOG_URL = "https://algoapi.dreamintraders.in/api/paperlogger/event"
EVENT_LOG_URL = "https://algoapi.dreamintraders.in/api/paperlogger/paperlogger"
TELEMETRY_URL = "https://algoapi.dreamintraders.in/api/telemetry"
TRADELEG_URL = "https://algoapi.dreamintraders.in/api/tradelegs/create"
DEPLOYMENTS_URL = f"https://algoapi.dreamintraders.in/api/deployments/today/{strategy_id}"

IST = pytz.timezone("Asia/Kolkata")

TRADE_START = dtime(9, 17)
TRADE_END   = dtime(15, 14)

NIFTY_SECURITY_ID = 13
INDEX_TOKEN       = str(NIFTY_SECURITY_ID)

TARGET_LTP       = 180     # 09:17 initial selection: premium nearest to this

ROLL_UPPER       = 280     # flat + option LTP above this -> strike change
ROLL_LOWER       = 170     # flat + option LTP below this -> strike change
PICK_MIN         = 180     # on strike change, new strike LTP must be
PICK_MAX         = 280     #   PICK_MIN < ltp < PICK_MAX
MAX_ROLL_STEPS   = 20      # max strikes to walk on one strike change
CHAIN_MIN_GAP    = 3.1     # Dhan option chain API: 1 call per 3 sec

# Cumulative target PER SIDE (CE / PE separately, across all strikes of that side)
#   side pnl (realized + running) >= target_points * LOTSIZE  -> exit
#   after a hit: target_points += step, lot -> 2, fresh pivot touch needed
CUM_TARGET_START_POINTS = 50   # first target  = 50 * 65 = ₹3,250
CUM_TARGET_STEP_POINTS  = 50   # next targets  = ₹6,500, ₹9,750, ...

OPTION_SL_POINTS = 15      # option tick <= entry - 15 -> SL
BASE_LOT         = 2
MAX_LOT          = 15
MAX_LOSS         = -13000  # overall MTM (realized + running, both legs)
LOTSIZE          = 65

REARM_AFTER_TARGET    = True  # fresh index pivot touch needed after target exit too
ROLL_RETRY_SECONDS    = 15    # cooldown if a roll attempt fails
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
            fut = asyncio.run_coroutine_threadsafe(coro, loop)
            fut.add_done_callback(_signal_done)
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
    """Everything comes from the leg's CURRENT contract (safe after strike change)."""
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

    print(f"📦 PAYLOAD {symbol} | {side} | {event_type} | {reason} | price {price} | lot {leg['lot']}")

    return {
        "strategy_id": COMMON_ID,
        "users": get_users(),
        "option": name,
        "side": side,
        "quantity": leg["lot"] * LOTSIZE,
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

def wait_for_start():
    print("⏳ Waiting for market...")
    while True:
        if datetime.now(IST).time() >= TRADE_START:
            print("✅ Market Started")
            return
        time.sleep(1)

def in_trade_window():
    now = datetime.now(IST).time()
    return TRADE_START <= now < TRADE_END


# =========================
# INDEX PIVOTS
# =========================

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

def calculate_pivots(ohlc):
    h, l, c = ohlc["high"], ohlc["low"], ohlc["close"]
    p = (h + l + c) / 3

    pivots = {
        "S3": l - 2 * (h - p),
        "S2": p - (h - l),
        "S1": 2 * p - h,
        "P":  p,
        "R1": 2 * p - l,
        "R2": p + (h - l),
        "R3": h + 2 * (p - l),
    }
    levels = sorted([(k, round(v, 2)) for k, v in pivots.items()], key=lambda x: x[1])
    return pivots, levels


# =========================
# OPTION CHAIN (LIVE)
# =========================

EXPIRY = None
chain_lock = threading.Lock()
_last_chain_call = [0.0]

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

def get_fresh_chain():
    """
    LIVE option chain straight from Dhan (rate limited to 1 call / 3 sec).
    The shared option_chain_manager copy is only a fallback — it is fetched
    once at startup, so its LTPs are stale for strike changes later in the day.
    """
    with chain_lock:
        if EXPIRY:
            gap = CHAIN_MIN_GAP - (time.time() - _last_chain_call[0])
            if gap > 0:
                time.sleep(gap)
            try:
                res = dhan.option_chain(
                    under_security_id=NIFTY_SECURITY_ID,
                    under_exchange_segment="IDX_I",
                    expiry=str(EXPIRY)
                )
                _last_chain_call[0] = time.time()
                oc = res["data"]["data"]["oc"]
                if oc:
                    return oc
                print("⚠️ Live option chain empty:", res.get("remarks"))
            except Exception as e:
                _last_chain_call[0] = time.time()
                print("⚠️ Live option chain error:", e)

        print("⚠️ Using CACHED option chain (LTPs may be stale)")
        return option_chain_manager.get_option_chain()["data"]["data"]["oc"]

def chain_rows(oc, side):
    """[(strike, ltp, security_id)] for one side, sorted by strike."""
    key = side.lower()
    rows = []
    for strike, strike_data in oc.items():
        if key not in strike_data:
            continue
        ltp = float(strike_data[key].get("last_price") or 0)
        rows.append((float(strike), ltp, str(strike_data[key]["security_id"])))
    rows.sort(key=lambda r: r[0])
    return rows


# =========================
# STRIKE SELECTION
# =========================

def select_initial_strike(side, oc):
    """09:17 -> strike whose LTP is nearest to TARGET_LTP."""
    rows = [r for r in chain_rows(oc, side) if r[1] > 0]
    if not rows:
        print(f"❌ No {side} strikes with LTP in option chain")
        return None

    strike, ltp, sid = min(rows, key=lambda r: abs(r[1] - TARGET_LTP))
    return {"strike": strike, "ltp": ltp, "security_id": sid}

def roll_trigger(ltp):
    if ltp > ROLL_UPPER:
        return "HIGH"
    if ltp < ROLL_LOWER:
        return "LOW"
    return None

def roll_direction(side, trigger):
    """
    +1 -> walk to higher strikes, -1 -> walk to lower strikes
    CE  LOW (<170)  -> lower strikes  (22300 -> 22250 -> 22200 ...)
    CE  HIGH (>280) -> higher strikes (22300 -> 22350 -> 22400 ...)
    PE  LOW (<170)  -> higher strikes
    PE  HIGH (>280) -> lower strikes
    """
    if side == "CE":
        return -1 if trigger == "LOW" else +1
    return +1 if trigger == "LOW" else -1

def select_roll_strike(side, current_strike, trigger):
    """Walk strike by strike from the current one; first with PICK_MIN < LTP < PICK_MAX wins."""
    rows = chain_rows(get_fresh_chain(), side)
    strikes = [r[0] for r in rows]
    direction = roll_direction(side, trigger)
    cur = float(current_strike)

    if direction > 0:
        idx = bisect.bisect_right(strikes, cur)
    else:
        idx = bisect.bisect_left(strikes, cur) - 1

    steps = 0
    while 0 <= idx < len(rows) and steps < MAX_ROLL_STEPS:
        strike, ltp, sid = rows[idx]
        steps += 1
        print(f"   🔎 {side} check {int(strike)} -> LTP {ltp}")

        if ltp > 0:
            if PICK_MIN < ltp < PICK_MAX:
                return {"strike": strike, "ltp": ltp, "security_id": sid}
            # premium only moves further away from the range from here on
            if trigger == "LOW" and ltp >= PICK_MAX:
                break
            if trigger == "HIGH" and ltp <= PICK_MIN:
                break

        idx += direction

    return None

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

index = {
    "token": INDEX_TOKEN,
    "builder": OneMinuteCandleBuilder(),
    "ltp": 0.0,
    "prev_ltp": None,
    "pivots": {},
    "levels": [],
    "skip_first_candle": True,   # first candle after start is partial
}

def init_leg(name):
    return {
        "name": name,
        "token": None,
        "strike": None,
        "dhan_row": None,
        "angel_row": None,

        "armed_pivot": None,      # (name, value) of last INDEX pivot touched while flat

        "position": False,
        "entry_price": None,      # option price
        "entry_time": None,
        "entry_pivot": None,      # INDEX pivot used for this trade's candle SL

        "lot": BASE_LOT,
        "pnl": 0.0,               # realized SIDE pnl (all strikes of this side)
        "target_points": CUM_TARGET_START_POINTS,   # cumulative side target
        "ltp": 0.0,               # option ltp

        "trading_disabled": False,
        "rolling": False,
        "last_roll_attempt": 0.0,
    }

legs = {"CE": init_leg("CE"), "PE": init_leg("PE")}
token_map = {}   # option security_id -> "CE"/"PE"
day_closed = False

def apply_contract(leg, sel, dhan_row, angel_row):
    """Swap the option contract. Armed index pivot, lot, pnl and target are kept."""
    leg["token"] = sel["security_id"]
    leg["strike"] = sel["strike"]
    leg["dhan_row"] = dhan_row
    leg["angel_row"] = angel_row
    leg["ltp"] = sel["ltp"]
    print(f"📌 {leg['name']} {int(leg['strike'])} [{leg['token']}] LTP {sel['ltp']} | "
          f"armed {leg['armed_pivot']} | lot {leg['lot']}")


# =========================
# PNL HELPERS
# =========================

def running_pnl(leg):
    if not leg["position"]:
        return 0.0
    return (leg["ltp"] - leg["entry_price"]) * LOTSIZE * leg["lot"]

def realized_total():
    return legs["CE"]["pnl"] + legs["PE"]["pnl"]

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

def enter_position(leg, price, pivot, idx_close, idx_avg):
    leg["position"] = True
    leg["entry_price"] = price
    leg["entry_time"] = datetime.now(IST).isoformat()
    leg["entry_pivot"] = pivot

    side_word = "above" if leg["name"] == "CE" else "below"
    reason = f"Index close {idx_close} {side_word} {pivot[0]} {pivot[1]} (avg {idx_avg:.2f})"

    target_rs = leg["target_points"] * LOTSIZE
    tgt_price = price + (target_rs - leg["pnl"]) / (LOTSIZE * leg["lot"])
    print(f"🟢 BUY {leg['name']} {int(leg['strike'])} @ {price} | lot {leg['lot']} | {reason} | "
          f"SIDE TGT ₹{target_rs:.0f} (side pnl {leg['pnl']:.2f} -> ~price {tgt_price:.2f}) | "
          f"OPT SL {price - OPTION_SL_POINTS} | INDEX SL close {'<' if leg['name'] == 'CE' else '>'} {pivot[1]}")

    run_async(emit_signal(build_payload(leg, "BUY", "entry", "ENTRY", price, leg["pnl"], realized_total())))

    log_trade_event("ENTRY", leg["name"], leg["token"], SYMBOL, "BUY",
                    leg["lot"], price, reason, leg["pnl"], realized_total())
    log_event(f"{leg['name']} BUY", leg["token"], "ENTRY_EXECUTED", price, reason)

def exit_position(leg, price, reason, kind):
    """kind: TARGET / SL / FORCE"""
    trade_pnl = (price - leg["entry_price"]) * LOTSIZE * leg["lot"]
    leg["pnl"] += trade_pnl

    print(f"🔴 EXIT {leg['name']} {int(leg['strike'])} @ {price} | {reason} | "
          f"trade {trade_pnl:.2f} | side {leg['pnl']:.2f} | total {realized_total():.2f}")

    run_async(emit_signal(build_payload(leg, "SELL", "exit", "EXIT", price, trade_pnl, realized_total())))

    log_trade_event("EXIT", leg["name"], leg["token"], SYMBOL, "SELL",
                    leg["lot"], price, reason, leg["pnl"], realized_total())
    log_event(f"{leg['name']} SELL", leg["token"], "EXIT_EXECUTED", price, reason)

    leg["position"] = False
    leg["entry_price"] = None
    leg["entry_time"] = None
    leg["entry_pivot"] = None

    # fresh index pivot touch required after exit
    if kind == "SL" or (kind == "TARGET" and REARM_AFTER_TARGET):
        leg["armed_pivot"] = None

    # lot progression
    if kind == "TARGET":
        leg["lot"] = BASE_LOT
        leg["target_points"] += CUM_TARGET_STEP_POINTS
        # if the exit overshot by more than one step, move the bar above current pnl
        while leg["target_points"] * LOTSIZE <= leg["pnl"]:
            leg["target_points"] += CUM_TARGET_STEP_POINTS
        print(f"🎯 {leg['name']} next side target -> {leg['target_points']} pts "
              f"(₹{leg['target_points'] * LOTSIZE:.0f}) | lot -> {leg['lot']}")
    elif kind == "SL":
        leg["lot"] = min(leg["lot"] + 1, MAX_LOT)
        print(f"📈 {leg['name']} next lot -> {leg['lot']}")

def square_off_all(reason):
    global day_closed
    if day_closed:
        return
    for leg in legs.values():
        if leg["position"]:
            exit_position(leg, leg["ltp"], reason, "FORCE")
        leg["trading_disabled"] = True
    day_closed = True
    telemetry["status"] = "CLOSED"
    print(f"🛑 DAY CLOSED -> {reason}")

def check_time_exit():
    if datetime.now(IST).time() >= TRADE_END:
        square_off_all("TIME EXIT")
        return True
    return False

def check_mtm():
    total = realized_total() + running_pnl(legs["CE"]) + running_pnl(legs["PE"])
    if total <= MAX_LOSS and not day_closed:
        print(f"🚨 MTM LIMIT HIT ({total:.2f}) — FORCE EXIT ALL")
        square_off_all("FORCE EXIT MTM")


# =========================
# INDEX ENGINE (entries + candle SL)
# =========================

def on_index_candle(candle):
    """Returns the set of sides that exited on this candle."""
    close = candle["close"]
    avg = (candle["open"] + candle["high"] + candle["low"] + candle["close"]) / 4
    exited = set()

    # ---- index candle close SL ----
    for leg in legs.values():
        if not leg["position"]:
            continue
        pivot = leg["entry_pivot"]
        if leg["name"] == "CE" and close < pivot[1]:
            exit_position(leg, leg["ltp"], f"Index close {close} below {pivot[0]} {pivot[1]}", "SL")
            exited.add("CE")
        elif leg["name"] == "PE" and close > pivot[1]:
            exit_position(leg, leg["ltp"], f"Index close {close} above {pivot[0]} {pivot[1]}", "SL")
            exited.add("PE")

    # first candle after start is partial -> no entries on it
    if index["skip_first_candle"]:
        index["skip_first_candle"] = False
        return exited

    if not in_trade_window():
        return exited

    # ---- entries ----
    for leg in legs.values():
        if leg["position"] or leg["trading_disabled"] or leg["rolling"] or leg["name"] in exited:
            continue
        pivot = leg["armed_pivot"]
        if pivot is None or leg["ltp"] <= 0:
            continue

        level = pivot[1]
        if leg["name"] == "CE":
            ok = close > level and avg > level and avg < close
        else:
            ok = close < level and avg < level and avg > close

        if ok:
            enter_position(leg, leg["ltp"], pivot, close, avg)

    return exited

def update_index_touch(ltp, skip_sides):
    """Index crossing a pivot arms BOTH flat sides."""
    prev = index["prev_ltp"]
    if prev is None:
        return

    lo, hi = min(prev, ltp), max(prev, ltp)
    crossed = [lv for lv in index["levels"] if lo <= lv[1] <= hi]
    if not crossed:
        return

    level = min(crossed, key=lambda lv: abs(lv[1] - ltp))

    armed = []
    for leg in legs.values():
        if leg["position"] or leg["trading_disabled"] or leg["name"] in skip_sides:
            continue
        if leg["armed_pivot"] != level:
            leg["armed_pivot"] = level
            armed.append(leg["name"])

    if armed:
        print(f"🎯 INDEX touched {level[0]} {level[1]} (ltp {ltp}) -> ARMED {' & '.join(armed)}")

def on_index_tick(msg, ltp):
    index["ltp"] = ltp
    candle = index["builder"].process_tick(msg)

    if check_time_exit():
        index["prev_ltp"] = ltp
        return

    exited = set()
    if candle:
        print(f"🕯 INDEX {candle}")
        exited = on_index_candle(candle)

    update_index_touch(ltp, exited)
    index["prev_ltp"] = ltp


# =========================
# OPTION ENGINE (target + option SL + strike change)
# =========================

def check_option_exits(leg, ltp):
    # ---- cumulative SIDE target (realized + running, across strikes) ----
    target_rs = leg["target_points"] * LOTSIZE
    side_total = leg["pnl"] + running_pnl(leg)
    if side_total >= target_rs:
        exit_position(leg, ltp,
                      f"{leg['name']} side target {leg['target_points']} pts "
                      f"(₹{target_rs:.0f}) | side pnl {side_total:.2f}", "TARGET")
        return True

    # ---- option SL: entry - 15 ----
    if ltp <= leg["entry_price"] - OPTION_SL_POINTS:
        exit_position(leg, ltp,
                      f"Option {ltp} <= entry {leg['entry_price']} - {OPTION_SL_POINTS}", "SL")
        return True

    return False

def on_option_tick(leg, ltp):
    leg["ltp"] = ltp

    if check_time_exit():
        return

    if leg["trading_disabled"] or leg["rolling"]:
        return

    if leg["position"]:
        check_option_exits(leg, ltp)

    # ---- STRIKE CHANGE (only when flat: > 280 or < 170) ----
    if not leg["position"] and in_trade_window():
        trigger = roll_trigger(ltp)
        if trigger:
            start_roll(leg, trigger)


# =========================
# STRIKE ROLL
# =========================

def start_roll(leg, trigger):
    if leg["rolling"]:
        return
    if time.time() - leg["last_roll_attempt"] < ROLL_RETRY_SECONDS:
        return
    leg["rolling"] = True
    leg["last_roll_attempt"] = time.time()

    cond = f"> {ROLL_UPPER}" if trigger == "HIGH" else f"< {ROLL_LOWER}"
    way = "higher" if roll_direction(leg["name"], trigger) > 0 else "lower"
    print(f"🔁 {leg['name']} {int(leg['strike'])} LTP {leg['ltp']} {cond} -> strike change, walking {way} strikes")

    threading.Thread(
        target=roll_worker,
        args=(leg["name"], trigger, leg["strike"]),
        daemon=True
    ).start()

def roll_worker(name, trigger, from_strike):
    leg = legs[name]
    try:
        sel = select_roll_strike(name, from_strike, trigger)

        if not sel:
            print(f"⚠️ {name} no strike with {PICK_MIN} < LTP < {PICK_MAX} — staying on "
                  f"{int(from_strike)}, retry in {ROLL_RETRY_SECONDS}s")
            with state_lock:
                leg["rolling"] = False
            return

        dhan_row, angel_row = resolve_rows(name, sel["strike"])

        with state_lock:
            if leg["position"] or leg["trading_disabled"] or day_closed:
                leg["rolling"] = False
                return

            old_token = leg["token"]
            old_strike = leg["strike"]
            token_map.pop(old_token, None)

            # armed index pivot, lot, side pnl and side target all carried over
            apply_contract(leg, sel, dhan_row, angel_row)
            token_map[leg["token"]] = name
            leg["rolling"] = False

            # push new strike LTP to telemetry right away
            update_telemetry()

        if unsubscribe:
            try:
                unsubscribe(old_token, on_tick)
            except Exception as e:
                print("Unsubscribe error:", e)

        subscribe(leg["token"], on_tick)

        logtradeleg(COMMON_ID, name, f"NIFTY {name} {sel['strike']}",
                    str(sel["strike"]), str(today), leg["token"])

        print(f"✅ {name} STRIKE CHANGED {int(old_strike)} [{old_token}] -> "
              f"{int(sel['strike'])} [{leg['token']}] | LTP {sel['ltp']} | lot {leg['lot']} | "
              f"side pnl {leg['pnl']:.2f} | armed {leg['armed_pivot']}")

    except Exception as e:
        print(f"❌ {name} roll error:", e)
        with state_lock:
            leg["rolling"] = False


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
        else:
            name = token_map.get(token)
            if not name:
                return
            on_option_tick(legs[name], ltp)

        update_telemetry()
        check_mtm()


def on_tick(token, msg):
    t = str(token)
    if t != INDEX_TOKEN and t not in token_map:
        return
    on_message(msg)


# =========================
# START
# =========================

wait_for_start()

print(f"\n🚀 {STRATEGY_NAME} STARTED\n")

refresh_users()
threading.Thread(target=users_refresher, daemon=True).start()

EXPIRY = get_expiry()
print("📅 Expiry:", EXPIRY)

# ---- INDEX PIVOTS (NIFTY spot, previous day) ----
idx_ohlc = None
for attempt in range(20):
    idx_ohlc = get_prev_day_ohlc(NIFTY_SECURITY_ID, "IDX_I", "INDEX")
    if idx_ohlc:
        break
    time.sleep(3)

if not idx_ohlc:
    raise SystemExit("❌ Could not fetch NIFTY previous-day OHLC — stopping strategy")

index["pivots"], index["levels"] = calculate_pivots(idx_ohlc)
print(f"📈 NIFTY PrevDay {idx_ohlc}")
print("📐 INDEX PIVOTS -> " + " | ".join(f"{k}:{v}" for k, v in index["levels"]))

# ---- OPTION STRIKES ----
for side in ("CE", "PE"):
    sel = None
    for attempt in range(20):
        try:
            sel = select_initial_strike(side, get_fresh_chain())
        except Exception as e:
            print(f"❌ {side} selection error:", e)
        if sel:
            break
        time.sleep(3)

    if not sel:
        raise SystemExit(f"❌ Could not select {side} strike — stopping strategy")

    dhan_row, angel_row = resolve_rows(side, sel["strike"])
    print(f"angel token {side}:", angel_row)

    apply_contract(legs[side], sel, dhan_row, angel_row)
    token_map[legs[side]["token"]] = side

    logtradeleg(COMMON_ID, side, f"NIFTY {side} {sel['strike']}",
                str(sel["strike"]), str(today), legs[side]["token"])

if not EXPIRY:
    EXPIRY = legs["CE"]["dhan_row"]["SM_EXPIRY_DATE"].strftime("%Y-%m-%d")
    print("📅 Expiry (from FNO master):", EXPIRY)

update_telemetry()


# =====================
# START FEED
# =====================

subscribe(INDEX_TOKEN, on_tick)
for t in list(token_map.keys()):
    subscribe(t, on_tick)