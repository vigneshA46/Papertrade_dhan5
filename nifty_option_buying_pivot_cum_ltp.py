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


# =========================
# CONFIG
# =========================

load_dotenv()

STRATEGY_NAME = "NIFTY_OPTION_BUYING_PIVOT"

# 🔥 PUT THE NEW STRATEGY UUID HERE (deployments / logs / telemetry use this)
COMMON_ID = "4f62a290-efc7-4ed5-889d-0e10d1ed6a31"
strategy_id = "4f62a290-efc7-4ed5-889d-0e10d1ed6a31"

SYMBOL = "NIFTY"

TRADE_LOG_URL = "https://algoapi.dreamintraders.in/api/paperlogger/event"
EVENT_LOG_URL = "https://algoapi.dreamintraders.in/api/paperlogger/paperlogger"
TELEMETRY_URL = "https://algoapi.dreamintraders.in/api/telemetry"
TRADELEG_URL = "https://algoapi.dreamintraders.in/api/tradelegs/create"
DEPLOYMENTS_URL = f"https://algoapi.dreamintraders.in/api/deployments/today/{strategy_id}"

IST = pytz.timezone("Asia/Kolkata")

TRADE_START = dtime(9, 17)
TRADE_END   = dtime(15, 14)

TARGET_LTP       = 180     # 09:17 initial selection: premium nearest to this

ROLL_UPPER       = 280     # flat + tick LTP above this  -> strike change
ROLL_LOWER       = 170     # flat + tick LTP below this  -> strike change
PICK_MIN         = 180     # on strike change, new strike LTP must be
PICK_MAX         = 280     #   PICK_MIN < ltp < PICK_MAX
MAX_ROLL_STEPS   = 20      # max strikes to walk on one strike change
NIFTY_SECURITY_ID = 13
CHAIN_MIN_GAP    = 3.1     # Dhan option chain API: 1 call per 3 sec
TARGET_POINTS    = 25      # fixed target per trade
SL_BUFFER        = 8       # tick SL = pivot - 8
BASE_LOT         = 2
MAX_LOT          = 15
MAX_LOSS         = -13000  # overall MTM (realized + running, both legs)
LOTSIZE          = 65

REARM_AFTER_TARGET   = True   # fresh pivot touch needed after target exit too
ROLL_RETRY_SECONDS   = 15     # cooldown if a roll attempt fails
MAX_STRIKE_CANDIDATES = 6     # how many nearest-to-180 strikes to try for pivot data
USERS_REFRESH_SECONDS = 30

today = datetime.now(IST).strftime("%Y-%m-%d")


# =========================
# LOGIN
# =========================

client_id = os.getenv("CLIENT_ID1")
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
    """Everything comes from the leg's CURRENT contract (safe after strike roll)."""
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
# PIVOTS
# =========================

def get_prev_day_ohlc(security_id):
    """Previous trading day's OHLC of the option contract itself."""
    to_d = datetime.now(IST).date()
    from_d = to_d - timedelta(days=10)

    try:
        res = dhan.historical_daily_data(
            security_id=str(security_id),
            exchange_segment="NSE_FNO",
            instrument_type="OPTIDX",
            from_date=from_d.strftime("%Y-%m-%d"),
            to_date=to_d.strftime("%Y-%m-%d")
        )
    except Exception as e:
        print("❌ Daily data error:", security_id, e)
        return None

    data = res.get("data") or {}
    timestamps = data.get("timestamp", []) or []

    last_idx = None
    for i, ts in enumerate(timestamps):
        if datetime.fromtimestamp(ts, IST).date() < to_d:
            last_idx = i

    if last_idx is None:
        print("❌ No previous day candle for", security_id)
        return None

    return {
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
    # option premium can't go below 0 -> drop negative levels
    levels = sorted(
        [(k, round(v, 2)) for k, v in pivots.items() if v > 0],
        key=lambda x: x[1]
    )
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

def build_selection(strike, ltp, sid):
    ohlc = get_prev_day_ohlc(sid)
    if not ohlc:
        return None
    pivots, levels = calculate_pivots(ohlc)
    return {
        "strike": strike,
        "ltp": ltp,
        "security_id": sid,
        "ohlc": ohlc,
        "pivots": pivots,
        "levels": levels
    }


# =========================
# STRIKE SELECTION
# =========================

def select_initial_strike(side, oc):
    """09:17 -> strike whose LTP is nearest to TARGET_LTP (with prev-day data for pivots)."""
    rows = [r for r in chain_rows(oc, side) if r[1] > 0]
    rows.sort(key=lambda r: abs(r[1] - TARGET_LTP))

    for strike, ltp, sid in rows[:MAX_STRIKE_CANDIDATES]:
        sel = build_selection(strike, ltp, sid)
        if sel:
            return sel

    print(f"❌ No valid {side} strike found near {TARGET_LTP}")
    return None

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
                sel = build_selection(strike, ltp, sid)
                if sel:
                    return sel
                print(f"   ⚠️ {side} {int(strike)} has no prev-day data, checking next")
            # premium only moves further away from the range from here on
            elif trigger == "LOW" and ltp >= PICK_MAX:
                break
            elif trigger == "HIGH" and ltp <= PICK_MIN:
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
# LEG STATE
# =========================

def init_leg(name):
    return {
        "name": name,
        "token": None,
        "strike": None,
        "dhan_row": None,
        "angel_row": None,
        "builder": None,
        "pivots": {},
        "levels": [],

        "armed_pivot": None,      # (name, value) of last touched pivot
        "prev_ltp": None,
        "skip_first_candle": True,

        "position": False,
        "entry_price": None,
        "entry_time": None,
        "entry_pivot": None,      # (name, value) used for this trade's SL

        "lot": BASE_LOT,
        "pnl": 0.0,               # realized leg pnl
        "ltp": 0.0,

        "trading_disabled": False,
        "rolling": False,
        "last_roll_attempt": 0.0,
    }

legs = {"CE": init_leg("CE"), "PE": init_leg("PE")}
token_map = {}   # security_id -> "CE"/"PE"
day_closed = False

def apply_contract(leg, sel, dhan_row, angel_row):
    leg["token"] = sel["security_id"]
    leg["strike"] = sel["strike"]
    leg["dhan_row"] = dhan_row
    leg["angel_row"] = angel_row
    leg["builder"] = OneMinuteCandleBuilder()
    leg["pivots"] = sel["pivots"]
    leg["levels"] = sel["levels"]
    leg["armed_pivot"] = None
    leg["prev_ltp"] = None
    leg["skip_first_candle"] = True
    leg["ltp"] = sel["ltp"]

    lv = " | ".join(f"{k}:{v}" for k, v in leg["levels"])
    print(f"📌 {leg['name']} {int(leg['strike'])} [{leg['token']}] LTP {sel['ltp']} | PrevDay {sel['ohlc']}")
    print(f"📐 {leg['name']} PIVOTS -> {lv}")


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

def enter_position(leg, price, pivot):
    leg["position"] = True
    leg["entry_price"] = price
    leg["entry_time"] = datetime.now(IST).isoformat()
    leg["entry_pivot"] = pivot

    reason = f"Pivot {pivot[0]} {pivot[1]} reclaim"
    print(f"🟢 BUY {leg['name']} {int(leg['strike'])} @ {price} | lot {leg['lot']} | {reason} | "
          f"TGT {price + TARGET_POINTS} | SL {round(pivot[1] - SL_BUFFER, 2)}")

    run_async(emit_signal(build_payload(leg, "BUY", "entry", "ENTRY", price, leg["pnl"], realized_total())))

    log_trade_event("ENTRY", leg["name"], leg["token"], SYMBOL, "BUY",
                    leg["lot"], price, reason, leg["pnl"], realized_total())
    log_event(f"{leg['name']} BUY", leg["token"], "ENTRY_EXECUTED", price, reason)

def exit_position(leg, price, reason, kind):
    """kind: TARGET / SL / FORCE"""
    trade_pnl = (price - leg["entry_price"]) * LOTSIZE * leg["lot"]
    leg["pnl"] += trade_pnl

    print(f"🔴 EXIT {leg['name']} {int(leg['strike'])} @ {price} | {reason} | "
          f"trade {trade_pnl:.2f} | leg {leg['pnl']:.2f} | total {realized_total():.2f}")

    run_async(emit_signal(build_payload(leg, "SELL", "exit", "EXIT", price, trade_pnl, realized_total())))

    log_trade_event("EXIT", leg["name"], leg["token"], SYMBOL, "SELL",
                    leg["lot"], price, reason, leg["pnl"], realized_total())
    log_event(f"{leg['name']} SELL", leg["token"], "EXIT_EXECUTED", price, reason)

    leg["position"] = False
    leg["entry_price"] = None
    leg["entry_time"] = None
    leg["entry_pivot"] = None

    # fresh pivot touch required after exit
    if kind == "SL" or (kind == "TARGET" and REARM_AFTER_TARGET):
        leg["armed_pivot"] = None

    # lot progression
    if kind == "TARGET":
        leg["lot"] = BASE_LOT
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


# =========================
# STRATEGY ENGINE
# =========================

def on_candle_close(leg, candle, ltp):
    close = candle["close"]

    # ---- candle close SL ----
    if leg["position"]:
        pivot = leg["entry_pivot"]
        if close < pivot[1]:
            exit_position(leg, ltp, f"Candle close {close} below {pivot[0]} {pivot[1]}", "SL")
            return True
        return False

    # first candle after (re)selection is partial -> ignore for entry
    if leg["skip_first_candle"]:
        leg["skip_first_candle"] = False
        return False

    pivot = leg["armed_pivot"]
    if pivot is None or not in_trade_window():
        return False

    avg = (candle["open"] + candle["high"] + candle["low"] + candle["close"]) / 4
    level = pivot[1]

    if close > level and avg > level and avg < close:
        enter_position(leg, ltp, pivot)

    return False

def check_tick_exits(leg, ltp):
    if ltp >= leg["entry_price"] + TARGET_POINTS:
        exit_position(leg, ltp, f"Target {TARGET_POINTS} pts", "TARGET")
        return True

    pivot = leg["entry_pivot"]
    if ltp <= pivot[1] - SL_BUFFER:
        exit_position(leg, ltp, f"Tick {ltp} <= {pivot[0]} {pivot[1]} - {SL_BUFFER}", "SL")
        return True

    return False

def update_touch(leg, ltp):
    prev = leg["prev_ltp"]
    if prev is None:
        return

    lo, hi = min(prev, ltp), max(prev, ltp)
    crossed = [lv for lv in leg["levels"] if lo <= lv[1] <= hi]
    if not crossed:
        return

    level = min(crossed, key=lambda lv: abs(lv[1] - ltp))
    if leg["armed_pivot"] != level:
        leg["armed_pivot"] = level
        print(f"🎯 {leg['name']} touched {level[0]} {level[1]} (ltp {ltp}) -> ARMED")

def check_mtm():
    total = realized_total() + running_pnl(legs["CE"]) + running_pnl(legs["PE"])
    if total <= MAX_LOSS and not day_closed:
        print(f"🚨 MTM LIMIT HIT ({total:.2f}) — FORCE EXIT ALL")
        square_off_all("FORCE EXIT MTM")


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

            # lot + leg pnl carried over (pnl stays overall for the day)
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
              f"leg pnl {leg['pnl']:.2f}")

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

    with state_lock:
        name = token_map.get(token)
        if not name:
            return

        leg = legs[name]

        ltp = float(msg.get("LTP") or 0)
        if ltp <= 0:
            return

        leg["ltp"] = ltp
        candle = leg["builder"].process_tick(msg)

        # ---- TIME EXIT ----
        if datetime.now(IST).time() >= TRADE_END:
            square_off_all("TIME EXIT")
            update_telemetry()
            return

        if leg["trading_disabled"] or leg["rolling"]:
            leg["prev_ltp"] = ltp
            update_telemetry()
            return

        exited = False

        # ---- CANDLE LOGIC (entry / candle close SL) ----
        if candle:
            print(f"🕯 {name} {candle}")
            exited = on_candle_close(leg, candle, ltp)

        # ---- TICK EXITS (target / pivot-8 SL) ----
        if leg["position"]:
            exited = check_tick_exits(leg, ltp) or exited

        # ---- PIVOT TOUCH (only when flat, not on the exit tick) ----
        if not leg["position"] and not exited:
            update_touch(leg, ltp)

        leg["prev_ltp"] = ltp

        # ---- STRIKE CHANGE (only when flat: > 280 or < 170) ----
        if not leg["position"] and in_trade_window():
            trigger = roll_trigger(ltp)
            if trigger:
                start_roll(leg, trigger)

        update_telemetry()
        check_mtm()


def on_tick(token, msg):
    if str(token) not in token_map:
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

for t in list(token_map.keys()):
    subscribe(t, on_tick)