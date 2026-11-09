"""
NIFTY OPTION BUYING - STANDARD DEVIATION + FIBONACCI LEVELS
============================================================

DAILY FLOW
----------
09:09  Daily volatility from NIFTY index (last 360 completed daily closes)
         r_t        = ln(close_t / close_t-1)
         daily_vol  = mean(r) + 1 * stdev(r)            (sample stdev, like Excel STDEV)
         option_vol = daily_vol * VOL_MULTIPLIER (100)  (e.g. 0.00815 -> 0.815 = 81.5%)

09:15  Option chain -> CE and PE whose LTP is nearest to OPTION_SELECTION_LTP (160)

Each leg, after its day OPEN is known:
         1SD range  = ROUNDUP(open * option_vol, 2)
         fib range  = ROUND(factor * 1SD, 2)
         level      = open + fib range              (15 upward levels)

ENTRY  (1 lot, only one open position per option at a time)
         a) tick : LTP >= level + TICK_TRIGGER_POINTS (8)
         b) 5-min: completed candle CLOSE > level
         A level only fires on a FRESH cross: after it fires it is disarmed and
         re-arms only when LTP trades back below it (e.g. after an SL hit).
         If several levels qualify at once, the highest one is used.

EXIT   priority: TIME -> SL -> TARGET
         SL     = level - SL_POINTS (20)
         TARGET = next level up (TGT-12 has none -> SL / time exit only)
         TIME   = all open trades closed at 15:14

Re-entry allowed any number of times, same rules on expiry day.
"""

import math
import os
import statistics
import threading
import time
from datetime import datetime, date, timedelta, time as dtime
from queue import Queue

import asyncio
import pytz
import requests
from dotenv import load_dotenv

from dhanhq import MarketFeed, DhanContext, dhanhq

from dhan_token import get_access_token
from find_security import load_fno_master, find_option_security
from find_instrument import FindInstrument
from signal_emitter import emit_signal



# ==========================================================
# STRATEGY CONFIG
# ==========================================================

STRATEGY_NAME = "NIFTY_SD_FIB_OPTION_BUYING"
COMMON_ID = "3f357707-f543-4491-8fbe-5e2e21922abc"          # <-- set the new strategy id
SYMBOL = "NIFTY"
LOTSIZE = 65
LOTS_PER_ENTRY = 1

NIFTY_INDEX_SECURITY_ID = 13

# --- Volatility ---
VOL_LOOKBACK_DAYS = 360          # number of daily log returns
VOL_SD_MULTIPLE = 1              # mean + 1 * SD
VOL_MULTIPLIER = 100             # index vol -> option vol (sheet: K8 = L30*100)

# --- Strike selection ---
OPTION_SELECTION_LTP = 160

# --- Levels ---
FIB_LEVELS = [
    ("BUY ABOVE",  0.2611),
    ("TGT-1",      0.382),
    ("TGT-2",      0.5),
    ("REV.ZONE 1", 0.618),
    ("REV.ZONE 2", 0.786),
    ("TGT-3",      1.0),
    ("TGT-4",      1.382),
    ("TGT-5",      1.618),
    ("TGT-6",      1.786),
    ("TGT-7",      2.0),
    ("TGT-8",      2.5),
    ("TGT-9",      3.0),
    ("TGT-10",     3.5),
    ("TGT-11",     4.0),
    ("TGT-12",     4.5),
]

# --- Entry / exit ---
TICK_TRIGGER_POINTS = 8
SL_POINTS = 20
CANDLE_MINUTES = 5

# --- Timings (IST) ---
VOL_CALC_TIME = dtime(9, 9)
STRIKE_SELECT_TIME = dtime(9, 15, 5)   # a few seconds after open so chain LTPs are live
TRADE_START = dtime(9, 15)
ENTRY_CUTOFF = dtime(15, 14)           # no new entries at/after this time
TRADE_END = dtime(15, 14)              # square off everything

IST = pytz.timezone("Asia/Kolkata")

NSE_HOLIDAYS = {
    date(2026, 1, 15), date(2026, 1, 26), date(2026, 3, 3), date(2026, 3, 26),
    date(2026, 3, 31), date(2026, 4, 3), date(2026, 4, 14), date(2026, 5, 1),
    date(2026, 5, 28), date(2026, 6, 26), date(2026, 9, 14), date(2026, 10, 2),
    date(2026, 10, 20), date(2026, 11, 10), date(2026, 11, 24), date(2026, 12, 25),
}

TRADE_LOG_URL = "https://algoapi.dreamintraders.in/api/paperlogger/event"
TELEMETRY_URL = "https://algoapi.dreamintraders.in/api/telemetry"
TRADELEG_URL = "https://algoapi.dreamintraders.in/api/tradelegs/create"
DEPLOYMENTS_URL = "https://algoapi.dreamintraders.in/api/deployments/today/{}"


# ==========================================================
# GLOBAL RUNTIME STATE (each declared once)
# ==========================================================

dhan = None
dhan_context = None
loop = asyncio.new_event_loop()
trade_log_queue = Queue()

combined_pnl = 0.0
legs = {}                 # security_id(str) -> leg state
today_str = datetime.now(IST).strftime("%Y-%m-%d")

telemetry = {
    "strategy_id": COMMON_ID,
    "run_id": COMMON_ID,
    "status": "ACTIVE",
    "pnl": 0,
    "pnl_percentage": 0,
    "ce_ltp": 0,
    "pe_ltp": 0,
    "ce_pnl": 0,
    "pe_pnl": 0,
}


# ==========================================================
# BACKGROUND WORKERS (async loop, trade log, telemetry)
# ==========================================================

def start_loop():
    asyncio.set_event_loop(loop)
    loop.run_forever()


def run_async(coro):
    try:
        if asyncio.iscoroutine(coro):
            asyncio.run_coroutine_threadsafe(coro, loop)
        else:
            print("❌ Not coroutine:", coro)
    except Exception as e:
        print("WS error:", e)


def trade_log_worker():
    while True:
        payload = trade_log_queue.get()
        try:
            requests.post(TRADE_LOG_URL, json=payload, timeout=2)
        except Exception as e:
            print("TRADE EVENT LOG ERROR:", e)
        finally:
            trade_log_queue.task_done()


def telemetry_broadcaster():
    numeric = ("pnl", "ce_pnl", "pe_pnl", "ce_ltp", "pe_ltp", "pnl_percentage")
    while True:
        try:
            payload = {}
            for k, v in telemetry.copy().items():
                if k in numeric:
                    try:
                        v = float(v)
                    except (TypeError, ValueError):
                        v = 0
                payload[k] = v
            res = requests.post(TELEMETRY_URL, json=payload, timeout=0.5)
            if res.status_code != 200:
                print("Telemetry failed:", res.status_code)
        except Exception as e:
            print("Telemetry error:", e)
        time.sleep(1)


# ==========================================================
# DEPLOYMENTS / LOGGING
# ==========================================================

def get_today_deployments():
    try:
        response = requests.get(DEPLOYMENTS_URL.format(COMMON_ID), timeout=10)
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


def logtradeleg(leg, symbol, strike_price, token):
    payload = {
        "strategy_id": COMMON_ID,
        "leg": leg,
        "symbol": symbol,
        "strike_price": strike_price,
        "date": today_str,
        "token": str(token),
    }
    try:
        response = requests.post(TRADELEG_URL, json=payload, timeout=5)
        if response.status_code in (200, 201):
            print(f"✅ Trade leg logged: {leg}")
            return response.json()
        print(f"❌ Failed to log trade leg: {response.status_code} {response.text}")
    except Exception as e:
        print(f"⚠️ Error while logging trade leg: {e}")
    return None


def log_trade_event(event_type, leg_name, token, side, lot, price, reason, pnl, cum_pnl):
    trade_log_queue.put({
        "run_id": COMMON_ID,
        "strategy_id": COMMON_ID,
        "trade_id": COMMON_ID,
        "event_type": event_type,
        "leg_name": leg_name,
        "token": int(token),
        "symbol": SYMBOL,
        "side": side,
        "lots": lot,
        "quantity": lot * LOTSIZE,
        "price": float(price),
        "reason": reason,
        "deployed_by": COMMON_ID,
        "pnl": str(pnl),
        "cum_pnl": str(cum_pnl),
    })


def build_payload(state, side, event_type, reason, price, pnl, cum_pnl, users):
    name = state["leg_name"]
    expiry_date = state["dhan_row"]["SM_EXPIRY_DATE"]
    symbol = (
        f"NIFTY{expiry_date.strftime('%d')}{expiry_date.strftime('%b').upper()}"
        f"{expiry_date.strftime('%y')}{int(float(state['strike']))}{name}"
    )
    return {
        "strategy_id": COMMON_ID,
        "users": users,
        "option": name,
        "side": side,
        "quantity": state["lot"] * LOTSIZE,
        "security_id": state["token"],
        "token": int(state["angel_row"]["token"]),
        "event_type": event_type,
        "leg_name": name,
        "symbol": symbol,
        "exchange": "NFO",
        "expiry": expiry_date.strftime("%Y-%m-%d"),
        "strike": int(float(state["strike"])),
        "price": price,
        "pnl": pnl,
        "cum_pnl": cum_pnl,
        "zebusymbol": "NIFTY",
        "is_ce": name == "CE",
        "is_fno": True,
        "antsymbol": "NIFTY",
        "reason": reason,
    }


def send_order(state, side, event_type, reason, price, trade_pnl):
    """Broker signal + trade log for one entry / exit."""
    users = group_users_by_broker(get_today_deployments())
    run_async(emit_signal(build_payload(
        state, side, event_type, reason.lower(), price, trade_pnl, combined_pnl, users
    )))
    log_trade_event(
        event_type=event_type,
        leg_name=state["leg_name"],
        token=state["token"],
        side=side,
        lot=state["lot"],
        price=price,
        reason=reason,
        pnl=state["pnl"],
        cum_pnl=combined_pnl,
    )


# ==========================================================
# MARKET CALENDAR / TIME HELPERS
# ==========================================================

def is_market_holiday(check_date):
    if isinstance(check_date, datetime):
        check_date = check_date.date()
    return check_date.weekday() >= 5 or check_date in NSE_HOLIDAYS


def now_ist():
    return datetime.now(IST)


def wait_until(target_time, label):
    print(f"⏳ Waiting for {label} ({target_time.strftime('%H:%M:%S')}) ...")
    while now_ist().time() < target_time:
        time.sleep(0.5)
    print(f"✅ {label} reached")


# ==========================================================
# STEP 1 : DAILY VOLATILITY (NIFTY INDEX)
# ==========================================================

def fetch_index_daily_closes(lookback_days=VOL_LOOKBACK_DAYS):
    """
    Returns the last (lookback_days + 1) COMPLETED daily closes of NIFTY,
    oldest -> newest. Today's (live) candle is excluded.
    """
    today = now_ist().date()
    from_date = today - timedelta(days=int(lookback_days * 1.6) + 30)  # calendar buffer

    data = dhan.historical_daily_data(
        security_id=str(NIFTY_INDEX_SECURITY_ID),
        exchange_segment="IDX_I",
        instrument_type="INDEX",
        from_date=from_date.strftime("%Y-%m-%d"),
        to_date=today.strftime("%Y-%m-%d"),
    )

    if not isinstance(data, dict) or data.get("status") != "success":
        raise RuntimeError(f"Daily history failed: {data}")

    raw = data["data"]
    rows = []
    for ts, close in zip(raw["timestamp"], raw["close"]):
        d = datetime.fromtimestamp(ts, IST).date()
        if d < today:
            rows.append((d, float(close)))

    rows.sort(key=lambda r: r[0])
    closes = [c for _, c in rows]

    needed = lookback_days + 1
    if len(closes) < needed:
        raise RuntimeError(f"Only {len(closes)} daily closes, need {needed}")

    print(f"Index closes: {rows[-needed][0]} -> {rows[-1][0]} ({needed} days)")
    return closes[-needed:]


def calculate_daily_volatility(closes):
    """closes oldest -> newest. Returns (mean, sd, daily_vol)."""
    returns = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes))]
    mean = sum(returns) / len(returns)
    sd = statistics.stdev(returns)                 # sample SD == Excel STDEV
    daily_vol = mean + VOL_SD_MULTIPLE * sd
    return mean, sd, daily_vol


def get_option_volatility():
    for attempt in range(1, 4):
        try:
            closes = fetch_index_daily_closes()
            mean, sd, daily_vol = calculate_daily_volatility(closes)
            option_vol = daily_vol * VOL_MULTIPLIER
            print("\n========== DAILY VOLATILITY ==========")
            print(f"Mean        : {mean:.8f}")
            print(f"Std Dev     : {sd:.8f}")
            print(f"Daily Vol   : {daily_vol:.8f}  ({daily_vol * 100:.4f}%)")
            print(f"Option Vol  : {option_vol:.6f}  ({option_vol * 100:.2f}%)")
            print("======================================\n")
            return option_vol
        except Exception as e:
            print(f"Volatility attempt {attempt} failed:", e)
            time.sleep(3)
    raise RuntimeError("Could not calculate daily volatility")


# ==========================================================
# STEP 2 : EXPIRY + STRIKE SELECTION
# ==========================================================

def get_next_expiry():
    expiries = dhan.expiry_list(
        under_security_id=NIFTY_INDEX_SECURITY_ID,
        under_exchange_segment="IDX_I",
    )
    return expiries["data"]["data"][0]


def select_nearest_ltp_options(oc, target_ltp=OPTION_SELECTION_LTP):
    """CE and PE whose LTP is nearest to target_ltp (above or below)."""
    best = {"ce": None, "pe": None}

    for strike, contracts in oc["data"]["data"]["oc"].items():
        strike = float(strike)
        for side in ("ce", "pe"):
            c = contracts.get(side) or {}
            ltp = float(c.get("last_price", 0) or 0)
            sid = c.get("security_id", 0)
            if not sid or ltp <= 0:
                continue
            diff = abs(ltp - target_ltp)
            cur = best[side]
            # equal distance (e.g. 155 vs 165) -> take the higher LTP
            if cur is None or diff < cur["diff"] or (diff == cur["diff"] and ltp > cur["ltp"]):
                best[side] = {"strike": strike, "security_id": str(sid), "ltp": ltp, "diff": diff}

    if best["ce"] is None or best["pe"] is None:
        raise RuntimeError("No valid CE/PE found in option chain")
    return best["ce"], best["pe"]


# ==========================================================
# STEP 3 : LEVELS
# ==========================================================

def round_up(value, decimals=2):
    factor = 10 ** decimals
    return math.ceil(round(value * factor, 6)) / factor


def build_levels(open_price, option_vol):
    one_sd = round_up(open_price * option_vol, 2)
    levels = []
    for name, factor in FIB_LEVELS:
        fib_range = round(factor * one_sd, 2)
        levels.append({
            "name": name,
            "factor": factor,
            "fib_range": fib_range,
            "price": round(open_price + fib_range, 2),
            "armed": True,
        })
    return one_sd, levels


def print_levels(state):
    print(f"\n========== {state['leg_name']} {int(state['strike'])} LEVELS ==========")
    print(f"Open: {state['open']}   Vol: {state['option_vol'] * 100:.2f}%   1SD: {state['one_sd']}")
    print(f"{'LEVEL':<12}{'FACTOR':>8}{'RANGE':>10}{'PRICE':>10}{'TRIGGER':>10}{'SL':>10}")
    for lv in state["levels"]:
        print(f"{lv['name']:<12}{lv['factor']:>8}{lv['fib_range']:>10.2f}{lv['price']:>10.2f}"
              f"{lv['price'] + TICK_TRIGGER_POINTS:>10.2f}{lv['price'] - SL_POINTS:>10.2f}")
    print("=" * 60 + "\n")


# ==========================================================
# LEG STATE
# ==========================================================

def init_state(leg_name, token, strike, dhan_row, angel_row, option_vol):
    return {
        "leg_name": leg_name,
        "token": str(token),
        "strike": strike,
        "dhan_row": dhan_row,
        "angel_row": angel_row,
        "lot": LOTS_PER_ENTRY,

        "option_vol": option_vol,
        "open": None,
        "one_sd": None,
        "levels": [],
        "ready": False,

        "position": False,
        "entry_price": None,
        "entry_level": None,      # index into levels
        "stoploss": None,
        "target": None,
        "entry_time": None,

        "pnl": 0.0,               # realised pnl of this leg
        "trades": 0,
        "last_ltp": None,
        "trading_disabled": False,
        "candle_builder": FiveMinuteCandleBuilder(),
    }


def setup_levels(state, open_price):
    state["open"] = float(open_price)
    state["one_sd"], state["levels"] = build_levels(state["open"], state["option_vol"])
    state["ready"] = True
    print_levels(state)


# ==========================================================
# 5-MIN CANDLE BUILDER (aligned to 09:15)
# ==========================================================

class FiveMinuteCandleBuilder:
    """
    process_tick(ltp, ts) -> returns the COMPLETED candle when a tick
    arrives in a new 5-min bucket, else None.
    """

    def __init__(self, minutes=CANDLE_MINUTES):
        self.minutes = minutes
        self.current = None

    def _bucket_start(self, ts):
        m = ts.hour * 60 + ts.minute
        start = (m // self.minutes) * self.minutes
        return ts.replace(hour=start // 60, minute=start % 60, second=0, microsecond=0)

    def process_tick(self, ltp, ts):
        start = self._bucket_start(ts)
        completed = None

        if self.current is not None and start != self.current["datetime"]:
            completed = self.current
            self.current = None

        if self.current is None:
            self.current = {"datetime": start, "open": ltp, "high": ltp, "low": ltp, "close": ltp}
        else:
            c = self.current
            c["high"] = max(c["high"], ltp)
            c["low"] = min(c["low"], ltp)
            c["close"] = ltp

        return completed


# ==========================================================
# CORE ENGINE
# ==========================================================

def rearm_levels(state, ltp):
    """A level re-arms once price trades back below it."""
    for lv in state["levels"]:
        if not lv["armed"] and ltp < lv["price"]:
            lv["armed"] = True


def enter_trade(state, level_idx, ltp, reason):
    lv = state["levels"][level_idx]
    stoploss = round(lv["price"] - SL_POINTS, 2)

    if ltp <= stoploss:
        print(f"{state['leg_name']} skip entry @ {ltp}: already at/below SL {stoploss}")
        return

    # disarm the triggered level and everything below it that price is above
    for i, l in enumerate(state["levels"]):
        if i <= level_idx and ltp >= l["price"]:
            l["armed"] = False
    lv["armed"] = False
    # remember the 5-min candle this level was traded in, so that same
    # candle's close cannot fire the level a second time (tick + close = 1 lot)
    current = state["candle_builder"].current
    lv["entered_bucket"] = current["datetime"] if current else None

    nxt = level_idx + 1
    state["position"] = True
    state["entry_price"] = ltp
    state["entry_level"] = level_idx
    state["stoploss"] = stoploss
    state["target"] = state["levels"][nxt]["price"] if nxt < len(state["levels"]) else None
    state["entry_time"] = now_ist()
    state["trades"] += 1

    full_reason = f"{reason} {lv['name']} ({lv['price']})"
    print(f"🟢 {state['leg_name']} BUY @ {ltp} | {full_reason} | "
          f"SL {state['stoploss']} | TGT {state['target']} | trade #{state['trades']}")

    send_order(state, "BUY", "ENTRY", full_reason, ltp, 0)


def exit_trade(state, ltp, reason):
    global combined_pnl

    trade_pnl = (ltp - state["entry_price"]) * LOTSIZE * state["lot"]
    state["pnl"] += trade_pnl
    combined_pnl += trade_pnl

    lv = state["levels"][state["entry_level"]]
    full_reason = f"{reason} [{lv['name']}]"
    print(f"🔴 {state['leg_name']} SELL @ {ltp} | {full_reason} | "
          f"PNL {trade_pnl:.2f} | LEG {state['pnl']:.2f} | TOTAL {combined_pnl:.2f}")

    send_order(state, "SELL", "EXIT", full_reason, ltp, trade_pnl)

    state["position"] = False
    state["entry_price"] = None
    state["entry_level"] = None
    state["stoploss"] = None
    state["target"] = None
    state["entry_time"] = None


def check_tick_entry(state, ltp):
    for idx in range(len(state["levels"]) - 1, -1, -1):
        lv = state["levels"][idx]
        if lv["armed"] and ltp >= lv["price"] + TICK_TRIGGER_POINTS:
            enter_trade(state, idx, ltp, f"TICK +{TICK_TRIGGER_POINTS}")
            return True
    return False


def check_candle_entry(state, candle, ltp):
    for idx in range(len(state["levels"]) - 1, -1, -1):
        lv = state["levels"][idx]
        if lv.get("entered_bucket") == candle["datetime"]:
            continue
        if lv["armed"] and candle["close"] > lv["price"]:
            enter_trade(state, idx, ltp, f"{CANDLE_MINUTES}M CLOSE {candle['close']} >")
            return True
    return False


def manage_positions(state, ltp, now):
    """Tick-level: TIME -> SL -> TARGET -> ENTRY."""
    if state["trading_disabled"] or not state["ready"]:
        return

    # ---------- TIME EXIT ----------
    if now >= TRADE_END:
        if state["position"]:
            exit_trade(state, ltp, "TIME EXIT")
        state["trading_disabled"] = True
        return

    rearm_levels(state, ltp)

    if state["position"]:
        # ---------- SL ----------
        if ltp <= state["stoploss"]:
            exit_trade(state, ltp, "STOP LOSS")
            rearm_levels(state, ltp)
            return
        # ---------- TARGET ----------
        if state["target"] is not None and ltp >= state["target"]:
            exit_trade(state, ltp, "TARGET")
            return
        return

    # ---------- ENTRY (tick) ----------
    if TRADE_START <= now < ENTRY_CUTOFF:
        check_tick_entry(state, ltp)


def on_candle_close(state, candle, ltp, now):
    c = candle
    print(f"🕯️ {state['leg_name']} {CANDLE_MINUTES}M {c['datetime'].strftime('%H:%M')} "
          f"O {c['open']} H {c['high']} L {c['low']} C {c['close']}")

    if state["trading_disabled"] or not state["ready"] or state["position"]:
        return
    if TRADE_START <= now < ENTRY_CUTOFF:
        check_candle_entry(state, candle, ltp)


def update_telemetry():
    pnl_by_leg = {}
    for st in legs.values():
        running = 0.0
        if st["position"] and st["last_ltp"] is not None:
            running = (st["last_ltp"] - st["entry_price"]) * LOTSIZE * st["lot"]
        pnl_by_leg[st["leg_name"]] = st["pnl"] + running
        telemetry[f"{st['leg_name'].lower()}_ltp"] = st["last_ltp"] or 0

    telemetry["ce_pnl"] = pnl_by_leg.get("CE", 0)
    telemetry["pe_pnl"] = pnl_by_leg.get("PE", 0)
    telemetry["pnl"] = telemetry["ce_pnl"] + telemetry["pe_pnl"]


def on_message(msg):
    if msg.get("type") != "Quote Data":
        return

    token = str(msg.get("security_id"))
    state = legs.get(token)
    if state is None:
        return

    ltp = float(msg.get("LTP", 0) or 0)
    if ltp <= 0:
        return

    now_dt = now_ist()
    now = now_dt.time()
    state["last_ltp"] = ltp

    # Levels are built from the option's day OPEN (from the quote packet)
    if not state["ready"]:
        day_open = float(msg.get("open", 0) or 0)
        if day_open <= 0:
            return
        setup_levels(state, day_open)

    candle = state["candle_builder"].process_tick(ltp, now_dt)

    manage_positions(state, ltp, now)

    if candle:
        on_candle_close(state, candle, ltp, now)

    update_telemetry()


def square_off_all_if_due():
    """Safety net: time exit even if a leg stops ticking."""
    if now_ist().time() < TRADE_END:
        return
    for st in legs.values():
        if st["trading_disabled"]:
            continue
        if st["position"] and st["last_ltp"] is not None:
            exit_trade(st, st["last_ltp"], "TIME EXIT")
        st["trading_disabled"] = True
    telemetry["status"] = "CLOSED"


# ==========================================================
# MAIN
# ==========================================================

def main():
    global dhan, dhan_context

    if is_market_holiday(now_ist()):
        print("Market holiday / weekend. Exiting.")
        return

    load_dotenv()
    access_token = get_access_token()
    dhan_context = DhanContext(os.getenv("CLIENT_ID"), access_token)
    dhan = dhanhq(dhan_context)

    fno_df = load_fno_master()
    finder = FindInstrument()

    threading.Thread(target=start_loop, daemon=True).start()
    threading.Thread(target=trade_log_worker, daemon=True).start()
    threading.Thread(target=telemetry_broadcaster, daemon=True).start()

    # ---------- 09:09 volatility ----------
    wait_until(VOL_CALC_TIME, "volatility calc")
    option_vol = get_option_volatility()

    # ---------- 09:15 strike selection ----------
    wait_until(STRIKE_SELECT_TIME, "strike selection")
    next_expiry = get_next_expiry()
    print("Next expiry:", next_expiry)

    oc = dhan.option_chain(
        under_security_id=NIFTY_INDEX_SECURITY_ID,
        under_exchange_segment="IDX_I",
        expiry=str(next_expiry),
    )
    ce, pe = select_nearest_ltp_options(oc)
    print(f"CE: {int(ce['strike'])} sid {ce['security_id']} ltp {ce['ltp']}")
    print(f"PE: {int(pe['strike'])} sid {pe['security_id']} ltp {pe['ltp']}")

    for leg_name, pick in (("CE", ce), ("PE", pe)):
        dhan_row = find_option_security(fno_df, pick["strike"], leg_name, today_str, SYMBOL)
        angel_row = finder.get_option(SYMBOL, int(pick["strike"]), leg_name)
        legs[pick["security_id"]] = init_state(
            leg_name, pick["security_id"], pick["strike"], dhan_row, angel_row, option_vol
        )
        logtradeleg(leg_name, f"NIFTY {leg_name} {int(pick['strike'])}",
                    str(int(pick["strike"])), pick["security_id"])

    # ---------- live feed ----------
    instruments = [(MarketFeed.NSE_FNO, sid, MarketFeed.Quote) for sid in legs]
    feed = MarketFeed(dhan_context, instruments, "v2")

    while True:
        try:
            feed.run_forever()
            msg = feed.get_data()
            if msg:
                on_message(msg)
            square_off_all_if_due()

            if all(st["trading_disabled"] for st in legs.values()):
                print(f"\n✅ Day done. CE {legs_pnl('CE'):.2f} | PE {legs_pnl('PE'):.2f} | "
                      f"TOTAL {combined_pnl:.2f}")
                time.sleep(3)   # let log queue / signals flush
                break
        except Exception as e:
            print("WS ERROR:", e)
            time.sleep(1)


def legs_pnl(leg_name):
    return sum(st["pnl"] for st in legs.values() if st["leg_name"] == leg_name)


if __name__ == "__main__":
    main()