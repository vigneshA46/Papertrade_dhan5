import time
import pytz
import requests
from datetime import datetime, time as dtime
from dotenv import load_dotenv
import os
from dhanhq import MarketFeed
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
from option_chain_cache import set_option_chain, get_option_chain


# =========================
# CONFIG
# =========================
trade_log_queue = Queue()
def trade_log_worker():
    while True:
        payload = trade_log_queue.get()
        try:
            requests.post(TRADE_LOG_URL, json=payload, timeout=2)
        except Exception as e:
            print("TRADE EVENT LOG ERROR:", e)
        finally:
            trade_log_queue.task_done()

ATM = None 
TRADE_LOG_URL = "https://algoapi.dreamintraders.in/api/paperlogger/event"
EVENT_LOG_URL = "https://algoapi.dreamintraders.in/api/paperlogger/paperlogger"

COMMON_ID = "2f95dbca-9602-4982-af2b-aa7bcd4aa509"
SYMBOL = "NIFTY"

load_dotenv()

STRATEGY_NAME = "NIFTY_OPTION_BUYING_50_reentry"
client_id = os.getenv("CLIENT_ID")
access_token = get_access_token()


IST = pytz.timezone("Asia/Kolkata")

TRADE_START = dtime(9, 20)
TRADE_OPTION = dtime(9, 25)
TRADE_END   = dtime(15, 20)

CE_TARGET_POINTS = 50
TARGET_POINTS = 50
PE_TARGET_POINTS = 50
LOTSIZE = 65

today = datetime.now(IST).strftime("%Y-%m-%d")


# =========================
# LOGIN
# =========================

dhan_context = DhanContext(client_id, access_token)
dhan = dhanhq(dhan_context)
fno_df = load_fno_master()

strategy_id = "2f95dbca-9602-4982-af2b-aa7bcd4aa509"

loop = asyncio.new_event_loop()

def start_loop():
    asyncio.set_event_loop(loop)
    loop.run_forever()

threading.Thread(target=start_loop, daemon=True).start()

def run_async(coro):
    try:
        if asyncio.iscoroutine(coro):
            asyncio.run_coroutine_threadsafe(coro, loop)
        else:
            print("❌ Not coroutine:", coro)
    except Exception as e:
        print("WS error: ", e)

def get_today_deployments():
    url = f"https://algoapi.dreamintraders.in/api/deployments/today/{strategy_id}"

    try:
        response = requests.get(url, timeout=10)

        # Raise error if status not 200
        response.raise_for_status()

        data = response.json()

        # 👉 store in variable (this is what you asked)
        user_deployments = data

        return user_deployments

    except requests.exceptions.RequestException as e:
        print("API Error:", e)
        return None

def group_users_by_broker(deployments):
    grouped = {}

    if not deployments:
        return grouped

    for d in deployments:

        if d["type"] == "paper":
            continue
        broker = d.get("broker_name")

        if not broker:
            continue

        if broker not in grouped:
            grouped[broker] = []

        grouped[broker].append(d)

    return grouped

def build_payload(name, side, token , reason,event_type,ltp,pnl,cum_pnl,lot,users,  strike):

    if name == "CE":
        row = AngelCE
    else:
        row = AngelPE

    expiry_date = ce_row["SM_EXPIRY_DATE"]

    day = expiry_date.strftime("%d")
    month = expiry_date.strftime("%b").upper()
    year = expiry_date.strftime("%y")

    symbol = f"NIFTY{day}{month}{year}{ATM}{name}"
    expiry = expiry_date.strftime("%Y-%m-%d")

    return {
        "strategy_id": COMMON_ID,
        "users": users,
        "option": name,
        "side": side,
        "quantity": lot * LOTSIZE,
        "security_id": token,
        "token": int(row["token"]),
        "event_type": event_type,
        "leg_name": name,
        "symbol": symbol,
        "exchange": "NFO",
        "expiry":expiry,
        "strike": strike,
        "price":ltp,
        "pnl":pnl,
        "cum_pnl":cum_pnl,
        "zebusymbol": "NIFTY",
        "is_ce": True if name == "CE" else False,
        "is_fno": True,
        "antsymbol": "NIFTY",
        "reason":reason
    }


# =========================
# HELPERS
# =========================

def logtradeleg(strategyid, leg, symbol, strike_price, date, token):
    url = "https://algoapi.dreamintraders.in/api/tradelegs/create"
    
    payload = {
        "strategy_id": strategyid,
        "leg": leg,
        "symbol": symbol,
        "strike_price": strike_price,
        "date": date,
        "token":str(token)
    }

    try:
        response = requests.post(url, json=payload)

        if response.status_code == 200 or response.status_code == 201:
            print("✅ Trade leg logged successfully")
            return response.json()
        else:
            print(f"❌ Failed to log trade leg: {response.status_code}")
            print(response.text)
            return None

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

    try:
        requests.post(EVENT_LOG_URL, json=payload, timeout=3)
    except Exception as e:
        print("EVENT LOG ERROR:", e)


def log_trade_event(
    event_type,   # ENTRY / EXIT
    leg_name,
    token,
    symbol,
    side,
    lot,
    price,
    reason,
    pnl,
    cum_pnl
        ):
    payload = {
        "run_id": COMMON_ID,
        "strategy_id": COMMON_ID,

        "trade_id": COMMON_ID,         # 🔥 VERY IMPORTANT
        "event_type": event_type,     # ENTRY / EXIT

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
        "cum_pnl":str(cum_pnl)
    }
   
    trade_log_queue.put(payload)

def wait_for_start():
    print("⏳ Waiting for market...")
    while True:
        if datetime.now(IST).time() >= TRADE_START:
            print("✅ Market Started")
            return
        time.sleep(1)


def wait_for_option():
    print("⏳ Waiting for market...")
    while True:
        if datetime.now(IST).time() >= TRADE_OPTION:
            print("✅ Market Started")
            return
        time.sleep(1)



def calculate_atm(price, step=50):
    return int(round(price / step) * step)

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


def get_second_candle_mark(security_id):

    today = datetime.now(IST).strftime("%Y-%m-%d")
   

    idx= dhan.intraday_minute_data(
        security_id=security_id,
        exchange_segment="NSE_FNO",
        instrument_type="OPTIDX",
        from_date=today,
        to_date=today,
        interval="5"
    )
    print("Today :",type(today),today)

    data = idx.get("data", {})
    closes = data.get("close", [])
    highs = data.get("high", [])
    lows = data.get("low", [])
    timestamps = data.get("timestamp", [])

    for i in range(len(timestamps)):
        ts = datetime.fromtimestamp(timestamps[i], IST)  

        if ts.hour == 9 and ts.minute == 20:
            high = float(highs[i])
            low = float(lows[i])
            print(f"📍 HIST MARK {security_id} @ high {high} , and low {low}")
            return high , low

    print("❌ 09:15 candle not found")
    return None



def telemetry_broadcaster():
    while True:
        try:
            # 🔥 COPY to avoid mutation issues
            payload = telemetry.copy()

            # 🔥 optional: sanitize (prevents TypeError)
            def safe_number(x):
                try:
                    return float(x)
                except:
                    return 0

            payload = {k: safe_number(v) if k in ["pnl","ce_pnl","pe_pnl","ce_ltp","pe_ltp","pnl_percentage"] else v
                for k, v in payload.items()}


            res = requests.post(
                "https://algoapi.dreamintraders.in/api/telemetry",
                json=payload,
                timeout=0.5   # 🔥 keep it LOW
            )

            # optional debug
            if res.status_code != 200:
                print("Telemetry failed:", res.status_code)

        except Exception as e:
            print("Telemetry error:", e)

        time.sleep(1)


t = threading.Thread(target=telemetry_broadcaster, daemon=True)
t.start()


def get_next_expiry():
    """
    Returns current/next NIFTY expiry date
    directly from Dhan expiry list API
    """

    expiries = dhan.expiry_list(
        under_security_id=13,
        under_exchange_segment="IDX_I"
    )

    expiry_list = expiries["data"]

    # first expiry is always nearest expiry
    next_expiry = expiry_list["data"][0]

    return next_expiry



def on_message(msg):

    if msg.get("type") != "Quote Data":
        return

    token = str(msg["security_id"])
    ltp = float(msg.get("LTP", 0))

    builder = builders.get(token)

    if not builder:
        return

    candle = builder.process_tick(msg)

    # =========================
    # TELEMETRY (REAL-TIME PnL)
    # =========================
    ce_running = 0
    pe_running = 0

    if ce_state["position"]:
        ce_running = (telemetry["ce_ltp"] - ce_state["entry_price"]) * LOTSIZE

    if pe_state["position"]:
        pe_running = (telemetry["pe_ltp"] - pe_state["entry_price"]) * LOTSIZE

    telemetry["ce_pnl"] = ce_state["pnl"] + ce_running
    telemetry["pe_pnl"] = pe_state["pnl"] + pe_running
    telemetry["pnl"] = telemetry["ce_pnl"] + telemetry["pe_pnl"]



    # ==========================================================
    # CE
    # ==========================================================

    if token == CE_ID:

        telemetry["ce_ltp"] = ltp
        on_tick_check(
            ce_state,
            ltp,
            "CE"
            )

        # Every completed 5-minute candle
        if candle:

            print("\n========== CE 1 MIN CANDLE ==========")
            print(candle)
            print("=====================================\n")
            check_candle_close_exit(
                ce_state,
                candle,
                "CE"
            )


    # ==========================================================
    # PE
    # ==========================================================

    elif token == PE_ID:

        telemetry["pe_ltp"] = ltp
        on_tick_check(
           pe_state,
            ltp,
            "PE"
            )

        # Every completed 5-minute candle
        if candle:

            print("\n========== PE 5 MIN CANDLE ==========")
            print(candle)
            print("=====================================\n")
            check_candle_close_exit(
                pe_state,
                candle,
                "PE"
            )

def on_tick_check(state, ltp, leg_name):

    token = CE_ID if leg_name == "CE" else PE_ID

    # =========================
    # DAY TARGET
    # =========================
    if telemetry["pnl"] >= 1500:

        print(f"🎯 DAY TARGET HIT | MTM: {telemetry['pnl']}")

        ce_state["trading_disabled"] = True
        pe_state["trading_disabled"] = True

        # =========================
        # EXIT CE
        # =========================
        if ce_state["position"]:

            print("🔴 CE EXIT | DAY TARGET")

            ce_state["position"] = False

            # Actual exit will be added here later

        # =========================
        # EXIT PE
        # =========================
        if pe_state["position"]:

            print("🔴 PE EXIT | DAY TARGET")

            pe_state["position"] = False

            # Actual exit will be added here later

        return "DAY_TARGET"


    # =========================
    # ENTRY
    # =========================
    if not state["trading_disabled"] and not state["position"]:

        if ltp > state["high"]:

            entry_price = ltp

            state["entry_price"] = entry_price
            state["entry_time"] = datetime.now(IST).isoformat()
            state["position"] = True

            deployments = get_today_deployments()
            users = group_users_by_broker(deployments)

            print("FORMATTED USERS:", users)

            print(
                f"🟢 BUY {leg_name} | "
                f"ENTRY: {entry_price}"
            )

            # =========================
            # SEND ENTRY SIGNAL
            # =========================

            run_async(
                emit_signal(
                    build_payload(
                        leg_name,
                        "BUY",
                        token,
                        "entry",
                        "ENTRY",
                        ltp,
                        state["pnl"],
                        telemetry["pnl"],
                        state["lot"],
                        users,
                        state["strike"]
                    )
                )
            )

            # =========================
            # TRADE EVENT LOG
            # =========================

            log_trade_event(
                event_type="ENTRY",
                leg_name=leg_name,
                token=token,
                symbol="NIFTY",
                side="BUY",
                lot=state["lot"],
                price=entry_price,
                reason="Trade opened",
                pnl=state["pnl"],
                cum_pnl=telemetry["pnl"]
            )

            # =========================
            # EVENT LOG
            # =========================

            log_event(
                f"{leg_name} BUY",
                token,
                "ENTRY_EXECUTED",
                entry_price,
                "Trade opened"
            )

            return "ENTRY"


    # =========================
    # TICK EXIT
    # =========================
    if state["position"]:

        if ltp < state["low"]:

            exit_price = ltp

            print(
                f"🔴 EXIT {leg_name} | "
                f"EXIT: {exit_price} < LOW: {state['low']}"
            )

            # Calculate this trade's P&L
            trade_pnl = (
                (exit_price - state["entry_price"])
                * state["lot"]
                * LOTSIZE
            )

            state["pnl"] += trade_pnl

            state["position"] = False

            deployments = get_today_deployments()
            users = group_users_by_broker(deployments)

            # =========================
            # SEND EXIT SIGNAL
            # =========================

            run_async(
                emit_signal(
                    build_payload(
                        leg_name,
                        "SELL",
                        token,
                        "low_touch",
                        "EXIT",
                        ltp,
                        state["pnl"],
                        telemetry["pnl"],
                        state["lot"],
                        users,
                        state["strike"]
                    )
                )
            )

            # =========================
            # TRADE EVENT LOG
            # =========================

            log_trade_event(
                event_type="EXIT",
                leg_name=leg_name,
                token=token,
                symbol="NIFTY",
                side="SELL",
                lot=state["lot"],
                price=exit_price,
                reason="Low touched",
                pnl=trade_pnl,
                cum_pnl=telemetry["pnl"]
            )

            # =========================
            # EVENT LOG
            # =========================

            log_event(
                f"{leg_name} SELL",
                token,
                "EXIT_EXECUTED",
                exit_price,
                "Low touched"
            )

            return "EXIT"

    return None


def check_candle_close_exit(state, candle, leg_name):

    if state["position"]:

        close = float(candle["close"])

        if close < state["high"]:

            print(
                f"🔴 {leg_name} CANDLE CLOSE EXIT | "
                f"CLOSE: {close} < HIGH: {state['high']}"
            )

            state["position"] = False
            return "EXIT"

    return None

next_expiry = get_next_expiry()


def init_state():
    return {
        "marked": None,
        "high": None,
        "low": None,
        "position": False,
        "trading_disabled": False,
        "entry_price": None,
        "entry_time": None,
        "lot": 1,
        "pnl": 0.0,
        "symbol": None,
        "rearm_required": False,
        "moment":0.0,
        "strike":None

    }

wait_for_start()


threading.Thread(target=trade_log_worker, daemon=True).start()


idx = dhan.intraday_minute_data(
    security_id=13,
    exchange_segment="IDX_I",
    instrument_type="INDEX",
    from_date=today,
    to_date=today
)

data = idx.get("data", {})

opens = data.get("open", [])
highs = data.get("high", [])
lows = data.get("low", [])
closes = data.get("close", [])
volumes = data.get("volume", [])
timestamps = data.get("timestamp", [])

opening_candles = []

for i in range(len(timestamps)):
    ts = datetime.fromtimestamp(timestamps[i], IST) 

    if ts.hour == 9 and 15 <= ts.minute <= 20:
        candle = {
            "timestamp": timestamps[i],
            "open": opens[i],
            "high": highs[i],
            "low": lows[i],
            "close": closes[i],
            "volume": volumes[i]
        }
        opening_candles.append(candle)

print("Opening candles:", opening_candles)

if opening_candles:
    atm_price = float(opening_candles[0]["close"])  
    ATM = calculate_atm(atm_price)
    print("📌 ATM:", ATM)

else:
    print("Waiting for 9:17 candle...")


atm = ATM


oc = dhan.option_chain(
    under_security_id=13,
    under_exchange_segment="IDX_I",
    expiry=str(next_expiry)  
)


option_data = oc["data"]["data"]["oc"]

target = 210

best_ce = None
best_pe = None

best_ce_ltp = float("inf")
best_pe_ltp = float("inf")


for strike, strike_data in option_data.items():

    strike = float(strike)

    # ================= CE =================
    # ONLY ATM OR ITM CE
    if strike <= atm and "ce" in strike_data:

        ce_ltp = strike_data["ce"]["last_price"]

        if ce_ltp >= target and ce_ltp < best_ce_ltp:

            best_ce_ltp = ce_ltp

            best_ce = {
                "strike": strike,
                "ltp": ce_ltp,
                "security_id": strike_data["ce"]["security_id"]
                }

    # ================= PE =================
    # ONLY ATM OR ITM PE
    # ================= PE =================
    
    if strike >= atm and "pe" in strike_data:

        pe_ltp = strike_data["pe"]["last_price"]

        if pe_ltp >= target and pe_ltp < best_pe_ltp:

            best_pe_ltp = pe_ltp

            best_pe = {
                "strike": strike,
                "ltp": pe_ltp,
                "security_id": strike_data["pe"]["security_id"]
            }    # FINAL VALUES

ce_strike = best_ce["strike"]
CE_ID = str(best_ce["security_id"])

pe_strike = best_pe["strike"]
PE_ID = str(best_pe["security_id"])


finder=FindInstrument()

ce_row = find_option_security(fno_df, ce_strike, "CE", today, "NIFTY")
pe_row = find_option_security(fno_df, pe_strike, "PE", today, "NIFTY")

AngelCE = finder.get_option("NIFTY" , int(ce_strike) , "CE")
AngelPE = finder.get_option("NIFTY" , int(pe_strike) , "PE")

print("angel tokens" , AngelCE , AngelPE)


print("📌 CE:", CE_ID)
print("📌 PE:", PE_ID)

builders = {
    CE_ID: OneMinuteCandleBuilder(),
    PE_ID: OneMinuteCandleBuilder()
}

# Log CE leg
logtradeleg(
    COMMON_ID,
    "CE",
    f"NIFTY CE {ce_strike}",
    str(ce_strike),
    str(today),
    CE_ID
)

# Log PE leg
logtradeleg(
    COMMON_ID,
    "PE",
    f"NIFTY PE {pe_strike}",
    str(pe_strike),
    str(today),
    PE_ID
)




# =========================
# STATE
# =========================

ce_state = init_state()
pe_state = init_state()

ce_state["strike"] = float(ce_strike)
pe_state["strike"] = float(pe_strike)

combined_pnl=0

wait_for_option()


ce_state["high"], ce_state["low"] = get_second_candle_mark(CE_ID)
pe_state["high"], pe_state["low"] = get_second_candle_mark(PE_ID)



instruments = [
    (MarketFeed.NSE_FNO, str(CE_ID), MarketFeed.Quote),
    (MarketFeed.NSE_FNO, str(PE_ID), MarketFeed.Quote)
]

feed = MarketFeed(dhan_context, instruments, "v2")

TOKENS = [
  str(CE_ID) , str(PE_ID)
]

while True:
    try:

        feed.run_forever()
        msg = feed.get_data()

        if msg:
            on_message(msg)

    except Exception as e:
        print("WS ERROR:", e)
        feed.run_forever()
        