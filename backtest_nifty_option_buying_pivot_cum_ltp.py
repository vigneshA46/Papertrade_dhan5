"""
BACKTEST — nifty_option_buying_pivot_cum_ltp (Option pivot)

    python backtest_nifty_option_buying_pivot_cum_ltp.py 2026-09-01 2026-10-28
    python backtest_nifty_option_buying_pivot_cum_ltp.py 2026-10-21 2026-10-21 --quiet

Data: Dhan "Expired Options Data" API (/v2/charts/rollingoption), 1-minute bars,
ATM-10 .. ATM+10 for CE and PE, nearest weekly expiry (expiryCode 1). Dhan allows
only ONE day per request, so every day costs 42 calls (+42 for the next expiry
when the previous day was an expiry day). Everything is cached in backtest_cache/,
so re-runs are instant.

Simulation (same rules as the live file, tick logic copied 1:1):
  * every 1-min bar becomes a tick path  O -> L -> H -> C  (green bar)
                                     or  O -> H -> L -> C  (red bar)
  * inside each leg of that path, extra ticks are injected at every price level the
    strategy reacts to (pivots, pivot+12, pivot-8, side target, MTM limit), so
    tick entries / exits fill AT the level, like a real tick stream would
  * candle-close entries / exits fill at the NEXT bar's open (= first tick of the
    new minute, exactly how the live candle builder fires)
  * 09:17 selection uses the 09:16 bar close as the chain LTP
  * strike change uses the chain at that minute's close, new strike trades from
    the next minute
  * previous-day pivots: H / L from the contract's previous-day 1-min bars,
    C = volume-weighted close of 15:00-15:29 (approximation of NSE close)
"""

import os
import sys
import json
import time
import argparse
from datetime import datetime, date, timedelta, time as dtime, timezone

import requests
from dotenv import load_dotenv


# =========================
# CONFIG (same as live)
# =========================

TRADE_START = dtime(9, 17)
TRADE_END   = dtime(15, 14)

TARGET_LTP       = 180
ROLL_UPPER       = 280
ROLL_LOWER       = 170
PICK_MIN         = 180
PICK_MAX         = 280
MAX_ROLL_STEPS   = 20

CUM_TARGET_START_POINTS = 50
CUM_TARGET_STEP_POINTS  = 50
SL_BUFFER         = 8
TICK_ENTRY_POINTS = 12
BASE_LOT          = 2
MAX_LOT           = 15
MAX_LOSS          = -13000
LOTSIZE           = 65

REARM_AFTER_TARGET    = True
MAX_STRIKE_CANDIDATES = 6

# =========================
# DATA CONFIG
# =========================

ROLLING_URL   = "https://api.dhan.co/v2/charts/rollingoption"
NIFTY_ID      = 13
OFFSETS       = range(-10, 11)
CALL_GAP      = 0.22          # seconds between API calls (Dhan data API ~5 req/s)
EXPIRY_TV_MAX = 25.0          # lowest ATM straddle time value 15:15-15:29 below this -> expiry day
TICK_EPS      = 0.05          # NSE option tick size

IST = timezone(timedelta(hours=5, minutes=30))
BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(BASE_DIR, "backtest_cache")


# =========================
# DHAN DATA
# =========================

class DhanData:
    def __init__(self, refresh=False):
        load_dotenv(os.path.join(BASE_DIR, ".env"))
        self.refresh = refresh
        self.headers = None
        self._last_call = 0.0
        self.calls = 0
        os.makedirs(CACHE_DIR, exist_ok=True)

    def _auth(self):
        if self.headers:
            return
        token = os.getenv("DHAN_ACCESS_TOKEN")
        if not token:
            sys.path.insert(0, BASE_DIR)
            from dhan_token import get_access_token
            token = get_access_token()
        self.headers = {
            "access-token": token,
            "client-id": os.getenv("CLIENT_ID") or "",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _post(self, body):
        self._auth()
        for attempt in range(6):
            gap = CALL_GAP - (time.time() - self._last_call)
            if gap > 0:
                time.sleep(gap)
            self._last_call = time.time()
            self.calls += 1
            try:
                r = requests.post(ROLLING_URL, json=body, headers=self.headers, timeout=30)
            except requests.RequestException as e:
                print(f"   ⚠️ network error {e} — retrying")
                time.sleep(2 * (attempt + 1))
                continue
            if r.status_code == 429:
                time.sleep(1.5 * (attempt + 1))
                continue
            if r.status_code != 200:
                if attempt < 2:
                    time.sleep(2)
                    continue
                raise RuntimeError(f"Dhan rollingoption {r.status_code}: {r.text[:200]} | body {body}")
            return r.json().get("data") or {}
        raise RuntimeError("Dhan rollingoption: too many retries")

    def _fetch_series(self, day, code, offset, opt):
        strike = "ATM" if offset == 0 else f"ATM{offset:+d}"
        body = {
            "exchangeSegment": "NSE_FNO",
            "interval": "1",
            "securityId": NIFTY_ID,
            "instrument": "OPTIDX",
            "expiryFlag": "WEEK",
            "expiryCode": code,
            "strike": strike,
            "drvOptionType": opt,
            "requiredData": ["open", "high", "low", "close", "volume", "strike", "spot"],
            "fromDate": day.isoformat(),
            "toDate": (day + timedelta(days=1)).isoformat(),
        }
        data = self._post(body)
        s = data.get("ce") or data.get("pe") or {}
        rows = []
        for i, ts in enumerate(s.get("timestamp") or []):
            t = datetime.fromtimestamp(ts, IST)
            if t.date() != day:
                continue          # Dhan may also return the next day
            rows.append((t.strftime("%H:%M"), float(s["strike"][i]),
                         float(s["open"][i]), float(s["high"][i]), float(s["low"][i]),
                         float(s["close"][i]), int(s["volume"][i] or 0), float(s["spot"][i] or 0)))
        return rows

    def day(self, day, code=1):
        """
        {"holiday": bool, "spot": {hhmm: spot}, "CE": {strike: {hhmm: [o,h,l,c,v]}}, "PE": {...}}
        """
        path = os.path.join(CACHE_DIR, f"{day.isoformat()}_c{code}.json")
        if not self.refresh and os.path.exists(path):
            with open(path) as f:
                return _decode_day(json.load(f))

        if day.weekday() >= 5:
            return {"holiday": True}

        # ATM CALL first: no rows for this date -> market holiday
        probe = self._fetch_series(day, code, 0, "CALL")
        if not probe:
            out = {"holiday": True}
        else:
            out = {"holiday": False, "spot": {}, "CE": {}, "PE": {}}
            for opt, side in (("CALL", "CE"), ("PUT", "PE")):
                for off in OFFSETS:
                    if side == "CE" and off == 0:
                        rows = probe
                    else:
                        rows = self._fetch_series(day, code, off, opt)
                    for hhmm, strike, o, h, l, c, v, spot in rows:
                        out[side].setdefault(strike, {})[hhmm] = [o, h, l, c, v]
                        if spot:
                            out["spot"][hhmm] = spot

        if day < datetime.now(IST).date():
            with open(path, "w") as f:
                json.dump(_encode_day(out), f)
        return out


def _encode_day(d):
    if d.get("holiday"):
        return d
    return {"holiday": False, "spot": d["spot"],
            "CE": {str(k): v for k, v in d["CE"].items()},
            "PE": {str(k): v for k, v in d["PE"].items()}}


def _decode_day(d):
    if d.get("holiday"):
        return d
    return {"holiday": False, "spot": d["spot"],
            "CE": {float(k): v for k, v in d["CE"].items()},
            "PE": {float(k): v for k, v in d["PE"].items()}}


def is_expiry_day(dd):
    """Nearest-expiry ATM straddle has ~zero time value at the close only on its expiry day."""
    if dd.get("holiday"):
        return False
    # spot and option prints are not perfectly aligned near the close -> take the
    # lowest time value seen 15:15-15:29 (expiry day ~0-10, any other day 60+)
    both = set(dd["CE"]) & set(dd["PE"])
    tvs = []
    for m in range(15, 30):
        hhmm = f"15:{m:02d}"
        spot = dd["spot"].get(hhmm)
        if not spot or not both:
            continue
        strike = min(both, key=lambda k: abs(k - spot))
        ce = dd["CE"][strike].get(hhmm)
        pe = dd["PE"][strike].get(hhmm)
        if ce and pe:
            tvs.append(ce[3] + pe[3] - abs(spot - strike))
    return bool(tvs) and min(tvs) < EXPIRY_TV_MAX


def prev_day_ohlc(bars):
    """Daily H / L / C of one strike from its 1-min bars (C ~ NSE closing price)."""
    if not bars:
        return None
    keys = sorted(bars)
    last30 = [bars[k] for k in keys if "15:00" <= k <= "15:29"]
    if not last30:
        return None
    vol = sum(b[4] for b in last30)
    if vol > 0:
        close = sum(b[3] * b[4] for b in last30) / vol
    else:
        close = last30[-1][3]
    return {
        "high": max(b[1] for b in bars.values()),
        "low": min(b[2] for b in bars.values()),
        "close": round(close, 2),
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
    levels = sorted(
        [(k, round(v, 2)) for k, v in pivots.items() if v > 0],
        key=lambda x: x[1]
    )
    return pivots, levels


# =========================
# ONE DAY SIMULATION
# =========================

class DaySim:
    def __init__(self, day, today_data, prev_data):
        self.day = day
        self.data = today_data          # nearest expiry, today
        self.prev = prev_data           # SAME contracts, previous trading day
        self.legs = {"CE": self._init_leg("CE"), "PE": self._init_leg("PE")}
        self.day_closed = False
        self.close_reason = None
        self.trades = []
        self.events = []
        self.now = None

    # ---------- leg state (mirrors live init_leg) ----------

    def _init_leg(self, name):
        return {
            "name": name, "strike": None, "pivots": {}, "levels": [], "ohlc": None,
            "armed_pivot": None, "prev_ltp": None, "skip_first_candle": True,
            "position": False, "entry_price": None, "entry_time": None, "entry_pivot": None,
            "entry_reason": None,
            "lot": BASE_LOT, "pnl": 0.0, "target_points": CUM_TARGET_START_POINTS, "ltp": 0.0,
            "trading_disabled": False, "rolling": False, "pending": None, "last_bar": None,
            "last_roll_fail": None, "strikes_used": [],
            "max_lot": BASE_LOT,
        }

    def log(self, msg):
        self.events.append(f"{self.now}  {msg}")

    # ---------- selection ----------

    def build_selection(self, side, strike, ltp):
        ohlc = prev_day_ohlc(self.prev[side].get(strike))
        if not ohlc:
            return None
        pivots, levels = calculate_pivots(ohlc)
        return {"strike": strike, "ltp": ltp, "ohlc": ohlc, "pivots": pivots, "levels": levels}

    def chain_rows(self, side, hhmm):
        rows = []
        for strike, bars in self.data[side].items():
            b = bars.get(hhmm)
            if b and b[3] > 0:
                rows.append((strike, b[3]))
        rows.sort()
        return rows

    def select_initial_strike(self, side):
        rows = self.chain_rows(side, "09:16")
        rows.sort(key=lambda r: abs(r[1] - TARGET_LTP))
        for strike, ltp in rows[:MAX_STRIKE_CANDIDATES]:
            sel = self.build_selection(side, strike, ltp)
            if sel:
                return sel
        return None

    @staticmethod
    def roll_trigger(ltp):
        if ltp > ROLL_UPPER:
            return "HIGH"
        if ltp < ROLL_LOWER:
            return "LOW"
        return None

    @staticmethod
    def roll_direction(side, trigger):
        if side == "CE":
            return -1 if trigger == "LOW" else +1
        return +1 if trigger == "LOW" else -1

    def select_roll_strike(self, side, current_strike, trigger, hhmm):
        rows = self.chain_rows(side, hhmm)
        strikes = [r[0] for r in rows]
        direction = self.roll_direction(side, trigger)
        if direction > 0:
            idx = next((i for i, s in enumerate(strikes) if s > current_strike), len(strikes))
        else:
            idx = max([i for i, s in enumerate(strikes) if s < current_strike], default=-1)

        steps = 0
        while 0 <= idx < len(rows) and steps < MAX_ROLL_STEPS:
            strike, ltp = rows[idx]
            steps += 1
            if PICK_MIN < ltp < PICK_MAX:
                sel = self.build_selection(side, strike, ltp)
                if sel:
                    return sel
            elif trigger == "LOW" and ltp >= PICK_MAX:
                break
            elif trigger == "HIGH" and ltp <= PICK_MIN:
                break
            idx += direction
        return None

    def apply_contract(self, leg, sel):
        leg["strike"] = sel["strike"]
        leg["pivots"] = sel["pivots"]
        leg["levels"] = sel["levels"]
        leg["ohlc"] = sel["ohlc"]
        leg["armed_pivot"] = None
        leg["prev_ltp"] = None
        leg["ltp"] = sel["ltp"]
        leg["last_bar"] = None
        leg["strikes_used"].append(int(sel["strike"]))
        lv = " | ".join(f"{k}:{v}" for k, v in leg["levels"])
        self.log(f"📌 {leg['name']} {int(sel['strike'])} LTP {sel['ltp']} | prev {sel['ohlc']} | {lv}")

    # ---------- pnl ----------

    @staticmethod
    def running_pnl(leg):
        if not leg["position"]:
            return 0.0
        return (leg["ltp"] - leg["entry_price"]) * LOTSIZE * leg["lot"]

    def realized_total(self):
        return self.legs["CE"]["pnl"] + self.legs["PE"]["pnl"]

    # ---------- orders ----------

    def enter_position(self, leg, price, pivot, reason=None):
        leg["position"] = True
        leg["entry_price"] = price
        leg["entry_time"] = self.now
        leg["entry_pivot"] = pivot
        leg["entry_reason"] = reason or f"Candle close > {pivot[0]} {pivot[1]}"
        leg["max_lot"] = max(leg["max_lot"], leg["lot"])
        self.log(f"🟢 BUY {leg['name']} {int(leg['strike'])} @ {price:.2f} lot {leg['lot']} | {leg['entry_reason']}")

    def exit_position(self, leg, price, reason, kind):
        trade_pnl = (price - leg["entry_price"]) * LOTSIZE * leg["lot"]
        leg["pnl"] += trade_pnl
        self.trades.append({
            "side": leg["name"], "strike": int(leg["strike"]), "lot": leg["lot"],
            "entry_time": leg["entry_time"], "entry": leg["entry_price"],
            "exit_time": self.now, "exit": price,
            "pivot": f"{leg['entry_pivot'][0]} {leg['entry_pivot'][1]}",
            "entry_reason": leg["entry_reason"], "exit_reason": reason, "kind": kind,
            "pnl": trade_pnl,
        })
        self.log(f"🔴 EXIT {leg['name']} {int(leg['strike'])} @ {price:.2f} | {reason} | {trade_pnl:.0f}")

        leg["position"] = False
        leg["entry_price"] = None
        leg["entry_time"] = None
        leg["entry_pivot"] = None

        if kind == "SL" or (kind == "TARGET" and REARM_AFTER_TARGET):
            leg["armed_pivot"] = None

        if kind == "TARGET":
            leg["lot"] = BASE_LOT
            leg["target_points"] += CUM_TARGET_STEP_POINTS
            while leg["target_points"] * LOTSIZE <= leg["pnl"]:
                leg["target_points"] += CUM_TARGET_STEP_POINTS
        elif kind == "SL":
            leg["lot"] = min(leg["lot"] + 1, MAX_LOT)

    def square_off_all(self, reason):
        if self.day_closed:
            return
        for leg in self.legs.values():
            if leg["position"]:
                self.exit_position(leg, leg["ltp"], reason, "FORCE")
            leg["trading_disabled"] = True
        self.day_closed = True
        self.close_reason = reason

    # ---------- strategy (mirrors live) ----------

    def in_trade_window(self):
        t = dtime(*map(int, self.now.split(":")))
        return TRADE_START <= t < TRADE_END

    def on_candle_close(self, leg, candle, ltp):
        o, h, l, c = candle[:4]
        if leg["position"]:
            pivot = leg["entry_pivot"]
            if c < pivot[1]:
                self.exit_position(leg, ltp, f"Candle close {c} < {pivot[0]} {pivot[1]}", "SL")
                return True
            return False

        if leg["skip_first_candle"]:
            leg["skip_first_candle"] = False
            return False

        pivot = leg["armed_pivot"]
        if pivot is None or not self.in_trade_window():
            return False

        avg = (o + h + l + c) / 4
        level = pivot[1]
        if c > level and avg > level and avg < c:
            self.enter_position(leg, ltp, pivot)
        return False

    def check_tick_exits(self, leg, ltp):
        target_rs = leg["target_points"] * LOTSIZE
        side_total = leg["pnl"] + self.running_pnl(leg)
        if side_total >= target_rs - 1e-6:
            self.exit_position(leg, ltp, f"Side target ₹{target_rs:.0f} (side pnl {side_total:.0f})", "TARGET")
            return True
        pivot = leg["entry_pivot"]
        if ltp <= pivot[1] - SL_BUFFER + 1e-9:
            self.exit_position(leg, ltp, f"Tick {ltp:.2f} <= {pivot[0]} {pivot[1]} - {SL_BUFFER}", "SL")
            return True
        return False

    def check_tick_entry(self, leg, ltp):
        pivot = leg["armed_pivot"]
        if pivot is None or not self.in_trade_window():
            return False
        if ltp >= pivot[1] + TICK_ENTRY_POINTS - 1e-9:
            self.enter_position(leg, ltp, pivot, f"Tick >= {pivot[0]} {pivot[1]} + {TICK_ENTRY_POINTS}")
            return True
        return False

    def update_touch(self, leg, ltp):
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

    def check_mtm(self):
        total = self.realized_total() + self.running_pnl(self.legs["CE"]) + self.running_pnl(self.legs["PE"])
        if total <= MAX_LOSS + 1e-6 and not self.day_closed:
            self.log(f"🚨 MTM LIMIT HIT ({total:.0f})")
            self.square_off_all("MTM -13000")

    def on_tick(self, leg, ltp, candle=None):
        """One tick of one leg — same order as live on_message."""
        leg["ltp"] = ltp

        if dtime(*map(int, self.now.split(":"))) >= TRADE_END:
            self.square_off_all("TIME EXIT 15:14")
            return

        if leg["trading_disabled"] or leg["rolling"]:
            leg["prev_ltp"] = ltp
            return

        exited = False
        if candle:
            exited = self.on_candle_close(leg, candle, ltp)

        if leg["position"]:
            exited = self.check_tick_exits(leg, ltp) or exited

        if not leg["position"] and not exited:
            self.update_touch(leg, ltp)

        if not leg["position"] and not exited:
            self.check_tick_entry(leg, ltp)

        leg["prev_ltp"] = ltp

        if not leg["position"] and self.in_trade_window():
            trigger = self.roll_trigger(ltp)
            if trigger:
                self.start_roll(leg, trigger)

        self.check_mtm()

    # ---------- strike change ----------

    def start_roll(self, leg, trigger):
        if leg["rolling"] or leg["last_roll_fail"] == self.now:
            return
        sel = self.select_roll_strike(leg["name"], leg["strike"], trigger, self.now)
        if not sel:
            leg["last_roll_fail"] = self.now          # live retries after 15s -> next minute
            self.log(f"⚠️ {leg['name']} {int(leg['strike'])} LTP {leg['ltp']:.2f} roll {trigger}: no strike in range")
            return
        leg["rolling"] = True
        leg["pending"] = sel
        self.log(f"🔁 {leg['name']} {int(leg['strike'])} LTP {leg['ltp']:.2f} -> {int(sel['strike'])} (LTP {sel['ltp']})")

    def finish_roll(self, leg):
        sel = leg["pending"]
        leg["pending"] = None
        leg["rolling"] = False
        if leg["position"] or leg["trading_disabled"] or self.day_closed:
            return
        self.apply_contract(leg, sel)
        # first candle of the new contract is complete in 1-min data -> no skip
        leg["skip_first_candle"] = False

    # ---------- tick path ----------

    def level_candidates(self, leg):
        lv = [v for _, v in leg["levels"]]
        if leg["position"]:
            pivot = leg["entry_pivot"]
            lv.append(pivot[1] - SL_BUFFER)
            remaining = leg["target_points"] * LOTSIZE - leg["pnl"]
            lv.append(leg["entry_price"] + remaining / (LOTSIZE * leg["lot"]))
            other = self.legs["PE" if leg["name"] == "CE" else "CE"]
            room = MAX_LOSS - self.realized_total() - self.running_pnl(other)
            lv.append(leg["entry_price"] + room / (LOTSIZE * leg["lot"]))
        else:
            if leg["armed_pivot"]:
                lv.append(leg["armed_pivot"][1] + TICK_ENTRY_POINTS)
            lv.append(ROLL_UPPER + TICK_EPS)
            lv.append(ROLL_LOWER - TICK_EPS)
        return lv

    def walk(self, leg, a, b):
        """Continuous move a -> b: ticks at every level the strategy cares about, then b."""
        cur = a
        while True:
            if leg["trading_disabled"] or leg["rolling"] or self.day_closed:
                return
            if b > cur:
                nxt = [x for x in self.level_candidates(leg) if cur < x < b]
                step = min(nxt) if nxt else b
            elif b < cur:
                nxt = [x for x in self.level_candidates(leg) if b < x < cur]
                step = max(nxt) if nxt else b
            else:
                return
            self.on_tick(leg, round(step, 4))
            cur = step
            if step == b:
                return

    def run_bar(self, leg, hhmm, bar):
        o, h, l, c = bar[:4]
        candle = leg["last_bar"]
        leg["last_bar"] = bar
        # first tick of the minute closes the previous candle
        self.on_tick(leg, o, candle)
        path = [o, l, h, c] if c >= o else [o, h, l, c]
        for a, b in zip(path, path[1:]):
            self.walk(leg, a, b)

    # ---------- day ----------

    def run(self):
        self.now = "09:17"
        for side in ("CE", "PE"):
            sel = self.select_initial_strike(side)
            if not sel:
                return f"no {side} strike near {TARGET_LTP} with prev-day data"
            self.apply_contract(self.legs[side], sel)

        minutes = []
        t = datetime.combine(self.day, TRADE_START)
        while t.time() <= dtime(15, 29):
            minutes.append(t.strftime("%H:%M"))
            t += timedelta(minutes=1)

        for hhmm in minutes:
            self.now = hhmm
            for side in ("CE", "PE"):
                leg = self.legs[side]
                if leg["pending"]:
                    self.finish_roll(leg)
                bar = self.data[side].get(leg["strike"], {}).get(hhmm)
                if bar:
                    self.run_bar(leg, hhmm, bar)
                elif hhmm >= "15:14":
                    self.on_tick(leg, leg["ltp"])
            if self.day_closed:
                break

        if not self.day_closed:
            self.square_off_all("TIME EXIT 15:14")
        return None


# =========================
# RUNNER
# =========================

def daterange(a, b):
    d = a
    while d <= b:
        yield d
        d += timedelta(days=1)


def prev_trading_day(dh, day):
    d = day - timedelta(days=1)
    for _ in range(12):
        if d.weekday() < 5 and not dh.day(d, 1).get("holiday"):
            return d
        d -= timedelta(days=1)
    return None


def inr(x):
    sign = "-" if x < 0 else ""
    return f"{sign}₹{abs(x):,.0f}"


def print_day(day, sim, expiry_today, quiet):
    ce, pe = sim.legs["CE"], sim.legs["PE"]
    tag = "  [EXPIRY DAY]" if expiry_today else ""
    print(f"\n━━━━━━━━ {day.isoformat()} {day.strftime('%a')}{tag} ━━━━━━━━")
    for leg in (ce, pe):
        lv = " ".join(f"{k}:{v}" for k, v in leg["levels"])
        print(f"  {leg['name']} strikes {' -> '.join(map(str, leg['strikes_used']))} | first pivots {lv}"
              if leg["strikes_used"] else f"  {leg['name']} -")
    if quiet:
        pass
    elif sim.trades:
        print(f"  {'#':>2} {'side':4} {'strike':>6} {'lot':>3} {'in':>5} {'entry':>8} {'out':>5} {'exit':>8} {'pnl':>10}  exit reason")
        for i, t in enumerate(sim.trades, 1):
            print(f"  {i:>2} {t['side']:4} {t['strike']:>6} {t['lot']:>3} {t['entry_time']:>5} {t['entry']:>8.2f} "
                  f"{t['exit_time']:>5} {t['exit']:>8.2f} {inr(t['pnl']):>10}  {t['kind']}: {t['exit_reason']}")
    else:
        print("  no trades")
    total = ce["pnl"] + pe["pnl"]
    print(f"  ➜ CE {inr(ce['pnl'])} | PE {inr(pe['pnl'])} | DAY {inr(total)} | trades {len(sim.trades)} | "
          f"closed by {sim.close_reason}")


def print_summary(results):
    days = [r for r in results if r["status"] == "ok"]
    if not days:
        print("\nNo traded days.")
        return

    print("\n\n════════════════════════ DAY-WISE SUMMARY ════════════════════════")
    print(f"  {'date':10} {'day':3} {'CE':>10} {'PE':>10} {'DAY':>10} {'cum':>11} {'trd':>4} {'tgt':>4} {'sl':>3}  close")
    cum = 0.0
    curve = []
    for r in days:
        cum += r["total"]
        curve.append(cum)
        print(f"  {r['date']} {r['dow']:3} {inr(r['ce']):>10} {inr(r['pe']):>10} {inr(r['total']):>10} {inr(cum):>11} "
              f"{r['n']:>4} {r['targets']:>4} {r['sls']:>3}  {r['close']}{'  *exp' if r['expiry'] else ''}")
    for r in results:
        if r["status"] != "ok":
            print(f"  {r['date']} {r['dow']:3}  -- {r['status']}")

    trades = [t for r in days for t in r["trades"]]
    ce_tot = sum(r["ce"] for r in days)
    pe_tot = sum(r["pe"] for r in days)
    total = ce_tot + pe_tot
    win_days = [r for r in days if r["total"] > 0]
    loss_days = [r for r in days if r["total"] < 0]
    wins = [t["pnl"] for t in trades if t["pnl"] > 0]
    losses = [t["pnl"] for t in trades if t["pnl"] <= 0]
    peak, max_dd = 0.0, 0.0
    for v in curve:
        peak = max(peak, v)
        max_dd = min(max_dd, v - peak)
    streak = worst_streak = 0
    for r in days:
        streak = streak + 1 if r["total"] < 0 else 0
        worst_streak = max(worst_streak, streak)
    best = max(days, key=lambda r: r["total"])
    worst = min(days, key=lambda r: r["total"])
    mtm_days = [r for r in days if r["close"].startswith("MTM")]
    gross_win = sum(wins)
    gross_loss = -sum(losses)
    pf = gross_win / gross_loss if gross_loss else float("inf")
    kinds = {}
    for t in trades:
        kinds[t["kind"]] = kinds.get(t["kind"], 0) + 1
    max_lot = max([t["lot"] for t in trades], default=0)

    print("\n════════════════════════ OVERALL ════════════════════════")
    print(f"  Days traded        : {len(days)}  (win {len(win_days)} / loss {len(loss_days)} / flat {len(days) - len(win_days) - len(loss_days)})")
    print(f"  Net P&L            : {inr(total)}   (CE {inr(ce_tot)} | PE {inr(pe_tot)})")
    print(f"  Avg per day        : {inr(total / len(days))}")
    print(f"  Best / worst day   : {inr(best['total'])} ({best['date']}) / {inr(worst['total'])} ({worst['date']})")
    print(f"  Max drawdown       : {inr(max_dd)}   | longest losing streak {worst_streak} days")
    print(f"  Trades             : {len(trades)}  | win rate {100 * len(wins) / max(len(trades), 1):.1f}%  "
          f"| avg win {inr(gross_win / max(len(wins), 1))} | avg loss {inr(-gross_loss / max(len(losses), 1))}")
    print(f"  Profit factor      : {pf:.2f}")
    print(f"  Exits              : " + " | ".join(f"{k} {v}" for k, v in sorted(kinds.items())))
    print(f"  Max lot used       : {max_lot}   | MTM-stop days: {len(mtm_days)}")

    # ---- short analysis ----
    notes = []
    notes.append(f"Strategy {'made' if total >= 0 else 'lost'} {inr(abs(total))} over {len(days)} days; "
                 f"{100 * len(win_days) / len(days):.0f}% of days were green.")
    if ce_tot * pe_tot < 0:
        better = "CE" if ce_tot > pe_tot else "PE"
        notes.append(f"{better} side carried the result ({inr(ce_tot)} CE vs {inr(pe_tot)} PE).")
    else:
        notes.append(f"Both sides contributed in the same direction (CE {inr(ce_tot)}, PE {inr(pe_tot)}).")
    if wins and losses:
        notes.append(f"Trade win rate is {100 * len(wins) / len(trades):.0f}% but avg win/avg loss is "
                     f"{(gross_win / len(wins)) / (gross_loss / len(losses)):.2f} — the lot martingale "
                     f"(+1 lot per SL, up to {MAX_LOT}) is what turns the few target hits into profit.")
    if mtm_days:
        notes.append(f"The −₹13,000 MTM stop fired on {len(mtm_days)} day(s), costing {inr(sum(r['total'] for r in mtm_days))} in total — "
                     f"these are the days the lot ladder ran up without a target hit.")
    if max_lot >= 8:
        notes.append(f"Lots climbed to {max_lot} at least once — watch margin and slippage at that size.")
    exp = [r for r in days if r["expiry"]]
    if exp:
        notes.append(f"Expiry days: {len(exp)} day(s), net {inr(sum(r['total'] for r in exp))}.")
    notes.append("P&L is gross: no brokerage, taxes or slippage. Fills come from 1-min bars "
                 "(O-L-H-C / O-H-L-C path), so treat it as an estimate, not tick-exact.")
    print("\n════════════════════════ ANALYSIS ════════════════════════")
    for n in notes:
        print(f"  • {n}")
    print()


def main():
    ap = argparse.ArgumentParser(description="Backtest nifty_option_buying_pivot_cum_ltp on Dhan expired options data")
    ap.add_argument("from_date", help="YYYY-MM-DD")
    ap.add_argument("to_date", help="YYYY-MM-DD")
    ap.add_argument("--quiet", action="store_true", help="hide per-trade lines, show only day totals")
    ap.add_argument("--events", action="store_true", help="also print the event log (selections, rolls, entries)")
    ap.add_argument("--refresh", action="store_true", help="ignore the cache and download again")
    args = ap.parse_args()

    d1 = date.fromisoformat(args.from_date)
    d2 = date.fromisoformat(args.to_date)
    if d2 < d1:
        sys.exit("to_date is before from_date")

    dh = DhanData(refresh=args.refresh)
    print(f"🔎 Backtest nifty_option_buying_pivot_cum_ltp  {d1} -> {d2}")
    print(f"   cache: {CACHE_DIR}  (first run downloads ~42-126 API calls per day)")

    results = []
    for day in daterange(d1, d2):
        if day.weekday() >= 5:
            continue
        rec = {"date": day.isoformat(), "dow": day.strftime("%a")}
        try:
            calls0 = dh.calls
            today = dh.day(day, 1)
            if today.get("holiday"):
                rec["status"] = "holiday / no data"
                results.append(rec)
                continue
            pday = prev_trading_day(dh, day)
            if not pday:
                rec["status"] = "no previous trading day found"
                results.append(rec)
                continue
            # today's nearest contract on the previous day: code 1, or code 2 if that day was an expiry
            prev_code = 2 if is_expiry_day(dh.day(pday, 1)) else 1
            prev = dh.day(pday, prev_code)
            if dh.calls > calls0:
                print(f"   ⬇️  {day} data ready ({dh.calls - calls0} API calls)")
        except Exception as e:
            rec["status"] = f"data error: {e}"
            results.append(rec)
            print(f"   ❌ {day}: {e}")
            continue

        sim = DaySim(day, today, prev)
        err = sim.run()
        if err:
            rec["status"] = err
            results.append(rec)
            print(f"\n━━━━━━━━ {day} ━━━━━━━━\n  skipped: {err}")
            continue

        expiry_today = is_expiry_day(today)
        print_day(day, sim, expiry_today, args.quiet)
        if args.events:
            for e in sim.events:
                print(f"     {e}")

        rec.update({
            "status": "ok",
            "ce": sim.legs["CE"]["pnl"], "pe": sim.legs["PE"]["pnl"],
            "total": sim.legs["CE"]["pnl"] + sim.legs["PE"]["pnl"],
            "n": len(sim.trades),
            "targets": sum(1 for t in sim.trades if t["kind"] == "TARGET"),
            "sls": sum(1 for t in sim.trades if t["kind"] == "SL"),
            "close": sim.close_reason or "-",
            "expiry": expiry_today,
            "trades": sim.trades,
        })
        results.append(rec)

    print_summary(results)


if __name__ == "__main__":
    main()
