import time
import pytz
import requests
from datetime import datetime, time as dtime
from dotenv import load_dotenv
import os
from dhanhq import MarketFeed
from dhanhq import DhanContext, dhanhq
import threading

from queue import Queue
import pandas as pd

import asyncio




COMMON_ID = 'b3d3baff-9e47-4cbf-934b-48800ef96b8c'
strategy_id = 'b3d3baff-9e47-4cbf-934b-48800ef96b8c'


def logtradeleg(strategyid, leg, symbol, strike_price, date, token):
    url = "https://dreaminalgo-backend-production.up.railway.app/api/tradelegs/create"
    
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


##logtradeleg(
 #   COMMON_ID,
 #   "CE",
 #   f"NIFTY CE 23500",
 #   "23500",
 #   "2024-06-27",
 #   "625673"
#)

##logtradeleg(
 #   COMMON_ID,
 #   "PE",
 #   f"NIFTY PE 23500",
 #   "23500",
 #   "2024-06-27",
 #   "625672"
#)



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
                "https://dreaminalgo-backend-production.up.railway.app/api/telemetry",
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

print("Telemetry broadcaster started. Simulating strategy updates...")  
while True:
    telemetry["pnl"] += 1
    telemetry["pnl_percentage"] += 0.1
    telemetry["ce_ltp"] += 0.5
    telemetry["pe_ltp"] += 0.3
    telemetry["ce_pnl"] += 0.8
    telemetry["pe_pnl"] += 0.4

    #print("Updated telemetry:", telemetry)

    time.sleep(0.05)