from dispatcher import publish
from dhanhq import MarketFeed
from dhanhq import dhanhq,DhanContext
from datetime import datetime
from dhan_token import get_access_token
from dotenv import load_dotenv
import os
import pytz
import threading


import option_chain_manager

import paper_trade_niftyoption50_no_reentry as strategy1

option_chain_manager.update_option_chain()

import nifty_option_buying_pivot_cum_ltp as strategy14
import nifty_index_based_pivot_buying as strategy16
import nifty_vwap_pivot_buying as strategy17


rb_started = False
rb_buying=False

# collect all tokens
ALL_TOKENS = set()
if 'strategy1' in globals():
    ALL_TOKENS.update(strategy1.TOKENS)

 
access_token = get_access_token()
client_id = os.getenv("CLIENT_ID")
dhan_context = DhanContext(client_id, access_token)
dhan = dhanhq(dhan_context)

print("ALL TOKENS",ALL_TOKENS)

instruments = [
    (MarketFeed.NSE_FNO, token, MarketFeed.Quote)
    for (token) in ALL_TOKENS
]

instruments.append((MarketFeed.IDX, "13", MarketFeed.Quote))

feed = MarketFeed(dhan_context, instruments, "v2")

def on_message(msg):

    global rb_started, rb_buying

    try:

        token = str(msg["security_id"])
        publish(token, msg)


    except Exception as e:

        print("ON MESSAGE ERROR:", e)
    
feed.run_forever()
while True:
    try:


        data = feed.get_data()

        if data:
            on_message(data)

    except Exception as e:
        print("WS ERROR:", e)
        feed.run_forever()
        