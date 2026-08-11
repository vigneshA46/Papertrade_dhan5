import requests
from dhan_token import get_access_token

import pandas as pd
import pytz
from io import StringIO
from datetime import datetime
from dotenv import load_dotenv
import os
import json
from dhanhq import dhanhq,DhanContext



load_dotenv()

ACCESS_TOKEN = get_access_token()
client_id = os.getenv("CLIENT_ID")
dhan_context = DhanContext(client_id, ACCESS_TOKEN)
dhan = dhanhq(dhan_context)



HEADERS = {
    "Content-Type": "application/json",
    "access-token": ACCESS_TOKEN
}

IDX_INTRADAY_URL = "https://api.dhan.co/v2/charts/intraday"
FNO_MASTER_URL   = "https://api.dhan.co/v2/instrument/NSE_EQ"
STOCK_MASTER_URL = "https://api.dhan.co/v2/instrument/NSE_FNO"


IST = pytz.timezone("Asia/Kolkata")



API_URL = "https://dreaminalgo-backend-production.up.railway.app/api/stocks/today"


def load_fno_master() -> pd.DataFrame:
    print("...downloading FNO master")

    r = requests.get(FNO_MASTER_URL, headers={"access-token": ACCESS_TOKEN})
    r.raise_for_status()

    # ✅ Use header from API (IMPORTANT)
    df = pd.read_csv(StringIO(r.text), low_memory=False)

    # ✅ Drop unwanted column
    if "Unnamed: 31" in df.columns:
        df = df.drop(columns=["Unnamed: 31"])

    # ✅ Type conversions
    df["STRIKE_PRICE"] = pd.to_numeric(df["STRIKE_PRICE"], errors="coerce")
    df["SM_EXPIRY_DATE"] = pd.to_datetime(df["SM_EXPIRY_DATE"], errors="coerce")

    return df

def load_stock_master() -> pd.DataFrame:
    print("...downloading stock master")

    r = requests.get(STOCK_MASTER_URL, headers={"access-token": ACCESS_TOKEN})
    r.raise_for_status()

    # ✅ Use header from API (IMPORTANT)
    df = pd.read_csv(StringIO(r.text), low_memory=False)

    # ✅ Drop unwanted column
    if "Unnamed: 31" in df.columns:
        df = df.drop(columns=["Unnamed: 31"])

    # ✅ Type conversions
    df["STRIKE_PRICE"] = pd.to_numeric(df["STRIKE_PRICE"], errors="coerce")
    df["SM_EXPIRY_DATE"] = pd.to_datetime(df["SM_EXPIRY_DATE"], errors="coerce")

    return df

def calculate_atm(price, strike_gap):
    return int(round(price / strike_gap) * strike_gap)


def get_strike_gap(df, symbol):
    opt_df = df[
        (df["UNDERLYING_SYMBOL"] == symbol) &
        (df["INSTRUMENT"].isin(["OPTSTK", "OPTIDX"]))
    ].copy()

    if opt_df.empty:
        raise ValueError(f"❌ No option contracts found for {symbol}")

    opt_df = opt_df.dropna(subset=["SM_EXPIRY_DATE"])

    nearest_expiry = opt_df["SM_EXPIRY_DATE"].min()

    opt_df = opt_df[
        opt_df["SM_EXPIRY_DATE"] == nearest_expiry
    ]

    strikes = sorted(
        opt_df["STRIKE_PRICE"]
        .dropna()
        .unique()
    )

    if len(strikes) < 2:
        raise ValueError(
            f"❌ Not enough strikes for {symbol}"
        )

    diffs = [
        strikes[i + 1] - strikes[i]
        for i in range(len(strikes) - 1)
    ]

    positive_diffs = [d for d in diffs if d > 0]

    strike_gap = min(positive_diffs)

    return strike_gap

def find_stock_security(df,target_symbol):

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", None)
    pd.set_option("display.max_colwidth", None)
    

    opt = df[
        (df["INSTRUMENT"] == "EQUITY") &
        (df["UNDERLYING_SYMBOL"] == target_symbol)
    ]

    row = opt.iloc[0]

    print(row.to_dict())

    #print(opt)

    if opt.empty:
        raise ValueError(f"❌ No {option_type} found for strike {strike}")

    return opt.sort_values("SM_EXPIRY_DATE").iloc[0]


def get_premarket_prices(dhan, security_ids):
    try:
        response = dhan.ohlc_data(
            securities={"NSE_EQ": security_ids}
        )

        return response["data"]["data"]["NSE_EQ"]

    except Exception as e:
        print(f"❌ Quote fetch failed: {e}")
        return {}


    except Exception as e:
        print(f"❌ Quote fetch failed for {security_id}: {e}")
        return None


def get_nearest_expiry(dhan, security_id):
    try:
        response = dhan.expiry_list(
            under_security_id=security_id,
            under_exchange_segment="NSE_EQ"
        )

        expiries = response["data"]["data"]

        if not expiries:
            raise ValueError(
                f"No expiries found for {security_id}"
            )

        return expiries[0]

    except Exception as e:
        print(
            f"❌ Expiry fetch failed for {security_id}: {e}"
        )
        return None

def build_ladder_values(dhan, security_id, expiry, atm, strike_gap):

    response = dhan.option_chain(
        under_security_id=security_id,
        under_exchange_segment="NSE_EQ",
        expiry=expiry
    )

    oc = response["data"]["data"]["oc"]

    atm_key = f"{float(atm):.6f}"

    if atm_key not in oc:
        raise ValueError(
            f"ATM strike {atm} not found in option chain"
        )

    atm_ce_close = float(
        oc[atm_key]["ce"]["previous_close_price"]
    )

    atm_pe_close = float(
        oc[atm_key]["pe"]["previous_close_price"]
    )

    values = []

    value1 = (atm_ce_close + atm_pe_close) / 2

    values.append(round(value1, 2))

    for i in range(1, 8):

        strike = atm + (i * strike_gap)
        strike_key = f"{float(strike):.6f}"

        if strike_key not in oc:
            raise ValueError(
                f"Strike {strike} not found in option chain"
            )

        pe_close = float(
            oc[strike_key]["pe"]["previous_close_price"]
        )

        values.append(round(pe_close, 2))

    return values


def get_todays_stocks():
    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()

        data = response.json()

        if not data.get("success"):
            print("API returned success=False")
            return []

        stocks = data.get("stocks", [])

        print(f"Found {len(stocks)} stock(s)")

        for stock in stocks:
            print(
                f"{stock['symbol']} | "
                f"IEP: {stock['iep']} | "
                f"Prev Close: {stock['prev_close']}"
            )

        return stocks

    except Exception as e:
        print(f"Error fetching stocks: {e}")
        return []


if __name__ == "__main__":

    #sample_expiry = dhan.expiry_list(
    #under_security_id=23650,                       # Nifty
    #under_exchange_segment="NSE_EQ"
    #)

    #print(sample_expiry)

    #sample = dhan.option_chain(
    #    under_security_id=23650,                       # Nifty
    #    under_exchange_segment="NSE_EQ",
    #    expiry="2026-06-30"
    #   )
    #print("SAMPLE OPTION CHAIN RESPONSE:\n\n")

    #print(json.dumps(sample, indent=4))

    #sample = dhan.ohlc_data(
    #    securities = {"NSE_EQ":[23650,1333]}
    #    )

    #print(sample)


    fno_df = load_fno_master()
    stock_df = load_stock_master()  
    #find_stock_security(fno_df, "SHRIRAMFIN")


    stocks = get_todays_stocks()
    print("STOCKS FOR TODAY:", stocks)

    

    #symbols = [stock["symbol"] for stock in stocks]

    #print("SYMBOLS:\n", symbols)

    strategy_state = {}
    for stock in stocks:
        symbol = stock["symbol"]

        try:
            instrument = find_stock_security(fno_df, symbol)

            strategy_state[symbol] = {
                "security_id": int(instrument["SECURITY_ID"]),
                "iep": float(stock["iep"]),
                "prev_close": float(stock["prev_close"])
            }
            print("\n\n")
            print("strategy state")
            print(strategy_state[symbol])


        except Exception as e:
            print(f"❌ {symbol}: {e}")

    security_ids = [
        data["security_id"]
        for data in strategy_state.values()
    ]

    quotes = get_premarket_prices(dhan, security_ids)

    for symbol, data in strategy_state.items():

        security_id = str(data["security_id"])

        if security_id in quotes:

            data["premarket_price"] = float(
                quotes[security_id]["last_price"]
            )

            print(
                f"{symbol} | "
                f"Security ID: {security_id} | "
                f"Premarket Price: {data['premarket_price']}"
            )

        else:
            print(f"❌ No quote data for {symbol} ({security_id})")

    for symbol, data in strategy_state.items():

        try:
            strike_gap = get_strike_gap(
                stock_df,
                symbol
            )

            data["strike_gap"] = strike_gap

            print(
                f"{symbol} | "
                f"Strike Gap: {strike_gap}"
            )

        except Exception as e:
            print(f"❌ {symbol}: {e}")

    
    for symbol, data in strategy_state.items():

        try:
            atm = calculate_atm(
                data["premarket_price"],
                data["strike_gap"]
            )

            data["atm"] = atm

            print(
                f"{symbol} | "
                f"Premarket: {data['premarket_price']} | "
                f"Gap: {data['strike_gap']} | "
                f"ATM: {atm}"
            )

        except Exception as e:
            print(f"❌ ATM calculation failed for {symbol}: {e}")

    
    for symbol, data in strategy_state.items():

        expiry = get_nearest_expiry(
            dhan,
            data["security_id"]
        )

        if expiry:
            data["expiry"] = expiry

            print(
                f"{symbol} | "
                f"ATM: {data['atm']} | "
                f"Expiry: {expiry}"
            )

    for symbol, data in strategy_state.items():

        try:
            values = build_ladder_values(
                dhan=dhan,
                security_id=data["security_id"],
                expiry=data["expiry"],
                atm=data["atm"],
                strike_gap=data["strike_gap"]
            )

            data["values"] = values

            print(f"\n{symbol}")
            print(f"Values: {values}")

        except Exception as e:
            print(f"❌ Ladder build failed for {symbol}: {e}")

    print(json.dumps(strategy_state, indent=4))