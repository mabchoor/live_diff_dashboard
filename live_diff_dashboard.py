import requests
import pandas as pd
from IPython.display import display, clear_output
from time import sleep
from datetime import datetime

def fetch_binance_tickers():
    url = "https://data-api.binance.vision/api/v3/ticker/price"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    data = r.json()
    return {it["symbol"]: float(it["price"]) for it in data if it["symbol"].endswith("USDT")}

def fetch_okx_tickers():
    url = "https://www.okx.com/api/v5/market/tickers?instType=SPOT"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    data = r.json()
    return {it["instId"].upper(): float(it["last"]) for it in data.get("data", []) if it["instId"].endswith("-USDT")}

def normalize_binance_symbol(sym):
    return f"{sym[:-4]}-USDT"

def compute_differences(binance, okx):
    results = []
    bnorm = {normalize_binance_symbol(s): p for s, p in binance.items()}
    common = set(bnorm.keys()) & set(okx.keys())

    for ns in common:
        bprice = bnorm[ns]
        oprice = okx[ns]
        if not bprice or not oprice or bprice == 0 or oprice == 0:
            continue
        abs_diff = abs(bprice - oprice)
        avg = (bprice + oprice) / 2
        pct_diff = (abs_diff / avg) * 100
        if pct_diff != 0:
            results.append((ns, bprice, oprice, abs_diff, pct_diff))
    return sorted(results, key=lambda x: x[4], reverse=True)

def live_refresh(interval=30, top_n=20):
    try:
        while True:
            binance = fetch_binance_tickers()
            okx = fetch_okx_tickers()
            diffs = compute_differences(binance, okx)

            # Build DataFrame for display
            df = pd.DataFrame(diffs, columns=["Pair", "Binance Price", "OKX Price", "Abs Diff", "% Diff"])
            df_display = df.head(top_n).copy()
            df_display["Binance Price"] = df_display["Binance Price"].map("{:.6f}".format)
            df_display["OKX Price"] = df_display["OKX Price"].map("{:.6f}".format)
            df_display["Abs Diff"] = df_display["Abs Diff"].map("{:.6f}".format)
            df_display["% Diff"] = df_display["% Diff"].map("{:.4f}%".format)

            clear_output(wait=True)
            print(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Showing top {top_n} price differences between Binance and OKX:\n")
            display(df_display)

            sleep(interval)

    except KeyboardInterrupt:
        print("Live refresh stopped by user.")

# Run it (interrupt cell execution to stop)
live_refresh(interval=30, top_n=20)
