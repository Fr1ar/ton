import os
from dotenv import load_dotenv
import requests
import pandas as pd
from datetime import datetime, timezone

load_dotenv()

# ================= НАСТРОЙКИ =================

BASE_URL = "https://toncenter.com/api/v2"

# Fragment: "UQDm89iCT0ax77q5r8aEeTQoxV_tabRqFaSb5popvEnlMO6e"
ACCOUNT = "0:e6f3d8824f46b1efbab9afc684793428c55fed69b46a15a49be69a29bc49e530" # TON address
LIMIT = 100  # максимум за запрос


# декабрь 2025
START_TS = int(datetime(2025, 12, 25, tzinfo=timezone.utc).timestamp())
END_TS   = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp())

# ============================================

API_KEY = os.getenv("TON_API_KEY")
if not API_KEY:
    raise RuntimeError("TON_API_KEY not set in .env")

def get_transactions(account):
    all_txs = []
    lt = None
    hash_ = None

    while True:
        params = {
            "address": account,
            "limit": LIMIT,
            "api_key": API_KEY
        }

        if lt and hash_:
            params["lt"] = lt
            params["hash"] = hash_

        print(f"Fetching transactions from {lt} {hash_}...")
        r = requests.get(f"{BASE_URL}/getTransactions", params=params)
        r.raise_for_status()
        data = r.json()["result"]
        print(f"Fetched {len(data)} transactions")

        if not data:
            break

        for tx in data:
            ts = tx["utime"]

            if ts < START_TS:
                return all_txs  # ушли глубже декабря

            if START_TS <= ts < END_TS:
                all_txs.append(tx)

        # pagination
        lt = data[-1]["transaction_id"]["lt"]
        hash_ = data[-1]["transaction_id"]["hash"]

    return all_txs


def save_to_csv(transactions, account, filename="ton_transactions_dec_2025.csv"):
    rows = []

    for tx in transactions:
        ts = datetime.fromtimestamp(tx["utime"], tz=timezone.utc)

        # ---------- RECEIVED ----------
        in_msg = tx.get("in_msg")
        if in_msg and in_msg.get("destination") == account:
            value = int(in_msg.get("value", 0)) / 1e9
            rows.append({
                "Timestamp": ts,
                "Action": "Received",
                "Address": in_msg.get("source"),
                "Value (TON)": value
            })

        # ---------- SENT ----------
        for out_msg in tx.get("out_msgs", []):
            dest = out_msg.get("destination")
            value = int(out_msg.get("value", 0)) / 1e9

            if value > 0 and dest != account:
                rows.append({
                    "Timestamp": ts,
                    "Action": "Sent",
                    "Address": dest,
                    "Value (TON)": -value
                })

    df = pd.DataFrame(rows)
    df.to_csv(filename, index=False)
    print(f"Saved {len(df)} rows to {filename}")


if __name__ == "__main__":
    txs = get_transactions(ACCOUNT)
    save_to_csv(txs, ACCOUNT)
