import time
import os
from dotenv import load_dotenv
import requests
import pandas as pd
from datetime import datetime, timezone

load_dotenv()

# ================= НАСТРОЙКИ =================

BASE_URL = "https://toncenter.com/api/v2"

MAX_RETRIES = 1000  # повтор в случае ошибки
RETRY_DELAY = 2   # секунды

ACCOUNT = "0:e6f3d8824f46b1efbab9afc684793428c55fed69b46a15a49be69a29bc49e530"  # TON address
LIMIT = 100  # максимум за запрос

API_KEY = os.getenv("TON_API_KEY")
if not API_KEY:
    raise RuntimeError("TON_API_KEY not set in .env")


def get_transactions(account, start_ts, end_ts):
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

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = requests.get(f"{BASE_URL}/getTransactions", params=params, timeout=10)
                r.raise_for_status()
                data = r.json()["result"]
                first_ts = data[0]["utime"]
                first_date = datetime.fromtimestamp(first_ts, tz=timezone.utc)
                print(f"Fetched {len(data)} transactions (date {first_date})")
                break

            except requests.exceptions.RequestException as e:
                print(f"Request failed (attempt {attempt}/{MAX_RETRIES}): {e}")
                if attempt == MAX_RETRIES:
                    raise
                time.sleep(RETRY_DELAY)

        if not data:
            break

        for tx in data:
            ts = tx["utime"]

            if ts < start_ts:
                return all_txs  # ушли глубже

            if start_ts <= ts < end_ts:
                all_txs.append(tx)

        # pagination
        lt = data[-1]["transaction_id"]["lt"]
        hash_ = data[-1]["transaction_id"]["hash"]

    return all_txs


def save_to_csv(transactions, account, filename):
    rows = []

    for tx in transactions:
        ts = datetime.fromtimestamp(tx["utime"], tz=timezone.utc)

        # RECEIVED
        in_msg = tx.get("in_msg")
        if in_msg and in_msg.get("destination") == account:
            value = int(in_msg.get("value", 0)) / 1e9
            rows.append({
                "Timestamp": ts,
                "Action": "Received",
                "Address": in_msg.get("source"),
                "Value (TON)": value
            })

        # SENT
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


def save_transactions(month, year):
    start_ts = int(datetime(year, month, 1, tzinfo=timezone.utc).timestamp())
    if month == 12:
        end_ts = int(datetime(year + 1, 1, 1, tzinfo=timezone.utc).timestamp())
    else:
        end_ts = int(datetime(year, month + 1, 1, tzinfo=timezone.utc).timestamp())
    filename = f"{month}_{year}.csv"
    txs = get_transactions(ACCOUNT, start_ts, end_ts)
    save_to_csv(txs, ACCOUNT, filename)


if __name__ == "__main__":
    # save_transactions(12, 2025)
    for month in range(12):
        save_transactions(month + 1, 2025)

