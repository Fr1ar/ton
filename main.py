import time
import os
from dotenv import load_dotenv
import requests
import pandas as pd
from datetime import datetime, timezone

load_dotenv()

# ================= НАСТРОЙКИ =================

BASE_URL = "https://toncenter.com/api/v3/transactions"

MAX_RETRIES = 100  # сколько попыток при ошибках
RETRY_DELAY = 2   # задержка между попытками в секундах

ACCOUNT = "0:e6f3d8824f46b1efbab9afc684793428c55fed69b46a15a49be69a29bc49e530"
LIMIT = 1000  # максимум за запрос (API v3 ограничивает до 1000) :contentReference[oaicite:1]{index=1}

API_KEY = os.getenv("TON_API_KEY")
if not API_KEY:
    raise RuntimeError("TON_API_KEY not set in .env")


def get_transactions_v3(account, start_ts, end_ts):
    all_txs = []
    offset = 0

    while True:
        params = {
            "account": account,
            "limit": LIMIT,
            "offset": offset,
            "start_utime": start_ts,
            "end_utime": end_ts,
        }

        headers = {
            "X-Api-Key": API_KEY
        }

        print(f"Fetching transactions offset={offset}...")

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = requests.get(BASE_URL, params=params, headers=headers, timeout=10)
                r.raise_for_status()
                result = r.json()
                txs = result.get("transactions", [])
                break

            except requests.exceptions.RequestException as e:
                print(f"Request failed (attempt {attempt}/{MAX_RETRIES}): {e}")
                if attempt == MAX_RETRIES:
                    raise
                time.sleep(RETRY_DELAY)

        if not txs:
            break

        first_date = datetime.fromtimestamp(txs[0]["now"], tz=timezone.utc)
        print(f"First transaction in this range at: {first_date}")
        all_txs.extend(txs)

        # если пришло меньше чем limit — это последняя страница
        if len(txs) < LIMIT:
            break

        offset += LIMIT

    print(f"Total fetched: {len(all_txs)}")
    return all_txs


def save_to_csv_v3(transactions, account, filename):
    rows = []

    for tx in transactions:
        ts = datetime.fromtimestamp(tx["now"], tz=timezone.utc)

        for out_msg in tx.get("out_msgs", []):
            src = out_msg.get("source")
            dest = out_msg.get("destination")
            value = int(out_msg.get("value", 0)) / 1e9

            if value <= 0:
                continue  # игнорируем нулевые переводы

            if dest == account:
                # Получили TON
                rows.append({
                    "Timestamp": ts,
                    "Action": "Received",
                    "Address": src,
                    "Value (TON)": value
                })
            elif src == account:
                # Отправили TON
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
    txs = get_transactions_v3(ACCOUNT, start_ts, end_ts)
    save_to_csv_v3(txs, ACCOUNT, filename)


if __name__ == "__main__":
    for month in range(12):
        save_transactions(month + 1, 2025)
