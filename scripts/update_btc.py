import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

DATA_FILE = Path(__file__).resolve().parent.parent / "data" / "btc-history.json"
CURRENT_PRICE_URL = "https://mempool.space/api/v1/prices"
HISTORICAL_PRICE_URL = "https://mempool.space/api/v1/historical-price"


def load_history():
    if DATA_FILE.exists():
        with open(DATA_FILE) as f:
            return json.load(f)
    return []


def save_history(history):
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(DATA_FILE, "w") as f:
        json.dump(history, f, indent=2)
        f.write("\n")


def date_to_midnight_utc(d):
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


def fetch_current_price():
    resp = requests.get(CURRENT_PRICE_URL, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data["time"], int(data["USD"])


def fetch_historical_price(unix_ts):
    resp = requests.get(
        HISTORICAL_PRICE_URL,
        params={"currency": "USD", "timestamp": unix_ts},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    entry = data["prices"][0]
    return entry["time"], int(entry["USD"])


def get_missing_dates(history):
    today = datetime.now(timezone.utc).date()

    if history:
        last_ts_ms = history[-1][0]
        last_date = datetime.fromtimestamp(last_ts_ms / 1000, tz=timezone.utc).date()
    else:
        last_date = datetime(2020, 1, 1, tzinfo=timezone.utc).date() - timedelta(days=1)

    missing = []
    d = last_date + timedelta(days=1)
    while d <= today:
        missing.append(d)
        d += timedelta(days=1)

    return missing, today


def main():
    history = load_history()
    missing_dates, today = get_missing_dates(history)

    if not missing_dates:
        print("No missing dates. Everything is up to date.")
        print(f"Total entries: {len(history)}.")
        return

    added = 0
    for d in missing_dates:
        unix_ts = date_to_midnight_utc(d)
        try:
            if d == today:
                ts, price = fetch_current_price()
            else:
                ts, price = fetch_historical_price(unix_ts)

            history.append([ts * 1000, price])
            print(f"Fetched {d.isoformat()} → ${price}")
            added += 1
        except Exception as e:
            print(f"Error fetching {d.isoformat()}: {e}")

        if d != missing_dates[-1]:
            time.sleep(0.5)

    # Idempotency: if the last entry is today, overwrite with freshest price
    if history:
        last_date = datetime.fromtimestamp(
            history[-1][0] / 1000, tz=timezone.utc
        ).date()
        if last_date == today:
            try:
                ts, price = fetch_current_price()
                history[-1] = [ts * 1000, price]
                print(f"Updated today's price → ${price}")
            except Exception as e:
                print(f"Error refreshing today's price: {e}")

    save_history(history)
    print(f"Done. Total entries: {len(history)}. Added: {added} new days.")


if __name__ == "__main__":
    main()
