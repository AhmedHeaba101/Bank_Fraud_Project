import requests
import random
import time
import os

API_URL = os.environ.get("API_URL", "http://localhost:8000/transaction")

ACCOUNTS = [f"ACC{i:04d}" for i in range(1, 21)]
LOCATIONS = ["Cairo", "Alexandria", "Giza", "Luxor", "Aswan"]
TX_TYPES = ["deposit", "withdrawal", "transfer"]


def generate_transaction(fraud_chance=0.1):
    is_fraud = random.random() < fraud_chance

    amount = random.uniform(5000, 20000) if is_fraud else random.uniform(50, 3000)

    return {
        "account_id": random.choice(ACCOUNTS),
        "amount": round(amount, 2),
        "transaction_type": random.choice(TX_TYPES),
        "location": random.choice(LOCATIONS),
    }


def run(n_transactions=200, delay=1.0):
    for i in range(n_transactions):
        tx = generate_transaction()
        try:
            res = requests.post(API_URL, json=tx, timeout=5)
            print(f"[{i+1}] sent -> {res.status_code} | {tx}")
        except requests.exceptions.RequestException as e:
            print(f"Error sending transaction: {e}")

        time.sleep(delay)


if __name__ == "__main__":
    run()
