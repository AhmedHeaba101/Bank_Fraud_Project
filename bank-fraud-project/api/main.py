
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from kafka import KafkaProducer
import json
import os
import uuid
from datetime import datetime

app = FastAPI(title="Bank Transaction Ingestion API")

KAFKA_BROKER = "kafka:29092"
TOPIC_NAME = "bank-transactions"

REPORTS_DIR = "/opt/reports"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def read_report(filename: str):
    path = os.path.join(REPORTS_DIR, filename)
    if not os.path.exists(path):
        raise HTTPException(
            status_code=404,
            detail=(
                f"Report '{filename}' not found yet." 
            ),
        )
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


class Transaction(BaseModel):
    account_id: str
    amount: float
    transaction_type: str  # deposit / withdrawal / transfer
    location: str


@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.post("/transaction")
def create_transaction(tx: Transaction):
    try:
        event = {
            "transaction_id": str(uuid.uuid4()),
            "account_id": tx.account_id,
            "amount": tx.amount,
            "transaction_type": tx.transaction_type,
            "location": tx.location,
            "timestamp": datetime.utcnow().isoformat(),
        }

        producer.send(TOPIC_NAME, value=event)
        producer.flush()

        return {"message": "Transaction sent to Kafka", "data": event}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/fraud-by-location")
def fraud_by_location():
    return read_report("fraud_by_location.json")

@app.get("/top-fraud-accounts")
def top_fraud_accounts():
    return read_report("top_fraud_accounts.json")

@app.get("/fraud-vs-normal")
def fraud_vs_normal():
    return read_report("fraud_vs_normal.json")


@app.get("/all-transactions")
def all_transactions():
    return read_report("all_transactions.json")
