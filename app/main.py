import os
from datetime import datetime, timedelta
import httpx
from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from prometheus_client import Gauge, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

app = FastAPI(title="KAPI Monitor")

# KAPI config
KAPI_URL = os.getenv("KAPI_URL")
KAPI_USER = os.getenv("KAPI_USER")
KAPI_PASSWORD = os.getenv("KAPI_PASSWORD")

# MongoDB config
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "kafka")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "clusters")

# Alert threshold in days
ALERT_THRESHOLD_DAYS = int(os.getenv("ALERT_THRESHOLD_DAYS", "7"))

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[MONGO_DB]
collection = db[MONGO_COLLECTION]

# Prometheus metrics
topic_empty_no_consumer = Gauge(
    "kafka_topic_empty_no_consumer_days",
    "Days since topic is empty without consumer",
    ["topic"]
)
topic_not_empty_no_consumer = Gauge(
    "kafka_topic_not_empty_no_consumer_days",
    "Days since topic has data but no consumer",
    ["topic"]
)
topic_empty_with_consumer = Gauge(
    "kafka_topic_empty_with_consumer_days",
    "Days since topic is empty but has consumer",
    ["topic"]
)
topic_size_bytes = Gauge(
    "kafka_topic_size_bytes",
    "Topic size in bytes",
    ["topic"]
)
topic_consumer_count = Gauge(
    "kafka_topic_consumer_count",
    "Number of consumers for topic",
    ["topic"]
)


def get_token() -> str:
    response = httpx.post(
        f"{KAPI_URL}/v3/auth/token",
        json={"username": KAPI_USER, "password": KAPI_PASSWORD},
        verify=False
    )
    if response.status_code != 200:
        raise HTTPException(status_code=401, detail="KAPI authentication failed")
    return response.json()["access_token"]


def kapi_request(endpoint: str):
    token = get_token()
    response = httpx.get(
        f"{KAPI_URL}{endpoint}",
        headers={"Authorization": f"Bearer {token}"},
        verify=False
    )
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=f"KAPI error: {endpoint}")
    return response.json()


def get_topic_consumers(cgroups: list) -> dict:
    """Build dict of topic -> list of consumers."""
    topic_consumers = {}
    for group in cgroups:
        for member in group.get("members", []):
            for sub_topic in member.get("subscription", []):
                if sub_topic not in topic_consumers:
                    topic_consumers[sub_topic] = []
                topic_consumers[sub_topic].append({
                    "group": group["group"],
                    "state": group["state"],
                    "client_id": member["client_id"],
                    "client_host": member["client_host"]
                })
    return topic_consumers


def classify_topic(size: int, consumer_count: int) -> str:
    """Classify topic state."""
    is_empty = size == 0
    has_consumer = consumer_count > 0

    if is_empty and not has_consumer:
        return "empty_no_consumer"
    elif not is_empty and not has_consumer:
        return "not_empty_no_consumer"
    elif is_empty and has_consumer:
        return "empty_with_consumer"
    else:
        return "healthy"


def days_since(dt: datetime) -> int:
    if dt is None:
        return 0
    return (datetime.utcnow() - dt).days


@app.post("/sync")
def sync_topics():
    """Sync topics from KAPI and update MongoDB with state tracking."""
    topics_size = kapi_request("/v3/topics/size")
    cgroups = kapi_request("/v3/cgroups")
    topic_consumers = get_topic_consumers(cgroups)

    now = datetime.utcnow()
    updated = 0

    for doc in collection.find({"topics": {"$exists": True}}):
        topics_updated = []

        for topic in doc.get("topics", []):
            topic_name = topic.get("name")
            size = topics_size.get(topic_name, 0)
            consumers = topic_consumers.get(topic_name, [])
            consumer_count = len(consumers)

            current_state = classify_topic(size, consumer_count)
            previous_state = topic.get("state")

            # Track state change timestamp
            if current_state != previous_state:
                topic["state_since"] = now
            elif "state_since" not in topic:
                topic["state_since"] = now

            topic["size"] = size
            topic["consumers"] = consumers
            topic["consumer_count"] = consumer_count
            topic["state"] = current_state
            topic["last_sync"] = now

            topics_updated.append(topic)

        collection.update_one(
            {"_id": doc["_id"]},
            {"$set": {"topics": topics_updated}}
        )
        updated += 1

    return {"status": "ok", "documents_updated": updated, "synced_at": now.isoformat()}


@app.get("/alerts")
def get_alerts():
    """Get topics in alert state (threshold exceeded)."""
    threshold = timedelta(days=ALERT_THRESHOLD_DAYS)
    now = datetime.utcnow()
    alerts = []

    for doc in collection.find({"topics": {"$exists": True}}):
        for topic in doc.get("topics", []):
            state = topic.get("state")
            state_since = topic.get("state_since")

            if state in ["empty_no_consumer", "not_empty_no_consumer", "empty_with_consumer"]:
                if state_since and (now - state_since) > threshold:
                    alerts.append({
                        "topic": topic.get("name"),
                        "state": state,
                        "since": state_since.isoformat(),
                        "days": days_since(state_since),
                        "size": topic.get("size"),
                        "consumer_count": topic.get("consumer_count")
                    })

    return {"alerts": alerts, "threshold_days": ALERT_THRESHOLD_DAYS}


@app.get("/metrics")
def metrics():
    """Prometheus metrics endpoint."""
    for doc in collection.find({"topics": {"$exists": True}}):
        for topic in doc.get("topics", []):
            topic_name = topic.get("name", "unknown")
            state = topic.get("state")
            state_since = topic.get("state_since")
            days = days_since(state_since)

            # Reset all gauges for this topic
            topic_empty_no_consumer.labels(topic=topic_name).set(0)
            topic_not_empty_no_consumer.labels(topic=topic_name).set(0)
            topic_empty_with_consumer.labels(topic=topic_name).set(0)

            # Set gauge based on state
            if state == "empty_no_consumer":
                topic_empty_no_consumer.labels(topic=topic_name).set(days)
            elif state == "not_empty_no_consumer":
                topic_not_empty_no_consumer.labels(topic=topic_name).set(days)
            elif state == "empty_with_consumer":
                topic_empty_with_consumer.labels(topic=topic_name).set(days)

            # Always set size and consumer count
            topic_size_bytes.labels(topic=topic_name).set(topic.get("size", 0))
            topic_consumer_count.labels(topic=topic_name).set(topic.get("consumer_count", 0))

    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
def health():
    return {"status": "ok"}
