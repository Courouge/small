import os
import requests
from datetime import datetime
from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
from typing import Optional

# Configuration
KAPI_URL = os.getenv("KAPI_URL", "URL")
KAPI_USER = os.getenv("KAPI_USER", "KAPI_USER")
KAPI_PASSWORD = os.getenv("KAPI_PASSWORD", "KAPI_PASSWORD")
CA_CERT = os.getenv("CA_CERT", "/etc/ssl/certs/ca-certificates.crt")

# MongoDB (optionnel)
MONGO_ENABLED = os.getenv("MONGO_ENABLED", "false").lower() == "true"
if MONGO_ENABLED:
    from pymongo import MongoClient
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    MONGO_DB = os.getenv("MONGO_DB", "kafka")
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[MONGO_DB]
    topics_collection = db["topics"]

app = FastAPI(title="KAPI Monitor")

# Cache pour les métriques
metrics_cache = {
    "topics": {},
    "last_sync": None
}


def get_access_token() -> str:
    """Authenticate and return access token."""
    url = f"{KAPI_URL}/v3/auth/token/"
    payload = {
        "grant_type": "",
        "username": KAPI_USER,
        "password": KAPI_PASSWORD,
        "scope": "",
        "client_id": "",
        "client_secret": ""
    }
    response = requests.post(url, data=payload, verify=CA_CERT)
    response.raise_for_status()
    return response.json()["access_token"]


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


def sync_topics():
    """Fetch topics from KAPI and update cache."""
    access_token = get_access_token()
    headers = {"Authorization": f"Bearer {access_token}"}

    # Get topics size
    response = requests.get(f"{KAPI_URL}/v3/topics/size", headers=headers, verify=CA_CERT)
    topics_size = response.json()

    # Get consumer groups
    response = requests.get(f"{KAPI_URL}/v3/cgroups", headers=headers, verify=CA_CERT)
    cgroups = response.json()
    topic_consumers = get_topic_consumers(cgroups)

    now = datetime.utcnow()
    topics = {}

    for topic, size in topics_size.items():
        consumers = topic_consumers.get(topic, [])
        consumer_count = len(consumers)
        state = classify_topic(size, consumer_count)

        topics[topic] = {
            "name": topic,
            "size": size,
            "consumer_count": consumer_count,
            "consumers": consumers,
            "state": state,
            "last_sync": now.isoformat()
        }

        # MongoDB (optionnel)
        if MONGO_ENABLED:
            topics_collection.update_one(
                {"name": topic},
                {"$set": topics[topic]},
                upsert=True
            )

    metrics_cache["topics"] = topics
    metrics_cache["last_sync"] = now

    return topics


def generate_prometheus_metrics() -> str:
    """Generate Prometheus metrics format."""
    lines = []

    lines.append("# HELP kafka_topic_size_messages Number of messages in topic")
    lines.append("# TYPE kafka_topic_size_messages gauge")
    lines.append("# HELP kafka_topic_consumer_count Number of active consumers for topic")
    lines.append("# TYPE kafka_topic_consumer_count gauge")
    lines.append("# HELP kafka_topic_state Topic state (0=healthy, 1=empty_no_consumer, 2=not_empty_no_consumer, 3=empty_with_consumer)")
    lines.append("# TYPE kafka_topic_state gauge")
    lines.append("# HELP kafka_topic_empty Topic is empty (0=has messages, 1=empty)")
    lines.append("# TYPE kafka_topic_empty gauge")
    lines.append("# HELP kafka_topic_has_consumer Topic has active consumer (0=no, 1=yes)")
    lines.append("# TYPE kafka_topic_has_consumer gauge")

    state_map = {
        "healthy": 0,
        "empty_no_consumer": 1,
        "not_empty_no_consumer": 2,
        "empty_with_consumer": 3
    }

    for topic, data in metrics_cache["topics"].items():
        safe_topic = topic.replace('"', '\\"')
        lines.append(f'kafka_topic_size_messages{{topic="{safe_topic}"}} {data["size"]}')
        lines.append(f'kafka_topic_consumer_count{{topic="{safe_topic}"}} {data["consumer_count"]}')
        lines.append(f'kafka_topic_state{{topic="{safe_topic}"}} {state_map[data["state"]]}')
        lines.append(f'kafka_topic_empty{{topic="{safe_topic}"}} {1 if data["size"] == 0 else 0}')
        lines.append(f'kafka_topic_has_consumer{{topic="{safe_topic}"}} {1 if data["consumer_count"] > 0 else 0}')

    return "\n".join(lines) + "\n"


@app.post("/sync")
def api_sync():
    """Trigger sync from KAPI."""
    topics = sync_topics()
    return {
        "status": "ok",
        "topics_count": len(topics),
        "synced_at": metrics_cache["last_sync"].isoformat()
    }


@app.get("/metrics", response_class=PlainTextResponse)
def api_metrics():
    """Prometheus metrics endpoint."""
    if not metrics_cache["topics"]:
        sync_topics()
    return generate_prometheus_metrics()


@app.get("/topics")
def api_topics():
    """List all topics with details."""
    if not metrics_cache["topics"]:
        sync_topics()
    return list(metrics_cache["topics"].values())


@app.get("/topics/{topic_name}")
def api_topic_detail(topic_name: str):
    """Get single topic details."""
    if not metrics_cache["topics"]:
        sync_topics()
    topic = metrics_cache["topics"].get(topic_name)
    if not topic:
        raise HTTPException(status_code=404, detail="Topic not found")
    return topic


@app.get("/summary")
def api_summary():
    """Summary by state."""
    if not metrics_cache["topics"]:
        sync_topics()

    summary = {
        "healthy": [],
        "empty_no_consumer": [],
        "not_empty_no_consumer": [],
        "empty_with_consumer": []
    }

    for topic, data in metrics_cache["topics"].items():
        summary[data["state"]].append(topic)

    return {
        "total": len(metrics_cache["topics"]),
        "healthy": len(summary["healthy"]),
        "empty_no_consumer": len(summary["empty_no_consumer"]),
        "not_empty_no_consumer": len(summary["not_empty_no_consumer"]),
        "empty_with_consumer": len(summary["empty_with_consumer"]),
        "details": summary,
        "last_sync": metrics_cache["last_sync"].isoformat() if metrics_cache["last_sync"] else None
    }


@app.get("/health")
def api_health():
    """Health check."""
    return {"status": "ok"}
