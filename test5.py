import os
import gzip
import requests
from datetime import datetime
from io import StringIO
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response, JSONResponse
import json

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

# Cache
metrics_cache = {
    "topics": {},
    "last_sync": None,
    "prometheus_output": None,
    "prometheus_gzip": None
}

STATE_MAP = {
    "healthy": 0,
    "empty_no_consumer": 1,
    "not_empty_no_consumer": 2,
    "empty_with_consumer": 3
}


def gzip_compress(data: str) -> bytes:
    """Compress string to gzip bytes."""
    return gzip.compress(data.encode("utf-8"), compresslevel=6)


def gzip_response(data, media_type: str = "application/json") -> Response:
    """Return gzip compressed response."""
    if isinstance(data, dict) or isinstance(data, list):
        content = json.dumps(data)
    else:
        content = data
    
    compressed = gzip_compress(content)
    return Response(
        content=compressed,
        media_type=media_type,
        headers={
            "Content-Encoding": "gzip",
            "Content-Length": str(len(compressed))
        }
    )


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
    """Build dict of topic -> consumer count."""
    topic_consumers = {}
    for group in cgroups:
        for member in group.get("members", []):
            for sub_topic in member.get("subscription", []):
                if sub_topic not in topic_consumers:
                    topic_consumers[sub_topic] = 0
                topic_consumers[sub_topic] += 1
    return topic_consumers


def classify_topic(size: int, consumer_count: int) -> int:
    """Return state as int directly."""
    is_empty = size == 0
    has_consumer = consumer_count > 0

    if is_empty and not has_consumer:
        return 1  # empty_no_consumer
    elif not is_empty and not has_consumer:
        return 2  # not_empty_no_consumer
    elif is_empty and has_consumer:
        return 3  # empty_with_consumer
    return 0  # healthy


def generate_prometheus_metrics(topics: dict) -> str:
    """Generate Prometheus metrics - optimized."""
    buf = StringIO()

    buf.write("# HELP kafka_topic_info Topic info with size, consumers and state\n")
    buf.write("# TYPE kafka_topic_info gauge\n")

    for topic, (size, consumer_count, state) in topics.items():
        safe_topic = topic.replace("\\", "\\\\").replace('"', '\\"')
        buf.write(f'kafka_topic_info{{topic="{safe_topic}",type="size"}} {size}\n')
        buf.write(f'kafka_topic_info{{topic="{safe_topic}",type="consumers"}} {consumer_count}\n')
        buf.write(f'kafka_topic_info{{topic="{safe_topic}",type="state"}} {state}\n')

    return buf.getvalue()


def sync_topics():
    """Fetch topics from KAPI and update cache."""
    access_token = get_access_token()
    headers = {"Authorization": f"Bearer {access_token}"}

    response = requests.get(f"{KAPI_URL}/v3/topics/size", headers=headers, verify=CA_CERT)
    topics_size = response.json()

    response = requests.get(f"{KAPI_URL}/v3/cgroups", headers=headers, verify=CA_CERT)
    cgroups = response.json()
    topic_consumers = get_topic_consumers(cgroups)

    now = datetime.utcnow()
    topics = {}

    for topic, size in topics_size.items():
        consumer_count = topic_consumers.get(topic, 0)
        state = classify_topic(size, consumer_count)
        topics[topic] = (size, consumer_count, state)

        if MONGO_ENABLED:
            topics_collection.update_one(
                {"name": topic},
                {"$set": {
                    "name": topic,
                    "size": size,
                    "consumer_count": consumer_count,
                    "state": state,
                    "last_sync": now
                }},
                upsert=True
            )

    # Update cache
    metrics_cache["topics"] = topics
    metrics_cache["last_sync"] = now
    
    # Pre-generate and compress prometheus output
    prometheus_output = generate_prometheus_metrics(topics)
    metrics_cache["prometheus_output"] = prometheus_output
    metrics_cache["prometheus_gzip"] = gzip_compress(prometheus_output)

    return topics


@app.post("/sync")
def api_sync(request: Request):
    """Trigger sync from KAPI."""
    topics = sync_topics()
    data = {
        "status": "ok",
        "topics_count": len(topics),
        "synced_at": metrics_cache["last_sync"].isoformat()
    }
    
    if "gzip" in request.headers.get("accept-encoding", ""):
        return gzip_response(data)
    return data


@app.get("/metrics")
def api_metrics(request: Request):
    """Prometheus metrics endpoint."""
    if not metrics_cache["prometheus_output"]:
        sync_topics()
    
    if "gzip" in request.headers.get("accept-encoding", ""):
        return Response(
            content=metrics_cache["prometheus_gzip"],
            media_type="text/plain",
            headers={
                "Content-Encoding": "gzip",
                "Content-Length": str(len(metrics_cache["prometheus_gzip"]))
            }
        )
    
    return Response(content=metrics_cache["prometheus_output"], media_type="text/plain")


@app.get("/topics")
def api_topics(request: Request, state: int = None):
    """List topics, optionally filtered by state."""
    if not metrics_cache["topics"]:
        sync_topics()

    result = []
    for topic, (size, consumer_count, topic_state) in metrics_cache["topics"].items():
        if state is None or topic_state == state:
            result.append({
                "name": topic,
                "size": size,
                "consumer_count": consumer_count,
                "state": topic_state
            })

    if "gzip" in request.headers.get("accept-encoding", ""):
        return gzip_response(result)
    return result


@app.get("/topics/{topic_name}")
def api_topic_detail(topic_name: str, request: Request):
    """Get single topic details."""
    if not metrics_cache["topics"]:
        sync_topics()

    data = metrics_cache["topics"].get(topic_name)
    if not data:
        raise HTTPException(status_code=404, detail="Topic not found")

    size, consumer_count, state = data
    result = {
        "name": topic_name,
        "size": size,
        "consumer_count": consumer_count,
        "state": state
    }

    if "gzip" in request.headers.get("accept-encoding", ""):
        return gzip_response(result)
    return result


@app.get("/summary")
def api_summary(request: Request):
    """Summary by state."""
    if not metrics_cache["topics"]:
        sync_topics()

    counts = {0: 0, 1: 0, 2: 0, 3: 0}
    for _, (_, _, state) in metrics_cache["topics"].items():
        counts[state] += 1

    result = {
        "total": len(metrics_cache["topics"]),
        "healthy": counts[0],
        "empty_no_consumer": counts[1],
        "not_empty_no_consumer": counts[2],
        "empty_with_consumer": counts[3],
        "last_sync": metrics_cache["last_sync"].isoformat() if metrics_cache["last_sync"] else None
    }

    if "gzip" in request.headers.get("accept-encoding", ""):
        return gzip_response(result)
    return result


@app.get("/health")
def api_health():
    """Health check."""
    return {"status": "ok"}
