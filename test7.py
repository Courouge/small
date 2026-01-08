import os
import gzip
import httpx
import asyncio
from datetime import datetime
from io import StringIO
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response, StreamingResponse
import json
from typing import AsyncIterator

# Configuration
KAPI_URL = os.getenv("KAPI_URL", "URL")
KAPI_USER = os.getenv("KAPI_USER", "KAPI_USER")
KAPI_PASSWORD = os.getenv("KAPI_PASSWORD", "KAPI_PASSWORD")
CA_CERT = os.getenv("CA_CERT", "/etc/ssl/certs/ca-certificates.crt")

# MongoDB (optionnel)
MONGO_ENABLED = os.getenv("MONGO_ENABLED", "false").lower() == "true"
if MONGO_ENABLED:
    from motor.motor_asyncio import AsyncIOMotorClient
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    MONGO_DB = os.getenv("MONGO_DB", "kafka")
    mongo_client = AsyncIOMotorClient(MONGO_URI)
    db = mongo_client[MONGO_DB]
    topics_collection = db["topics"]

# Batch size pour MongoDB
MONGO_BATCH_SIZE = int(os.getenv("MONGO_BATCH_SIZE", "1000"))

app = FastAPI(title="KAPI Monitor")

# Cache optimisé
metrics_cache = {
    "topics": {},
    "last_sync": None,
    "prometheus_gzip": None,
    "stats": {
        "total": 0,
        "by_state": {0: 0, 1: 0, 2: 0, 3: 0}
    }
}

# HTTP client
http_client: httpx.AsyncClient = None


@app.on_event("startup")
async def startup():
    global http_client
    http_client = httpx.AsyncClient(verify=CA_CERT, timeout=60.0)


@app.on_event("shutdown")
async def shutdown():
    await http_client.aclose()


def gzip_compress(data: bytes | str, level: int = 6) -> bytes:
    if isinstance(data, str):
        data = data.encode("utf-8")
    return gzip.compress(data, compresslevel=level)


def gzip_response(data, media_type: str = "application/json") -> Response:
    if isinstance(data, (dict, list)):
        content = json.dumps(data).encode("utf-8")
    elif isinstance(data, str):
        content = data.encode("utf-8")
    else:
        content = data

    compressed = gzip.compress(content, compresslevel=6)
    return Response(
        content=compressed,
        media_type=media_type,
        headers={
            "Content-Encoding": "gzip",
            "Content-Length": str(len(compressed))
        }
    )


async def get_access_token() -> str:
    url = f"{KAPI_URL}/v3/auth/token/"
    payload = {
        "grant_type": "",
        "username": KAPI_USER,
        "password": KAPI_PASSWORD,
        "scope": "",
        "client_id": "",
        "client_secret": ""
    }
    response = await http_client.post(url, data=payload)
    response.raise_for_status()
    return response.json()["access_token"]


def get_topic_consumers(cgroups: list) -> dict:
    topic_consumers = {}
    for group in cgroups:
        for member in group.get("members", []):
            for sub_topic in member.get("subscription", []):
                topic_consumers[sub_topic] = topic_consumers.get(sub_topic, 0) + 1
    return topic_consumers


def classify_topic(size: int, consumer_count: int) -> int:
    if size == 0:
        return 1 if consumer_count == 0 else 3
    return 2 if consumer_count == 0 else 0


def generate_prometheus_metrics_streaming(topics: dict) -> bytes:
    """Generate and compress Prometheus metrics in chunks."""
    buf = StringIO()
    buf.write("# HELP kafka_topic_info Topic info\n")
    buf.write("# TYPE kafka_topic_info gauge\n")

    chunk_size = 5000
    chunks = []
    count = 0

    for topic, (size, consumer_count, state) in topics.items():
        safe_topic = topic.replace("\\", "\\\\").replace('"', '\\"')
        buf.write(f'kafka_topic_info{{topic="{safe_topic}",type="size"}} {size}\n')
        buf.write(f'kafka_topic_info{{topic="{safe_topic}",type="consumers"}} {consumer_count}\n')
        buf.write(f'kafka_topic_info{{topic="{safe_topic}",type="state"}} {state}\n')

        count += 1
        if count % chunk_size == 0:
            chunks.append(buf.getvalue())
            buf = StringIO()

    if buf.getvalue():
        chunks.append(buf.getvalue())

    full_output = "".join(chunks)
    return gzip_compress(full_output)


async def bulk_upsert_mongo(topics: dict, now: datetime):
    """Bulk upsert to MongoDB in batches."""
    from pymongo import UpdateOne

    operations = []
    for topic, (size, consumer_count, state) in topics.items():
        operations.append(
            UpdateOne(
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
        )

        if len(operations) >= MONGO_BATCH_SIZE:
            await topics_collection.bulk_write(operations, ordered=False)
            operations = []

    if operations:
        await topics_collection.bulk_write(operations, ordered=False)


async def sync_topics():
    access_token = await get_access_token()
    headers = {"Authorization": f"Bearer {access_token}"}

    # Requêtes parallèles
    topics_response, cgroups_response = await asyncio.gather(
        http_client.get(f"{KAPI_URL}/v3/topics/size", headers=headers),
        http_client.get(f"{KAPI_URL}/v3/cgroups", headers=headers)
    )

    topics_size = topics_response.json()
    cgroups = cgroups_response.json()
    topic_consumers = get_topic_consumers(cgroups)

    now = datetime.utcnow()
    topics = {}
    stats = {0: 0, 1: 0, 2: 0, 3: 0}

    for topic, size in topics_size.items():
        consumer_count = topic_consumers.get(topic, 0)
        state = classify_topic(size, consumer_count)
        topics[topic] = (size, consumer_count, state)
        stats[state] += 1

    # MongoDB bulk (optionnel)
    if MONGO_ENABLED:
        await bulk_upsert_mongo(topics, now)

    # Update cache
    metrics_cache["topics"] = topics
    metrics_cache["last_sync"] = now
    metrics_cache["prometheus_gzip"] = generate_prometheus_metrics_streaming(topics)
    metrics_cache["stats"] = {
        "total": len(topics),
        "by_state": stats
    }

    return topics


@app.post("/sync")
async def api_sync(request: Request):
    topics = await sync_topics()
    data = {
        "status": "ok",
        "topics_count": len(topics),
        "synced_at": metrics_cache["last_sync"].isoformat()
    }

    if "gzip" in request.headers.get("accept-encoding", ""):
        return gzip_response(data)
    return data


@app.get("/metrics")
async def api_metrics(request: Request):
    if not metrics_cache["prometheus_gzip"]:
        await sync_topics()

    # Toujours retourner gzip (Prometheus le supporte)
    return Response(
        content=metrics_cache["prometheus_gzip"],
        media_type="text/plain",
        headers={
            "Content-Encoding": "gzip",
            "Content-Length": str(len(metrics_cache["prometheus_gzip"]))
        }
    )


@app.get("/topics")
async def api_topics(
    request: Request,
    state: int = None,
    limit: int = 1000,
    offset: int = 0
):
    """List topics with pagination."""
    if not metrics_cache["topics"]:
        await sync_topics()

    filtered = [
        {"name": topic, "size": size, "consumer_count": cc, "state": st}
        for topic, (size, cc, st) in metrics_cache["topics"].items()
        if state is None or st == state
    ]

    # Pagination
    total = len(filtered)
    result = {
        "total": total,
        "limit": limit,
        "offset": offset,
        "data": filtered[offset:offset + limit]
    }

    if "gzip" in request.headers.get("accept-encoding", ""):
        return gzip_response(result)
    return result


@app.get("/topics/stream")
async def api_topics_stream(state: int = None):
    """Stream topics as NDJSON for large datasets."""
    if not metrics_cache["topics"]:
        await sync_topics()

    async def generate() -> AsyncIterator[bytes]:
        for topic, (size, cc, st) in metrics_cache["topics"].items():
            if state is None or st == state:
                line = json.dumps({
                    "name": topic,
                    "size": size,
                    "consumer_count": cc,
                    "state": st
                }) + "\n"
                yield line.encode("utf-8")

    return StreamingResponse(generate(), media_type="application/x-ndjson")


@app.get("/topics/{topic_name}")
async def api_topic_detail(topic_name: str, request: Request):
    if not metrics_cache["topics"]:
        await sync_topics()

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
async def api_summary(request: Request):
    """Fast summary from pre-computed stats."""
    if not metrics_cache["topics"]:
        await sync_topics()

    stats = metrics_cache["stats"]
    result = {
        "total": stats["total"],
        "healthy": stats["by_state"][0],
        "empty_no_consumer": stats["by_state"][1],
        "not_empty_no_consumer": stats["by_state"][2],
        "empty_with_consumer": stats["by_state"][3],
        "last_sync": metrics_cache["last_sync"].isoformat() if metrics_cache["last_sync"] else None
    }

    if "gzip" in request.headers.get("accept-encoding", ""):
        return gzip_response(result)
    return result


@app.get("/health")
async def api_health():
    return {"status": "ok"}
