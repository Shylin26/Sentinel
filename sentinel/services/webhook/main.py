import hmac
import hashlib
import json
import os
import time                                                    # NEW
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, Response           # NEW
from kafka import KafkaProducer
from dotenv import load_dotenv
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST  # NEW

load_dotenv()

app = FastAPI()

GITHUB_SECRET = os.environ.get("GITHUB_WEBHOOK_SECRET", "sentinel_secret")

# NEW -----------------------------------------------------------------------
webhook_received = Counter(
    "sentinel_webhook_received_total",
    "Total webhook events received",
    ["repo", "event", "status"],  # status: queued | invalid_sig | kafka_error | ignored
)
# ---------------------------------------------------------------------------

producer = KafkaProducer(
    bootstrap_servers="127.0.0.1:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    acks="all",
    retries=3,
)


def verify_signature(payload: bytes, signature: str) -> bool:
    expected = "sha256=" + hmac.new(
        GITHUB_SECRET.encode(),
        payload,
        hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


@app.post("/webhook")
async def webhook(request: Request):
    signature = request.headers.get("X-Hub-Signature-256", "")
    payload = await request.body()

    if not verify_signature(payload, signature):
        webhook_received.labels(repo="unknown", event="unknown", status="invalid_sig").inc()  # NEW
        raise HTTPException(status_code=401, detail="Invalid signature")

    event = request.headers.get("X-GitHub-Event", "")
    data = json.loads(payload)
    repo = data.get("repository", {}).get("full_name", "unknown")

    if event == "ping":
        webhook_received.labels(repo=repo, event="ping", status="ignored").inc()  # NEW
        return JSONResponse({"msg": "pong"})

    if event not in ("push", "pull_request"):
        webhook_received.labels(repo=repo, event=event, status="ignored").inc()  # NEW
        return JSONResponse({"msg": "ignored"})

    commit = data.get("after", data.get("pull_request", {}).get("head", {}).get("sha", ""))

    message = {
        "event": event,
        "repo": repo,
        "commit": commit,
        "received_at": time.time(),   # NEW — threads through to publisher for e2e latency
        "payload": data,
    }

    try:
        future = producer.send("push-events", key=repo, value=message)
        result = future.get(timeout=5)
        print(f"Kafka: partition {result.partition} offset {result.offset}")
    except Exception as e:
        webhook_received.labels(repo=repo, event=event, status="kafka_error").inc()  # NEW
        print(f"Kafka error: {e}")
        raise HTTPException(status_code=500, detail=f"Kafka error: {e}")

    webhook_received.labels(repo=repo, event=event, status="queued").inc()  # NEW
    print(f"Queued — Event: {event} | Repo: {repo} | Commit: {commit[:7]}")
    return JSONResponse({
        "status": "queued",
        "repo": repo,
        "commit": commit,
        "event": event,
    })


@app.get("/health")
async def health():
    return {"status": "ok"}


# NEW -----------------------------------------------------------------------
@app.get("/metrics")
async def metrics():
    """Prometheus scrape endpoint on the same :8000 port as the webhook."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
# ---------------------------------------------------------------------------