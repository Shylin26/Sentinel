import json
import urllib.request
import urllib.error
import os
import time
import redis
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv
from prometheus_client import Counter, Histogram, start_http_server

load_dotenv()

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "your_token_here")
HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json",
    "User-Agent": "SENTINEL",
    "Content-Type": "application/json",
}

MAX_RETRIES = 3   # send to DLQ after this many failures

# Prometheus metrics
review_posted = Counter(
    "sentinel_review_posted_total",
    "Total reviews posted to GitHub",
    ["repo", "severity", "status"],  # status: posted | skipped | failed | duplicate | dlq
)
e2e_latency = Histogram(
    "sentinel_e2e_latency_seconds",
    "Webhook received to GitHub comment posted",
    buckets=[0.1, 0.25, 0.5, 1.0, 1.5, 2.0, 3.0, 5.0],
)

# Redis idempotency cache
try:
    cache = redis.Redis(host="127.0.0.1", port=6379, db=0, socket_connect_timeout=2)
    cache.ping()
    CACHE_AVAILABLE = True
    print("Redis connected — idempotency cache active.")
except Exception:
    cache = None
    CACHE_AVAILABLE = False
    print("Redis unavailable — idempotency cache disabled.")

CACHE_TTL = 86400  # 24 hours

consumer = KafkaConsumer(
    "reviews",
    bootstrap_servers="127.0.0.1:9092",
    group_id="result-publishers",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)
producer = KafkaProducer(
    bootstrap_servers="127.0.0.1:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)

SEVERITY_EMOJI = {
    "nit":        "💬",
    "suggestion": "💡",
    "bug":        "🐛",
    "critical":   "🚨",
}


def format_comment(review: dict) -> str:
    emoji = SEVERITY_EMOJI.get(review["severity"], "💬")
    return (
        f"{emoji} **SENTINEL** [{review['severity'].upper()}] "
        f"— {review['category']}\n\n"
        f"{review['message']}\n\n"
        f"*Confidence: {review['confidence']} | "
        f"File: `{review['filename']}`*"
    )


def post_commit_comment(repo: str, commit: str, comment: str):
    url = f"https://api.github.com/repos/{repo}/commits/{commit}/comments"
    body = json.dumps({"body": comment}).encode()
    req = urllib.request.Request(url, data=body, headers=HEADERS, method="POST")
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def is_duplicate(idempotency_key: str) -> bool:
    if not CACHE_AVAILABLE:
        return False
    return cache.exists(f"review:{idempotency_key}") == 1


def mark_posted(idempotency_key: str):
    if CACHE_AVAILABLE:
        cache.set(f"review:{idempotency_key}", 1, ex=CACHE_TTL)


def send_to_dlq(review: dict, reason: str):
    """Send a review to the dead letter queue after MAX_RETRIES failures."""
    dlq_msg = {**review, "dlq_reason": reason, "dlq_at": time.time()}
    producer.send("reviews-dlq", key=review["repo"], value=dlq_msg)
    producer.flush()
    print(f"  DLQ: {review['repo']}@{review['commit'][:7]} — {reason}")
    review_posted.labels(repo=review["repo"], severity=review["severity"], status="dlq").inc()


def process(review: dict):
    repo        = review["repo"]
    commit      = review["commit"]
    severity    = review["severity"]
    confidence  = review["confidence"]
    received_at = review.get("received_at")
    ikey        = review.get("idempotency_key", "")
    retries     = review.get("_retries", 0)

    # Idempotency check
    if is_duplicate(ikey):
        print(f"  Duplicate — skipping {ikey[:40]}...")
        review_posted.labels(repo=repo, severity=severity, status="duplicate").inc()
        return

    if confidence < 0.5:
        print(f"  Skipping low confidence review ({confidence})")
        review_posted.labels(repo=repo, severity=severity, status="skipped").inc()
        return

    comment = format_comment(review)
    print(f"Posting to {repo}@{commit[:7]} — {severity}...")
    try:
        result = post_commit_comment(repo, commit, comment)
        mark_posted(ikey)
        if received_at:
            e2e_latency.observe(time.time() - received_at)
        review_posted.labels(repo=repo, severity=severity, status="posted").inc()
        print(f"  Posted comment id {result['id']}")

    except urllib.error.HTTPError as e:
        retries += 1
        print(f"  Failed ({retries}/{MAX_RETRIES}): {e.code} {e.reason}")
        if retries >= MAX_RETRIES:
            send_to_dlq(review, f"HTTP {e.code} {e.reason}")
        else:
            # Re-enqueue with incremented retry count
            retry_msg = {**review, "_retries": retries}
            producer.send("reviews", key=repo, value=retry_msg)
            producer.flush()
            review_posted.labels(repo=repo, severity=severity, status="failed").inc()

    except Exception as e:
        retries += 1
        print(f"  Error ({retries}/{MAX_RETRIES}): {e}")
        if retries >= MAX_RETRIES:
            send_to_dlq(review, str(e))
        else:
            retry_msg = {**review, "_retries": retries}
            producer.send("reviews", key=repo, value=retry_msg)
            producer.flush()
            review_posted.labels(repo=repo, severity=severity, status="failed").inc()


print("Result publisher — metrics on :9102")
start_http_server(9102)
print("Result publisher running...")
for message in consumer:
    process(message.value)
