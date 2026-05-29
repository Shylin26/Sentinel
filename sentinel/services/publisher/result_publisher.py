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

MAX_RETRIES = 3

# Prometheus metrics
review_posted = Counter(
    "sentinel_review_posted_total",
    "Total reviews posted to GitHub",
    ["repo", "severity", "status"],
)
e2e_latency = Histogram(
    "sentinel_e2e_latency_seconds",
    "Webhook received to GitHub comment posted",
    buckets=[0.1, 0.25, 0.5, 1.0, 1.5, 2.0, 3.0, 5.0],
)

# Redis idempotency cache
try:
    cache = redis.Redis(host=os.environ.get("REDIS_HOST", "127.0.0.1"), port=6379, db=0, socket_connect_timeout=2)
    cache.ping()
    CACHE_AVAILABLE = True
    print("Redis connected — idempotency cache active.")
except Exception:
    cache = None
    CACHE_AVAILABLE = False
    print("Redis unavailable — idempotency cache disabled.")

CACHE_TTL = 86400

def create_kafka_clients():
    while True:
        try:
            c = KafkaConsumer(
                "reviews",
                bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
                group_id="result-publishers",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            )
            p = KafkaProducer(
                bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
            )
            print("Kafka connected.")
            return c, p
        except Exception as e:
            print(f"Kafka not ready ({e}) — retrying in 5s...")
            time.sleep(5)

consumer, producer = create_kafka_clients()

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

def get_pr_for_commit(repo: str, commit: str):
    """Find an open PR that contains this commit."""
    url = f"https://api.github.com/repos/{repo}/commits/{commit}/pulls"
    req = urllib.request.Request(url, headers={
        **HEADERS,
        "Accept": "application/vnd.github.groot-preview+json",
    })
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            prs = json.loads(r.read())
            return prs[0] if prs else None
    except Exception:
        return None


def post_pr_review_comment(repo: str, pr_number: int, commit: str,
                            filename: str, line: int, comment: str):
    """Post an inline review comment on a PR diff."""
    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/comments"
    body = json.dumps({
        "body": comment,
        "commit_id": commit,
        "path": filename,
        "line": max(line, 1),
        "side": "RIGHT",
    }).encode()
    req = urllib.request.Request(url, data=body, headers=HEADERS, method="POST")
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def post_commit_comment(repo: str, commit: str, comment: str):
    """Fallback: post a commit-level comment."""
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
    filename    = review["filename"]
    line        = review.get("line", 1)
    received_at = review.get("received_at")
    ikey        = review.get("idempotency_key", "")
    retries     = review.get("_retries", 0)

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
        # Try inline PR comment first
        # Post as commit comment (inline PR requires exact diff position)
        result = post_commit_comment(repo, commit, comment)
        print(f"  Posted commit comment id {result['id']}")

        mark_posted(ikey)
        if received_at:
            e2e_latency.observe(time.time() - received_at)
        review_posted.labels(repo=repo, severity=severity, status="posted").inc()

    except urllib.error.HTTPError as e:
        retries += 1
        print(f"  Failed ({retries}/{MAX_RETRIES}): {e.code} {e.reason}")
        if retries >= MAX_RETRIES:
            send_to_dlq(review, f"HTTP {e.code} {e.reason}")
        else:
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
