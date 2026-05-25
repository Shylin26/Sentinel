import json
import urllib.request
import urllib.error
import os
import time                                                              # NEW
from kafka import KafkaConsumer
from dotenv import load_dotenv
from prometheus_client import Counter, Histogram, start_http_server     # NEW

load_dotenv()

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "your_token_here")
HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json",
    "User-Agent": "SENTINEL",
    "Content-Type": "application/json",
}

review_posted = Counter(
    "sentinel_review_posted_total",
    "Total reviews posted to GitHub",
    ["repo", "severity", "status"],  # status: posted | skipped | failed
)
e2e_latency = Histogram(
    "sentinel_e2e_latency_seconds",
    "Webhook received to GitHub comment posted",
    buckets=[0.1, 0.25, 0.5, 1.0, 1.5, 2.0, 3.0, 5.0],
)


consumer = KafkaConsumer(
    "reviews",
    bootstrap_servers="127.0.0.1:9092",
    group_id="result-publishers",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

SEVERITY_EMOJI = {
    "nit": "💬",
    "suggestion": "💡",
    "bug": "🐛",
    "critical": "🚨",
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


def process(review: dict):
    repo = review["repo"]
    commit = review["commit"]
    severity = review["severity"]
    confidence = review["confidence"]
    received_at = review.get("received_at")                             # NEW

    if confidence < 0.5:
        print(f"  Skipping low confidence review ({confidence})")
        review_posted.labels(repo=repo, severity=severity, status="skipped").inc()  # NEW
        return

    comment = format_comment(review)
    print(f"Posting to {repo}@{commit[:7]} — {severity}...")
    try:
        result = post_commit_comment(repo, commit, comment)
        if received_at:                                                 # NEW
            e2e_latency.observe(time.time() - received_at)             # NEW
        review_posted.labels(repo=repo, severity=severity, status="posted").inc()  # NEW
        print(f"  Posted comment id {result['id']}")
    except urllib.error.HTTPError as e:
        review_posted.labels(repo=repo, severity=severity, status="failed").inc()  # NEW
        print(f"  Failed: {e.code} {e.reason}")


print("Result publisher — metrics on :9102")                            # NEW
start_http_server(9102)                                                 # NEW
print("Result publisher running...")
for message in consumer:
    process(message.value)