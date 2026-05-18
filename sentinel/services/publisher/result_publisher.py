import json
import urllib.request
import urllib.error
import os
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "your_token_here")
HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json",
    "User-Agent": "SENTINEL",
    "Content-Type": "application/json",
}

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

    if confidence < 0.5:
        print(f"  Skipping low confidence review ({confidence})")
        return

    comment = format_comment(review)
    print(f"Posting to {repo}@{commit[:7]} — {severity}...")

    try:
        result = post_commit_comment(repo, commit, comment)
        print(f" Posted comment id {result['id']}")
    except urllib.error.HTTPError as e:
        print(f" Failed: {e.code} {e.reason}")

print("Result publisher running...")
for message in consumer:
    process(message.value)