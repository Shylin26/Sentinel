import json
import urllib.request
from kafka import KafkaConsumer, KafkaProducer
import os
from dotenv import load_dotenv

load_dotenv()

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}" if GITHUB_TOKEN else None,
    "Accept": "application/vnd.github.v3+json",
    "User-Agent": "SENTINEL",
}

consumer = KafkaConsumer(
    "push-events",
    bootstrap_servers="127.0.0.1:9092",
    group_id="diff-extractors-v4",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)
producer = KafkaProducer(
    bootstrap_servers="127.0.0.1:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)


def fetch_diff(repo, commit):
    url = f"https://api.github.com/repos/{repo}/commits/{commit}"
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=10) as r:
        data = json.loads(r.read())
    files = data.get("files", [])
    chunks = []
    for f in files:
        patch = f.get("patch", "")
        if not patch:
            continue
        words = patch.split()
        for i in range(0, len(words), 400):
            chunk = " ".join(words[i:i+400])
            chunks.append({
                "filename": f["filename"],
                "patch": chunk,
                "chunk_index": i // 400,
            })
    return chunks


def process(msg):
    repo = msg.get("repo", "")
    commit = msg.get("commit", "")
    received_at = msg.get("received_at")   # NEW
    print(f"Processing {repo}@{commit[:7]}...")
    try:
        chunks = fetch_diff(repo, commit)
        for chunk in chunks:
            ikey = commit + ":" + chunk["filename"] + ":" + str(chunk["chunk_index"])
            payload = {
                "repo": repo,
                "commit": commit,
                "filename": chunk["filename"],
                "patch": chunk["patch"],
                "chunk_index": chunk["chunk_index"],
                "idempotency_key": ikey,
                "received_at": received_at,   # NEW
            }
            producer.send("diff-chunks", key=repo, value=payload)
        producer.flush()
        print(f"Published {len(chunks)} chunks")
    except Exception as e:
        print(f"Error: {e}")


print("Diff extractor running...")
for message in consumer:
    print(f"Got message: {message.value.get('repo')}")
    process(message.value)