import json
import urllib.request
import sys
import time
import grpc
from pathlib import Path
from kafka import KafkaConsumer, KafkaProducer
import os
from dotenv import load_dotenv

load_dotenv()
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT_DIR / "proto"))

import inference_pb2
import inference_pb2_grpc

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}" if GITHUB_TOKEN else None,
    "Accept": "application/vnd.github.v3+json",
    "User-Agent": "SENTINEL",
}

# gRPC channel to inference server
channel = grpc.insecure_channel(os.environ.get("INFERENCE_GRPC_HOST", "127.0.0.1:50051"))
stub    = inference_pb2_grpc.InferenceServiceStub(channel)

def create_kafka_clients():
    while True:
        try:
            c = KafkaConsumer(
                "push-events",
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id="diff-extractors-v4",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            )
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
            )
            print("Kafka connected.")
            return c, p
        except Exception as e:
            print(f"Kafka not ready ({e}) — retrying in 5s...")
            time.sleep(5)

consumer, producer = create_kafka_clients()


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
                "filename":    f["filename"],
                "patch":       chunk,
                "chunk_index": i // 400,
            })
    return chunks


def process(msg):
    repo        = msg.get("repo", "")
    commit      = msg.get("commit", "")
    received_at = msg.get("received_at") or time.time()
    print(f"Processing {repo}@{commit[:7]}...")
    try:
        chunks = fetch_diff(repo, commit)
        for chunk in chunks:
            ikey    = commit + ":" + chunk["filename"] + ":" + str(chunk["chunk_index"])
            request = inference_pb2.ChunkRequest(
                repo            = repo,
                commit          = commit,
                filename        = chunk["filename"],
                patch           = chunk["patch"],
                idempotency_key = ikey,
                received_at     = received_at,
                chunk_index     = chunk["chunk_index"],
            )
            # Direct gRPC call — no Kafka round-trip
            response = stub.ReviewChunk(request, timeout=5)

            # Publish result straight to reviews topic
            review = {
                "repo":            response.repo,
                "commit":          response.commit,
                "filename":        response.filename,
                "line":            response.line,
                "severity":        response.severity,
                "severity_id":     response.severity_id,
                "category":        response.category,
                "message":         response.message,
                "confidence":      response.confidence,
                "idempotency_key": response.idempotency_key,
                "received_at":     response.received_at,
            }
            producer.send("reviews", key=repo, value=review)
            print(f"  -> {response.severity} | {response.category} | conf {response.confidence}")

        producer.flush()
        print(f"Processed {len(chunks)} chunks via gRPC")
    except grpc.RpcError as e:
        print(f"gRPC error: {e.code()} — {e.details()}")
    except Exception as e:
        print(f"Error: {e}")


print("Diff extractor running (gRPC mode)...")
for message in consumer:
    print(f"Got message: {message.value.get('repo')}")
    process(message.value)
