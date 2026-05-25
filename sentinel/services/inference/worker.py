import json
import sys
import time
import torch
from pathlib import Path
from kafka import KafkaConsumer, KafkaProducer
from prometheus_client import Histogram, start_http_server

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT_DIR / "model" / "src"))
from transformer import BPETokenizerWrapper

MODEL_PATH = ROOT_DIR / "model/checkpoints/sentinel.pt"
TOKENIZER_PATH = ROOT_DIR / "model/src/sentinel_bpe.model"

SEVERITIES = ["nit", "suggestion", "bug", "critical"]
CATEGORIES = [
    "style", "naming", "logic", "performance", "security",
    "error_handling", "testing", "documentation", "complexity",
    "duplication", "typing", "other"
]
MESSAGES = {
    "style":          "Consider reformatting for consistency.",
    "naming":         "Variable or function name could be more descriptive.",
    "logic":          "Potential logic error — review this block carefully.",
    "performance":    "This may cause performance issues at scale.",
    "security":       "Potential security vulnerability — review immediately.",
    "error_handling": "Missing or insufficient error handling.",
    "testing":        "This code path lacks test coverage.",
    "documentation":  "Consider adding a docstring or inline comment.",
    "complexity":     "This block is complex — consider refactoring.",
    "duplication":    "Duplicated logic — consider extracting a helper.",
    "typing":         "Missing type annotations.",
    "other":          "Review this section.",
}

# Prometheus metrics
inference_latency = Histogram(
    "sentinel_inference_latency_seconds",
    "Time spent on transformer forward pass",
    buckets=[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0],
)
batch_size_histogram = Histogram(
    "sentinel_batch_size",
    "Number of chunks processed per forward pass",
    buckets=[1, 2, 4, 8, 16, 32],
)

# Batching config
BATCH_WINDOW_MS = 20      # wait up to 20ms to fill a batch
MAX_BATCH_SIZE  = 32      # never exceed 32 chunks per forward pass

print("Loading model...")
model = torch.jit.load(str(MODEL_PATH), map_location="cpu")
model.eval()
tokenizer = BPETokenizerWrapper(str(TOKENIZER_PATH))
print("Model loaded.")

consumer = KafkaConsumer(
    "diff-chunks",
    bootstrap_servers="127.0.0.1:9092",
    group_id="inference-workers",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    # poll returns as soon as data is available, up to this timeout
    consumer_timeout_ms=BATCH_WINDOW_MS,
)
producer = KafkaProducer(
    bootstrap_servers="127.0.0.1:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)


def run_batch_inference(patches: list) -> list:
    """Run one forward pass on a list of patches. Returns one result per patch."""
    ids, mask = tokenizer.batch_encode(patches)
    t0 = time.perf_counter()
    with torch.no_grad():
        out = model(ids, mask)
    inference_latency.observe(time.perf_counter() - t0)
    batch_size_histogram.observe(len(patches))

    results = []
    sev_probs = torch.softmax(out["severity"], dim=-1)
    cat_probs = torch.softmax(out["category"],  dim=-1)

    for i in range(len(patches)):
        sev_idx  = sev_probs[i].argmax().item()
        cat_idx  = cat_probs[i].argmax().item()
        sev_conf = sev_probs[i].max().item()
        cat_conf = cat_probs[i].max().item()
        category = CATEGORIES[cat_idx]
        results.append({
            "severity":    SEVERITIES[sev_idx],
            "severity_id": sev_idx,
            "category":    category,
            "message":     MESSAGES[category],
            "confidence":  round((sev_conf + cat_conf) / 2, 3),
        })
    return results


def collect_batch() -> list:
    """
    Collect messages for up to BATCH_WINDOW_MS ms OR until MAX_BATCH_SIZE.
    Returns a list of raw message dicts.
    """
    batch = []
    deadline = time.perf_counter() + BATCH_WINDOW_MS / 1000.0

    for message in consumer:
        batch.append(message.value)
        if len(batch) >= MAX_BATCH_SIZE:
            break
        if time.perf_counter() >= deadline:
            break

    return batch


def process_batch(msgs: list):
    patches = [m["patch"] for m in msgs]
    results = run_batch_inference(patches)

    for msg, result in zip(msgs, results):
        review = {
            "repo":            msg["repo"],
            "commit":          msg["commit"],
            "filename":        msg["filename"],
            "line":            msg.get("chunk_index", 0) * 20,
            "severity":        result["severity"],
            "severity_id":     result["severity_id"],
            "category":        result["category"],
            "message":         result["message"],
            "confidence":      result["confidence"],
            "idempotency_key": msg["idempotency_key"],
            "received_at":     msg.get("received_at"),
        }
        producer.send("reviews", key=msg["repo"], value=review)
        print(f"  -> {msg['repo']}@{msg['commit'][:7]} | {result['severity']} | {result['category']} | conf {result['confidence']}")

    producer.flush()
    print(f"Batch of {len(msgs)} processed.")


print("Inference worker - metrics on :9101")
start_http_server(9101)
print("Inference worker running...")

while True:
    batch = collect_batch()
    if batch:
        process_batch(batch)
