import json
import sys
import torch
from pathlib import Path
from kafka import KafkaConsumer, KafkaProducer

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
)

producer = KafkaProducer(
    bootstrap_servers="127.0.0.1:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)

def run_inference(patch: str) -> dict:
    ids, mask = tokenizer.batch_encode([patch])
    with torch.no_grad():
        out = model(ids, mask)
    sev_idx = out["severity"].argmax(-1).item()
    cat_idx = out["category"].argmax(-1).item()
    sev_conf = torch.softmax(out["severity"], dim=-1).max().item()
    cat_conf = torch.softmax(out["category"], dim=-1).max().item()
    category = CATEGORIES[cat_idx]
    return {
        "severity": SEVERITIES[sev_idx],
        "severity_id": sev_idx,
        "category": category,
        "message": MESSAGES[category],
        "confidence": round((sev_conf + cat_conf) / 2, 3),
    }

def process(msg: dict):
    repo = msg["repo"]
    commit = msg["commit"]
    filename = msg["filename"]
    patch = msg["patch"]
    idempotency_key = msg["idempotency_key"]

    print(f"Inferring {repo}@{commit[:7]} — {filename}...")

    result = run_inference(patch)

    review = {
        "repo": repo,
        "commit": commit,
        "filename": filename,
        "line": msg.get("chunk_index", 0) * 20,
        "severity": result["severity"],
        "severity_id": result["severity_id"],
        "category": result["category"],
        "message": result["message"],
        "confidence": result["confidence"],
        "idempotency_key": idempotency_key,
    }

    producer.send("reviews", key=repo, value=review)
    producer.flush()
    print(f"  → {result['severity']} | {result['category']} | confidence {result['confidence']}")

print("Inference worker running...")
for message in consumer:
    process(message.value)