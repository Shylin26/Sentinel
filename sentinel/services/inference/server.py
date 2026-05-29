import os
import sys
import time
import json
import urllib.request
import torch
import grpc
from pathlib import Path
from concurrent import futures
from prometheus_client import Histogram, start_http_server

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT_DIR / "model" / "src"))
sys.path.insert(0, str(ROOT_DIR / "proto"))

from transformer import BPETokenizerWrapper
import inference_pb2
import inference_pb2_grpc

MODEL_PATH     = ROOT_DIR / "model/checkpoints/sentinel.pt"
TOKENIZER_PATH = ROOT_DIR / "model/src/sentinel_bpe.model"
GRPC_PORT      = 50051
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434/api/generate")
OLLAMA_MODEL   = "llama3.1:8b"

SEVERITIES = ["nit", "suggestion", "bug", "critical"]
CATEGORIES = [
    "style", "naming", "logic", "performance", "security",
    "error_handling", "testing", "documentation", "complexity",
    "duplication", "typing", "other"
]

# Fallback messages if Ollama is unavailable
FALLBACK_MESSAGES = {
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

# Prometheus
inference_latency = Histogram(
    "sentinel_inference_latency_seconds",
    "Time spent on transformer forward pass",
    buckets=[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0],
)

print("Loading model...")
model = torch.jit.load(str(MODEL_PATH), map_location="cpu")
model.eval()
tokenizer = BPETokenizerWrapper(str(TOKENIZER_PATH))
print("Model loaded.")


def generate_explanation(patch: str, severity: str, category: str) -> str:
    """Call Ollama to generate a specific explanation for the code issue."""
    prompt = (
        f"You are a senior code reviewer. A ML model classified this code diff as "
        f"a {severity.upper()} severity {category} issue.\n\n"
        f"Code diff:\n```\n{patch[:500]}\n```\n\n"
        f"Write ONE specific sentence explaining exactly what the problem is and how to fix it. "
        f"Be concrete, reference the actual code. No preamble, no bullet points, just the sentence."
    )
    try:
        body = json.dumps({
            "model": OLLAMA_MODEL,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0.3, "num_predict": 80},
        }).encode()
        req = urllib.request.Request(
            OLLAMA_URL,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=8) as r:
            result = json.loads(r.read())
            return result.get("response", "").strip()
    except Exception as e:
        print(f"  Ollama unavailable ({e}) — using fallback message")
        return FALLBACK_MESSAGES.get(category, "Review this section.")


class InferenceServicer(inference_pb2_grpc.InferenceServiceServicer):

    def ReviewChunk(self, request, context):
        ids, mask = tokenizer.batch_encode([request.patch])
        t0 = time.perf_counter()
        with torch.no_grad():
            out = model(ids, mask)
        inference_latency.observe(time.perf_counter() - t0)

        sev_probs  = torch.softmax(out["severity"], dim=-1)
        cat_probs  = torch.softmax(out["category"],  dim=-1)
        sev_idx    = sev_probs[0].argmax().item()
        cat_idx    = cat_probs[0].argmax().item()
        sev_conf   = sev_probs[0].max().item()
        cat_conf   = cat_probs[0].max().item()
        severity   = SEVERITIES[sev_idx]
        category   = CATEGORIES[cat_idx]
        confidence = round((sev_conf + cat_conf) / 2, 3)

        # Generate specific explanation via Ollama
        message = generate_explanation(request.patch, severity, category)

        print(f"  gRPC: {request.repo}@{request.commit[:7]} | {severity} | {category} | conf {confidence}")
        print(f"  msg: {message[:80]}...")

        return inference_pb2.ReviewResponse(
            repo            = request.repo,
            commit          = request.commit,
            filename        = request.filename,
            line            = request.chunk_index * 20,
            severity        = severity,
            severity_id     = sev_idx,
            category        = category,
            message         = message,
            confidence      = confidence,
            idempotency_key = request.idempotency_key,
            received_at     = request.received_at,
        )


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    inference_pb2_grpc.add_InferenceServiceServicer_to_server(InferenceServicer(), server)
    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    server.start()
    print(f"gRPC inference server listening on :{GRPC_PORT}")
    server.wait_for_termination()


print("Inference server — metrics on :9101")
start_http_server(9101)
serve()
