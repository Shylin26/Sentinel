import torch
import json
import sys
from pathlib import Path
from transformer import BPETokenizerWrapper
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
MODEL_PATH = ROOT_DIR / "model/checkpoints/sentinel.pt"
TOKENIZER_PATH = ROOT_DIR / "model/src/sentinel_bpe.model"
SEVERITIES = ["nit", "suggestion", "bug", "critical"]
CATEGORIES = [
    "style", "naming", "logic", "performance", "security",
    "error_handling", "testing", "documentation", "complexity",
    "duplication", "typing", "other"
]
MESSAGES = {
    "style":         "Consider reformatting for consistency.",
    "naming":        "Variable or function name could be more descriptive.",
    "logic":         "Potential logic error — review this block carefully.",
    "performance":   "This may cause performance issues at scale.",
    "security":      "Potential security vulnerability — review immediately.",
    "error_handling":"Missing or insufficient error handling.",
    "testing":       "This code path lacks test coverage.",
    "documentation": "Consider adding a docstring or inline comment.",
    "complexity":    "This block is complex — consider refactoring.",
    "duplication":   "Duplicated logic — consider extracting a helper.",
    "typing":        "Missing type annotations.",
    "other":         "Review this section.",
}
def load_model():
    model = torch.jit.load(str(MODEL_PATH), map_location="cpu")
    model.eval()
    return model

def chunk_diff(diff: str, max_len: int = 512) -> list[dict]:
    lines = diff.split("\n")
    chunks = []
    current = []
    current_lines = []
    start_line = 0

    for i, line in enumerate(lines):
        current.append(line)
        current_lines.append(i)
        if len(" ".join(current).split()) > 400:
            chunks.append({
                "text": "\n".join(current),
                "start_line": start_line,
                "end_line": i,
            })
            current = []
            current_lines = []
            start_line = i + 1

    if current:
        chunks.append({
            "text": "\n".join(current),
            "start_line": start_line,
            "end_line": len(lines) - 1,
        })
    return chunks

def review(diff: str) -> list[dict]:
    model = load_model()
    tokenizer = BPETokenizerWrapper(str(TOKENIZER_PATH))
    chunks = chunk_diff(diff)
    results = []

    with torch.no_grad():
        for chunk in chunks:
            ids, mask = tokenizer.batch_encode([chunk["text"]])
            out = model(ids, mask)

            sev_idx = out["severity"].argmax(-1).item()
            cat_idx = out["category"].argmax(-1).item()
            sev_conf = torch.softmax(out["severity"], dim=-1).max().item()
            cat_conf = torch.softmax(out["category"], dim=-1).max().item()

            if sev_idx == 0 and sev_conf < 0.6:
                continue

            category = CATEGORIES[cat_idx]
            results.append({
                "line": chunk["start_line"],
                "severity": SEVERITIES[sev_idx],
                "severity_id": sev_idx,
                "category": category,
                "message": MESSAGES[category],
                "confidence": round((sev_conf + cat_conf) / 2, 3),
            })

    results.sort(key=lambda x: x["severity_id"], reverse=True)
    return results

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 infer.py <diff_file>")
        sys.exit(1)

    diff_path = Path(sys.argv[1])
    if not diff_path.exists():
        print(f"File not found: {diff_path}")
        sys.exit(1)
    
    diff = diff_path.read_text()
    comments = review(diff)

    print(json.dumps(comments, indent=2))
    



