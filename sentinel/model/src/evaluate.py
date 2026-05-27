"""
SENTINEL Model Evaluation
Splits data 80/20, runs inference on held-out test set, prints confusion matrix.

Run from sentinel/ directory:
  python3 model/src/evaluate.py
"""
import json
import torch
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT_DIR / "model" / "src"))

from transformer import SentinelTransformer, BPETokenizerWrapper

DEVICE = torch.device("mps" if torch.backends.mps.is_available() else "cpu")
MODEL_PATH    = ROOT_DIR / "model/checkpoints/best_model.pt"
TOKENIZER_PATH = ROOT_DIR / "model/src/sentinel_bpe.model"
DATA_PATH     = ROOT_DIR / "data/raw/pr_review_comments.jsonl"

SEVERITIES = ["nit", "suggestion", "bug", "critical"]
CATEGORIES = [
    "style", "naming", "logic", "performance", "security",
    "error_handling", "testing", "documentation", "complexity",
    "duplication", "typing", "other"
]

def infer_category(comment):
    c = comment.lower()
    rules = [
        ("security",      ["security", "vulnerability", "injection", "auth", "exploit"]),
        ("performance",   ["performance", "slow", "efficient", "cache", "complexity", "o(n"]),
        ("error_handling",["exception", "error", "try", "catch", "handle", "raise"]),
        ("testing",       ["test", "assert", "mock", "coverage", "unittest"]),
        ("typing",        ["type", "annotation", "hint", "mypy", "cast"]),
        ("documentation", ["comment", "docstring", "doc", "readme", "explain"]),
        ("naming",        ["name", "rename", "variable", "function", "class", "called"]),
        ("complexity",    ["complex", "simplify", "refactor", "readable", "clean"]),
        ("duplication",   ["duplicate", "repeat", "reuse", "dry", "copy"]),
        ("logic",         ["logic", "incorrect", "wrong", "bug", "broken", "fails"]),
        ("style",         ["style", "format", "indent", "whitespace", "nit"]),
    ]
    for cat, keywords in rules:
        if any(kw in c for kw in keywords):
            return CATEGORIES.index(cat)
    return CATEGORIES.index("other")


def load_data():
    examples = []
    with open(DATA_PATH) as f:
        for line in f:
            ex = json.loads(line)
            examples.append(ex)
    return examples


def get_label(ex):
    severity = min(int(ex["severity"]), 3)
    category = infer_category(ex["comment"])
    return severity, category


def run_evaluation():
    print("Loading data...")
    examples = load_data()
    total = len(examples)

    # 80/20 split — deterministic, no shuffle
    split = int(total * 0.8)
    test_examples = examples[split:]
    print(f"Total: {total} | Train: {split} | Test: {len(test_examples)}")

    print("Loading model...")
    tokenizer = BPETokenizerWrapper(str(TOKENIZER_PATH))

    # Load TorchScript model
    model = torch.jit.load(str(ROOT_DIR / "model/checkpoints/sentinel.pt"), map_location="cpu")
    model.eval()
    print("Model loaded.\n")

    # Confusion matrix for severity (4x4)
    sev_matrix = [[0]*4 for _ in range(4)]
    sev_correct = 0
    cat_correct = 0
    total_test  = 0
    errors      = 0

    print(f"Evaluating on {len(test_examples)} held-out examples...")
    for i, ex in enumerate(test_examples):
        try:
            text = ex["diff_hunk"] + " </s> " + ex["comment"]
            ids, mask = tokenizer.batch_encode([text])
            true_sev, true_cat = get_label(ex)

            with torch.no_grad():
                out = model(ids, mask)

            pred_sev = out["severity"].argmax(-1).item()
            pred_cat = out["category"].argmax(-1).item()

            sev_matrix[true_sev][pred_sev] += 1
            if pred_sev == true_sev:
                sev_correct += 1
            if pred_cat == true_cat:
                cat_correct += 1
            total_test += 1

        except Exception as e:
            errors += 1
            continue

        if (i + 1) % 100 == 0:
            print(f"  {i+1}/{len(test_examples)} — sev acc so far: {sev_correct/(total_test):.3f}")

    # Results
    sev_acc = sev_correct / total_test
    cat_acc = cat_correct / total_test

    print(f"\n{'='*50}")
    print(f"SENTINEL Model Evaluation — Held-out Test Set")
    print(f"{'='*50}")
    print(f"Test examples:       {total_test}")
    print(f"Errors skipped:      {errors}")
    print(f"Severity accuracy:   {sev_acc:.1%}")
    print(f"Category accuracy:   {cat_acc:.1%}")
    print(f"\nSeverity Confusion Matrix (rows=true, cols=predicted):")
    print(f"{'':12}", end="")
    for s in SEVERITIES:
        print(f"{s:12}", end="")
    print()
    for i, row in enumerate(sev_matrix):
        print(f"{SEVERITIES[i]:12}", end="")
        for val in row:
            print(f"{val:<12}", end="")
        print()

    print(f"\nPer-class severity accuracy:")
    for i, sev in enumerate(SEVERITIES):
        total_class = sum(sev_matrix[i])
        if total_class > 0:
            acc = sev_matrix[i][i] / total_class
            print(f"  {sev:12} {acc:.1%}  ({sev_matrix[i][i]}/{total_class})")
        else:
            print(f"  {sev:12} N/A (no examples)")

    print(f"\nLabel distribution in test set:")
    for i, sev in enumerate(SEVERITIES):
        count = sum(sev_matrix[i])
        pct = count / total_test * 100
        print(f"  {sev:12} {count:4d} ({pct:.1f}%)")

    return sev_acc, cat_acc, sev_matrix


if __name__ == "__main__":
    run_evaluation()
