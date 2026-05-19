import json
import torch
import torch.nn as nn
from pathlib import Path
from torch.utils.data import Dataset, DataLoader
from transformer import SentinelTransformer, BPETokenizerWrapper
import os

# Optimize memory allocation on Apple Silicon (MPS)
os.environ["PYTORCH_MPS_HIGH_WATERMARK_RATIO"] = "0.0"

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
DEVICE = torch.device("mps" if torch.backends.mps.is_available() else "cpu")
CHECKPOINT_DIR = ROOT_DIR / "model/checkpoints"
CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

CATEGORIES = [
    "style", "naming", "logic", "performance", "security",
    "error_handling", "testing", "documentation", "complexity",
    "duplication", "typing", "other"
]
CAT2IDX = {c: i for i, c in enumerate(CATEGORIES)}

def infer_category(comment):
    c = comment.lower()
    rules = [
        ("security",     ["security", "vulnerability", "injection", "auth", "exploit"]),
        ("performance",  ["performance", "slow", "efficient", "cache", "complexity", "o(n"]),
        ("error_handling",["exception", "error", "try", "catch", "handle", "raise"]),
        ("testing",      ["test", "assert", "mock", "coverage", "unittest"]),
        ("typing",       ["type", "annotation", "hint", "mypy", "cast"]),
        ("documentation",["comment", "docstring", "doc", "readme", "explain"]),
        ("naming",       ["name", "rename", "variable", "function", "class", "called"]),
        ("complexity",   ["complex", "simplify", "refactor", "readable", "clean"]),
        ("duplication",  ["duplicate", "repeat", "reuse", "dry", "copy"]),
        ("logic",        ["logic", "incorrect", "wrong", "bug", "broken", "fails"]),
        ("style",        ["style", "format", "indent", "whitespace", "nit"]),
    ]
    for cat, keywords in rules:
        if any(kw in c for kw in keywords):
            return CAT2IDX[cat]
    return CAT2IDX["other"]


class ReviewDataset(Dataset):
    def __init__(self, path, tokenizer, max_len=512):
        self.tokenizer = tokenizer
        self.max_len = max_len
        self.examples = []
        with open(path) as f:
            for line in f:
                ex = json.loads(line)
                self.examples.append(ex)

    def __len__(self):
        return len(self.examples)

    def __getitem__(self, idx):
        ex = self.examples[idx]
        text = ex["diff_hunk"] + " </s> " + ex["comment"]
        ids = self.tokenizer.encode(text, self.max_len)
        severity = min(int(ex["severity"]), 3)
        category = infer_category(ex["comment"])
        return ids, severity, category


def collate(batch):
    ids_list, severities, categories = zip(*batch)
    max_l = max(len(i) for i in ids_list)
    padded = [i + [0] * (max_l - len(i)) for i in ids_list]
    mask = [[1] * len(i) + [0] * (max_l - len(i)) for i in ids_list]
    return (
        torch.tensor(padded, dtype=torch.long),
        torch.tensor(mask, dtype=torch.long),
        torch.tensor(severities, dtype=torch.long),
        torch.tensor(categories, dtype=torch.long),
    )


def train():
    tokenizer = BPETokenizerWrapper(str(ROOT_DIR / "model/src/sentinel_bpe.model"))
    dataset = ReviewDataset(str(ROOT_DIR / "data/raw/pr_review_comments.jsonl"), tokenizer)
    loader = DataLoader(dataset, batch_size=32, shuffle=True,
                        collate_fn=collate, num_workers=0)

    model = SentinelTransformer().to(DEVICE)
    optimizer = torch.optim.AdamW(model.parameters(), lr=3e-4, weight_decay=0.01)
    scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(optimizer, T_max=20)

    severity_loss_fn = nn.CrossEntropyLoss()
    category_loss_fn = nn.CrossEntropyLoss()

    print(f"Device: {DEVICE}")
    print(f"Dataset: {len(dataset)} examples")
    print(f"Batches per epoch: {len(loader)}")
    print("Starting training...\n")

    for epoch in range(10):
        model.train()
        total_loss = 0
        severity_correct = 0
        total = 0

        for step, (ids, mask, severity, category) in enumerate(loader):
            ids = ids.to(DEVICE)
            mask = mask.to(DEVICE)
            severity = severity.to(DEVICE)
            category = category.to(DEVICE)

            optimizer.zero_grad()
            out = model(ids, mask)

            s_loss = severity_loss_fn(out["severity"], severity)
            c_loss = category_loss_fn(out["category"], category)
            loss = 0.5 * s_loss + 0.5 * c_loss

            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()

            total_loss += loss.item()
            severity_correct += (out["severity"].argmax(-1) == severity).sum().item()
            total += len(severity)

            if step % 20 == 0:
                print(f"  Epoch {epoch+1} | Step {step}/{len(loader)} | "
                      f"Loss: {loss.item():.4f} | "
                      f"Sev Acc: {severity_correct/total:.3f}")

            if step % 50 == 0 and DEVICE.type == "mps" and hasattr(torch, "mps"):
                torch.mps.empty_cache()

        scheduler.step()
        epoch_acc = severity_correct / total
        print(f"\nEpoch {epoch+1} done — Loss: {total_loss/len(loader):.4f} | "
              f"Severity Acc: {epoch_acc:.3f}")

        if DEVICE.type == "mps" and hasattr(torch, "mps"):
            torch.mps.empty_cache()

        torch.save({
            "epoch": epoch,
            "model_state": model.state_dict(),
            "optimizer_state": optimizer.state_dict(),
            "accuracy": epoch_acc,
        }, CHECKPOINT_DIR / f"checkpoint_epoch{epoch+1}.pt")

        if epoch_acc >= 0.70:
            print(f"\nTarget accuracy reached! Saving best model.")
            torch.save(model.state_dict(), CHECKPOINT_DIR / "best_model.pt")

    print("\nTraining complete.")

if __name__ == "__main__":
    train()