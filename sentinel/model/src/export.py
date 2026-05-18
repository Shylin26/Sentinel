import torch
from pathlib import Path
from transformer import SentinelTransformer,BPETokenizerWrapper
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
CHECKPOINT = ROOT_DIR / "model/checkpoints/best_model.pt"
EXPORT_PATH = ROOT_DIR / "model/checkpoints/sentinel.pt"
device=torch.device("cpu")
print("Loading model...")
model = SentinelTransformer()
model.load_state_dict(torch.load(CHECKPOINT, map_location=device))
model.eval()

tokenizer = BPETokenizerWrapper(str(ROOT_DIR / "model/src/sentinel_bpe.model"))

print("Tracing model...")
sample_texts = ["def foo(x): return x + 1"]
ids, mask = tokenizer.batch_encode(sample_texts)
with torch.no_grad():
    traced = torch.jit.trace(
        model,
        (ids, mask),
        strict=False
    )

traced.save(str(EXPORT_PATH))
print(f"Exported to {EXPORT_PATH}")

print("Verifying load...")
loaded = torch.jit.load(str(EXPORT_PATH))
loaded.eval()
with torch.no_grad():
    out = loaded(ids, mask)
print(f"Severity logits: {out['severity']}")
print(f"Predicted severity: {out['severity'].argmax(-1).item()}")
print("\nExport OK.")

