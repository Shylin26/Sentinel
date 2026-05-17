import json
import sentencepiece as spm
from pathlib import Path

DATA = Path("data/raw/pr_review_comments.jsonl")
CORPUS = Path("data/processed/corpus.txt")
MODEL_PREFIX = "model/src/sentinel_bpe"

Path("data/processed").mkdir(parents=True, exist_ok=True)
Path("model/src").mkdir(parents=True, exist_ok=True)

print("Building corpus...")
with open(DATA) as f, open(CORPUS, "w") as out:
    for line in f:
        ex = json.loads(line)
        out.write(ex["diff_hunk"].replace("\n", " ") + "\n")
        out.write(ex["comment"].replace("\n", " ") + "\n")

print("Training BPE tokenizer...")
spm.SentencePieceTrainer.train(
    input=str(CORPUS),
    model_prefix=MODEL_PREFIX,
    vocab_size=32000,
    character_coverage=0.9995,
    model_type="bpe",
    pad_id=0,
    unk_id=1,
    bos_id=2,
    eos_id=3,
    pad_piece="<pad>",
    unk_piece="<unk>",
    bos_piece="<s>",
    eos_piece="</s>",
)
print("Verifying...")
sp = spm.SentencePieceProcessor()
sp.load(f"{MODEL_PREFIX}.model")
test = "def foo(x): return x + 1"
print(f"  Input:  {test}")
print(f"  Tokens: {sp.encode(test, out_type=str)}")
print(f"  IDs:    {sp.encode(test)}")
print(f"\nTokenizer saved to {MODEL_PREFIX}.model")
