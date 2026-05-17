import torch
import torch.nn as nn
import math

class BPETokenizerWrapper:
    def __init__(self,model_path):
        import sentencepiece as spm
        self.sp=spm.SentencePieceProcessor()
        self.sp.load(model_path)
        self.pad_id=0
        self.bos_id=2
        self.eos_id=3
    
    def encode(self,text,max_len=512):
        ids=[self.bos_id]+self.sp.encode(text)+[self.eos_id]
        if len(ids)>max_len:
            ids=ids[:max_len-1] + [self.eos_id]
        return ids
    def batch_encode(self, texts, max_len=512):
        encoded = [self.encode(t, max_len) for t in texts]
        max_l = max(len(e) for e in encoded)
        padded = [e + [self.pad_id] * (max_l - len(e)) for e in encoded]
        attention_mask = [[1] * len(e) + [0] * (max_l - len(e)) for e in encoded]
        return (
            torch.tensor(padded, dtype=torch.long),
            torch.tensor(attention_mask, dtype=torch.long)
        )

class SentinelTransformer(nn.Module):
    def __init__(self, vocab_size=32000, d_model=512, n_heads=8,
                 n_layers=6, max_len=512, num_categories=12, dropout=0.1):
        super().__init__()
        self.d_model = d_model

        self.token_embedding = nn.Embedding(vocab_size, d_model, padding_idx=0)
        self.position_embedding = nn.Embedding(max_len, d_model)
        self.dropout = nn.Dropout(dropout)

        encoder_layer = nn.TransformerEncoderLayer(
            d_model=d_model,
            nhead=n_heads,
            dim_feedforward=d_model * 4,
            dropout=dropout,
            batch_first=True,
            norm_first=True,
        )
        self.encoder = nn.TransformerEncoder(encoder_layer, num_layers=n_layers, enable_nested_tensor=False)

        self.severity_head = nn.Sequential(
            nn.Linear(d_model, 256),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(256, 4)
        )
        self.category_head = nn.Sequential(
            nn.Linear(d_model, 256),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(256, num_categories)
        )
        self.span_head = nn.Sequential(
            nn.Linear(d_model, 64),
            nn.GELU(),
            nn.Linear(64, 2)
        )

        self._init_weights()

    def _init_weights(self):
        for p in self.parameters():
            if p.dim() > 1:
                nn.init.xavier_uniform_(p)

    def forward(self, input_ids, attention_mask=None):
        B, T = input_ids.shape
        positions = torch.arange(T, device=input_ids.device).unsqueeze(0).expand(B, -1)

        x = self.token_embedding(input_ids) * math.sqrt(self.d_model)
        x = x + self.position_embedding(positions)
        x = self.dropout(x)

        if attention_mask is not None:
            src_key_padding_mask = (attention_mask == 0)
        else:
            src_key_padding_mask = None

        x = self.encoder(x, src_key_padding_mask=src_key_padding_mask)
        cls = x[:, 0, :]

        return {
            "severity": self.severity_head(cls),
            "category": self.category_head(cls),
            "span": self.span_head(x),
            "hidden": cls,
        }


if __name__ == "__main__":
    device = torch.device("mps" if torch.backends.mps.is_available() else "cpu")
    print(f"Device: {device}")

    model = SentinelTransformer().to(device)
    total = sum(p.numel() for p in model.parameters())
    print(f"Parameters: {total/1e6:.1f}M")

    tokenizer = BPETokenizerWrapper("model/src/sentinel_bpe.model")
    texts = ["def foo(x): return x + 1", "if x == None: pass"]
    ids, mask = tokenizer.batch_encode(texts)
    ids, mask = ids.to(device), mask.to(device)

    out = model(ids, mask)
    print(f"Severity logits shape: {out['severity'].shape}")
    print(f"Category logits shape: {out['category'].shape}")
    print(f"Span logits shape:     {out['span'].shape}")
    print("\nModel OK.")