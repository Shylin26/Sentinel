# SENTINEL Benchmark Report

## Environment
- Machine: Apple M4 Air (local dev)
- Python: 3.9 (venv)
- Load tool: Locust 2.34.0
- Date: May 2026

## Webhook Layer (FastAPI + Kafka)

| Users | Throughput | p50 | p99 | p99.9 | Failures |
|-------|-----------|-----|-----|-------|----------|
| 5     | ~15 req/s  | 8ms | 15ms | 26ms | 0%      |
| 50    | ~80 req/s  | 5ms | 19ms | 47ms | 0%      |

**Observation:** Webhook layer is not the bottleneck. p99 stays under 20ms at 50 concurrent users. Zero failures across 9,019 requests.

## Inference Layer (gRPC + TorchScript)

| Model | Format | Latency (single chunk) |
|-------|--------|----------------------|
| SentinelTransformer (25M params) | TorchScript fp32 | ~180ms |
| Dynamic batch (32 chunks) | TorchScript fp32 | ~200ms total (~6ms/chunk) |

**Observation:** Dynamic batching gives ~30x throughput improvement over single-chunk processing at high load.

## End-to-End Latency (webhook → GitHub comment)

| Percentile | Latency |
|-----------|---------|
| p50 | ~400ms |
| p95 | ~900ms |
| p99 | ~1.4s  |
| SLO target | < 2.0s |

**Observation:** p99 comfortably under the 2s SLO target on local dev hardware.

## Bottleneck Analysis

| Stage | Latency contribution |
|-------|---------------------|
| Webhook → Kafka | ~5ms |
| GitHub API (fetch diff) | ~300-500ms |
| gRPC inference | ~180ms |
| GitHub API (post comment) | ~200-400ms |

**Primary bottleneck: GitHub API calls** — both diff fetch and comment post are external HTTP calls with variable latency. Not fixable, but parallelizable across chunks.

## Resilience

| Test | Result |
|------|--------|
| Bad signature rejection | ✅ 401 returned |
| Duplicate delivery idempotency | ✅ Redis cache deduplicates |
| Kafka connectivity | ✅ Reachable |
| gRPC inference end-to-end | ✅ Correct severity returned |
| Redis cache | ✅ Ping successful |
| Chaos suite (6/6) | ✅ All passing |

## Kafka Topics

| Topic | Partitions | Retention | Purpose |
|-------|-----------|-----------|---------|
| push-events | 12 | 24h | Webhook → diff extractor |
| diff-chunks | 12 | 24h | Legacy (replaced by gRPC) |
| reviews | 12 | 24h | Inference → publisher |
| reviews-dlq | 12 | 24h | Failed reviews after 3 retries |

## Cost Estimate (Hetzner VPS, production)

| Resource | Spec | Cost |
|----------|------|------|
| VPS | CX21 (2 vCPU, 4GB RAM) | ~€4/month |
| Inference | CPU-only (TorchScript) | included |
| Kafka | Single-node, self-hosted | included |

**Estimated cost per review: < €0.001**
