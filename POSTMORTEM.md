# SENTINEL — Project Postmortem

Every failure hit during 6 months of building. What broke, why, and what I learned.

---

## 1. Kafka Container Port Conflict

**What broke:** `docker compose up` failed with `Bind for 0.0.0.0:9092 failed: port is already allocated` whenever a previous Kafka instance (from another project) was running.

**Why:** Two separate docker-compose stacks both trying to bind port 9092. Docker doesn't namespace ports between stacks.

**Fix:** `docker stop eidolon-kafka` before starting SENTINEL infra. Long-term fix: use a shared Kafka instance or configure non-default ports per project.

**Learned:** Always check `docker ps` before starting infra. Port conflicts are silent until compose fails.

---

## 2. Zookeeper Stale Broker Registration

**What broke:** After a forced Kafka container kill, restarting Kafka crashed with `KeeperErrorCode = NodeExists`. Kafka couldn't register itself in Zookeeper because the previous registration was still there.

**Why:** Zookeeper persists broker registrations as ephemeral nodes. If the broker dies without deregistering (SIGKILL vs graceful shutdown), the node lingers until the session timeout.

**Fix:** `docker compose restart zookeeper && sleep 5 && docker compose restart kafka`. The restart clears the ephemeral node.

**Learned:** Always use `docker compose stop` (SIGTERM) not `docker kill` (SIGKILL). Graceful shutdown gives Kafka time to deregister from Zookeeper.

---

## 3. Python Version Mismatch (3.9 venv vs 3.11 system)

**What broke:** `python3 services/inference/worker.py` threw `ModuleNotFoundError: No module named 'torch'` even though torch was installed.

**Why:** `python3` was aliased to system Python 3.11, but the venv was Python 3.9. Packages installed via `pip` in the venv were invisible to the system Python.

**Fix:** Always use the full venv path: `/Users/parishachauhan/SENTINEL/.venv/bin/python3`. Added this to the Makefile so `make run` always uses the right interpreter.

**Learned:** Never alias `python3` globally on a dev machine with multiple projects. Use `which python3` to verify before running anything.

---

## 4. Diff Extractor Overwrote With Inference Worker Content

**What broke:** `diff_extractor.py` started importing `torch` and loading the model — it had been accidentally replaced with the inference worker's content during a copy-paste session.

**Why:** Both files were being edited simultaneously in chat. A paste went to the wrong file.

**Fix:** Restored from the original content. Added `python3 -c "import ast; ast.parse(...)"` as a standard check after every file edit.

**Learned:** Never edit two similar files in the same session without verifying the correct file path first. The syntax check catches syntax errors but not wrong-file errors — always `cat` the file after editing to confirm.

---

## 5. gRPC Proto Compilation Typo

**What broke:** `protoc` failed with `"ReviewResonse" is not defined` — a typo in the service definition (`ReviewResonse` vs `ReviewResponse`).

**Why:** The proto file was typed manually instead of copy-pasted from a verified source.

**Fix:** Fixed the typo, recompiled with `grpc_tools.protoc`.

**Learned:** Proto files are strict. Any typo in a type reference is a hard compile error. Always compile immediately after writing — don't accumulate changes.

---

## 6. Prometheus Scrape Targets Not Receiving Metrics

**What broke:** Grafana showed "No data" even though all services were running.

**Why:** Two separate issues:
1. `alerts.yml` wasn't mounted into the Prometheus container — only `prometheus.yml` was in the volume mount.
2. After Kafka restarts, all Python services lost their Kafka connections and needed manual restart.

**Fix:** Added `./alerts.yml:/etc/prometheus/alerts.yml` to the docker-compose volume mount. Added `make stop && make run` as the standard restart procedure.

**Learned:** Docker volume mounts are explicit — adding a file to a directory doesn't automatically mount it. Every config file needs its own mount entry.

---

## 7. Redis Idempotency Cache Breaking Tests

**What broke:** `pytest tests/test_publisher.py` failed with `OSError: [Errno 48] Address already in use` — the publisher's `start_http_server(9102)` fired at import time, clashing with the already-running publisher process.

**Why:** Module-level code (Prometheus server, Kafka consumer) runs on import. Tests that import the module trigger all side effects.

**Fix:** Added `patch("prometheus_client.start_http_server")` to the test's mock context. Also mocked `KafkaConsumer`, `KafkaProducer`, and `redis.Redis`.

**Learned:** Production service modules shouldn't have side effects at import time. The right pattern is to wrap startup code in `if __name__ == "__main__"` or a `main()` function. Refactor candidate for next iteration.

---

## 8. ngrok URL Instability Breaking GitHub App OAuth

**What broke:** The GitHub App manifest was created with `ngrok-free.app` but the active tunnel was `ngrok-free.dev`. The OAuth callback failed because GitHub redirected to the wrong domain.

**Why:** ngrok free tier assigns a random subdomain per session. The URL changed between the time the manifest was written and when the tunnel was restarted.

**Fix:** Used `curl http://127.0.0.1:4040/api/tunnels` to get the actual active URL. Long-term fix: deploy to Hetzner with a fixed domain.

**Learned:** Never hardcode ngrok URLs in config files. Always read the active URL from the ngrok API. For anything that needs a stable callback URL, use a real domain.

---

## 9. Dynamic Batching consumer_timeout_ms Behavior

**What broke:** The inference worker's `collect_batch()` function blocked indefinitely when no messages arrived — the `consumer_timeout_ms=BATCH_WINDOW_MS` setting was ignored because the consumer was initialized without it initially.

**Why:** `KafkaConsumer` without `consumer_timeout_ms` blocks forever on `for message in consumer`. The timeout only applies when it's set at construction time.

**Fix:** Added `consumer_timeout_ms=BATCH_WINDOW_MS` to the `KafkaConsumer` constructor. The consumer now raises `StopIteration` after the window, which breaks the collection loop.

**Learned:** Kafka consumer iteration is blocking by default. Any timeout behavior must be configured at construction, not patched in afterward.

---

## 10. Training Data vs Claims Mismatch

**What broke:** The README claimed "trained on 2M real PR review comments" but `wc -l data/raw/pr_review_comments.jsonl` returned 5,001.

**Why:** The implementation plan targeted 2M, but the actual mining run produced 5K. The README was written against the plan, not the actual data.

**Fix:** Updated README to say "5,001 labeled PR review comments" with a note that the pipeline scales to 2M+ via `mine_github_archive.py`.

**Learned:** Never write README claims against a plan — write them against what's actually on disk. Anyone reading carefully will check. Honest framing of a curated 5K dataset is stronger than a false 2M claim.

---

## What I'd Do Differently

1. **Wrap all startup code in `main()`** — no module-level side effects. Makes testing trivial.
2. **Use a fixed domain from day one** — ngrok is fine for quick tests but breaks anything with OAuth callbacks.
3. **Add `make test` to every commit workflow** — several bugs would have been caught immediately with tests running on every change.
4. **Version-pin docker images** — `prom/prometheus:latest` and `grafana/grafana:latest` can break between runs. Pin to specific versions.
5. **Use `docker compose down -v` not just `docker compose down`** — volumes persist between restarts. Stale Zookeeper state caused 30 minutes of debugging.
