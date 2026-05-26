"""
SENTINEL Load Test
Run from sentinel/ directory:
  locust -f tests/locustfile.py --headless -u 10 -r 2 --run-time 60s --host http://127.0.0.1:8000

  -u  total users (concurrent)
  -r  spawn rate (users/sec)
  --run-time  how long to run
"""
import hmac
import hashlib
import json
import time
from locust import HttpUser, task, between, events

WEBHOOK_SECRET = "sentinel_secret"

# Real commits to rotate through so GitHub API returns real diffs
COMMITS = [
    ("torvalds/linux", "b85ea95d086471afb4ad062012a4d73cd328fa86"),
    ("torvalds/linux", "4cece764965020c22cff7665b18a012006359095"),
    ("torvalds/linux", "9e98c678c2d6ae3a17cb2de55d17f69dddaa231b"),
]

_commit_index = 0


def next_commit():
    global _commit_index
    repo, commit = COMMITS[_commit_index % len(COMMITS)]
    _commit_index += 1
    return repo, commit


def make_payload(repo: str, commit: str) -> bytes:
    return json.dumps({
        "repository": {"full_name": repo},
        "after": commit,
        "commits": [],
    }).encode()


def sign(payload: bytes) -> str:
    return "sha256=" + hmac.new(
        WEBHOOK_SECRET.encode(), payload, hashlib.sha256
    ).hexdigest()


class WebhookUser(HttpUser):
    # Wait 0.1-0.5s between tasks — simulates ~5-10 req/sec per user
    wait_time = between(0.1, 0.5)

    @task
    def push_event(self):
        repo, commit = next_commit()
        payload = make_payload(repo, commit)
        self.client.post(
            "/webhook",
            data=payload,
            headers={
                "Content-Type": "application/json",
                "X-GitHub-Event": "push",
                "X-Hub-Signature-256": sign(payload),
            },
            name="/webhook [push]",
        )

    @task(1)
    def health_check(self):
        self.client.get("/health", name="/health")


@events.request.add_listener
def on_request(request_type, name, response_time, response_length, response, **kwargs):
    if response and response.status_code not in (200, 401):
        print(f"  Unexpected status {response.status_code} on {name}")
