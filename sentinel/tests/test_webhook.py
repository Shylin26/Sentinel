"""
Unit tests for webhook HMAC validation and payload parsing.
Run: pytest tests/test_webhook.py -v
"""
import hmac
import hashlib
import json
import pytest
from fastapi.testclient import TestClient

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import os
os.environ["GITHUB_TOKEN"] = "test_token"
os.environ["GITHUB_TOKEN"] = "test_token"
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "proto"))
os.environ["GITHUB_WEBHOOK_SECRET"] = "test_secret"
os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "127.0.0.1:9092"

from unittest.mock import MagicMock, patch

with patch("kafka.KafkaProducer") as mock_producer:
    mock_instance = MagicMock()
    mock_future = MagicMock()
    mock_future.get.return_value = MagicMock(partition=0, offset=1)
    mock_instance.send.return_value = mock_future
    mock_producer.return_value = mock_instance
    from services.webhook.main import app, verify_signature

client = TestClient(app)
SECRET = "test_secret"


def sign(payload: bytes, secret: str = SECRET) -> str:
    return "sha256=" + hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()


def make_push_payload(repo: str = "owner/repo", commit: str = "abc123") -> bytes:
    return json.dumps({
        "repository": {"full_name": repo},
        "after": commit,
        "commits": [],
    }).encode()


# ---------------------------------------------------------------------------
# verify_signature unit tests
# ---------------------------------------------------------------------------

class TestVerifySignature:

    def test_valid_signature(self):
        payload = b"hello world"
        sig = sign(payload)
        assert verify_signature(payload, sig) is True

    def test_invalid_signature(self):
        payload = b"hello world"
        assert verify_signature(payload, "sha256=invalidsig") is False

    def test_wrong_secret(self):
        payload = b"hello world"
        sig = sign(payload, secret="wrong_secret")
        assert verify_signature(payload, sig) is False

    def test_empty_payload(self):
        payload = b""
        sig = sign(payload)
        assert verify_signature(payload, sig) is True

    def test_missing_sha256_prefix(self):
        payload = b"hello"
        raw = hmac.new(SECRET.encode(), payload, hashlib.sha256).hexdigest()
        assert verify_signature(payload, raw) is False

    def test_tampered_payload(self):
        payload = b"original"
        sig = sign(payload)
        assert verify_signature(b"tampered", sig) is False


# ---------------------------------------------------------------------------
# /webhook endpoint tests
# ---------------------------------------------------------------------------

class TestWebhookEndpoint:

    def test_ping_event(self):
        payload = json.dumps({"repository": {"full_name": "owner/repo"}}).encode()
        resp = client.post(
            "/webhook",
            content=payload,
            headers={
                "X-GitHub-Event": "ping",
                "X-Hub-Signature-256": sign(payload),
                "Content-Type": "application/json",
            },
        )
        assert resp.status_code == 200
        assert resp.json()["msg"] == "pong"

    def test_invalid_signature_returns_401(self):
        payload = make_push_payload()
        resp = client.post(
            "/webhook",
            content=payload,
            headers={
                "X-GitHub-Event": "push",
                "X-Hub-Signature-256": "sha256=badsignature",
                "Content-Type": "application/json",
            },
        )
        assert resp.status_code == 401

    def test_missing_signature_returns_401(self):
        payload = make_push_payload()
        resp = client.post(
            "/webhook",
            content=payload,
            headers={
                "X-GitHub-Event": "push",
                "Content-Type": "application/json",
            },
        )
        assert resp.status_code == 401

    def test_valid_push_queued(self):
        payload = make_push_payload(repo="owner/repo", commit="deadbeef")
        resp = client.post(
            "/webhook",
            content=payload,
            headers={
                "X-GitHub-Event": "push",
                "X-Hub-Signature-256": sign(payload),
                "Content-Type": "application/json",
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "queued"
        assert data["repo"] == "owner/repo"
        assert data["commit"] == "deadbeef"
        assert data["event"] == "push"

    def test_ignored_event_returns_200(self):
        payload = json.dumps({"repository": {"full_name": "owner/repo"}}).encode()
        resp = client.post(
            "/webhook",
            content=payload,
            headers={
                "X-GitHub-Event": "issues",
                "X-Hub-Signature-256": sign(payload),
                "Content-Type": "application/json",
            },
        )
        assert resp.status_code == 200
        assert resp.json()["msg"] == "ignored"

    def test_health_endpoint(self):
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_metrics_endpoint(self):
        resp = client.get("/metrics")
        assert resp.status_code == 200
        assert b"sentinel_webhook_received_total" in resp.content
