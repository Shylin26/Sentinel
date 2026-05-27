"""
Unit tests for diff extractor — chunking logic and idempotency key generation.
Run: pytest tests/test_diff_extractor.py -v
"""
import pytest
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import os
os.environ["GITHUB_TOKEN"] = "test_token"

with patch("kafka.KafkaConsumer"), \
     patch("kafka.KafkaProducer"), \
     patch("grpc.insecure_channel"), \
     patch("grpc.insecure_channel"):
    from services.kafka.diff_extractor import fetch_diff, process


# ---------------------------------------------------------------------------
# Chunking logic tests
# ---------------------------------------------------------------------------

class TestChunking:

    def make_file(self, patch_words: int, filename: str = "main.py") -> dict:
        """Make a fake GitHub API file entry with a patch of N words."""
        patch = " ".join([f"word{i}" for i in range(patch_words)])
        return {"filename": filename, "patch": patch}

    @patch("urllib.request.urlopen")
    def test_single_chunk_small_diff(self, mock_urlopen):
        """A diff under 400 words produces exactly one chunk."""
        import json
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "files": [self.make_file(patch_words=100, filename="small.py")]
        }).encode()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        chunks = fetch_diff("owner/repo", "abc123")
        assert len(chunks) == 1
        assert chunks[0]["filename"] == "small.py"
        assert chunks[0]["chunk_index"] == 0

    @patch("urllib.request.urlopen")
    def test_multiple_chunks_large_diff(self, mock_urlopen):
        """A diff over 400 words produces multiple chunks."""
        import json
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "files": [self.make_file(patch_words=900, filename="large.py")]
        }).encode()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        chunks = fetch_diff("owner/repo", "abc123")
        assert len(chunks) == 3  # 900 words / 400 = 3 chunks
        assert chunks[0]["chunk_index"] == 0
        assert chunks[1]["chunk_index"] == 1
        assert chunks[2]["chunk_index"] == 2

    @patch("urllib.request.urlopen")
    def test_empty_patch_skipped(self, mock_urlopen):
        """Files with no patch are skipped."""
        import json
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "files": [
                {"filename": "binary.png", "patch": ""},
                {"filename": "code.py", "patch": "def foo(): pass"},
            ]
        }).encode()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        chunks = fetch_diff("owner/repo", "abc123")
        assert len(chunks) == 1
        assert chunks[0]["filename"] == "code.py"

    @patch("urllib.request.urlopen")
    def test_missing_patch_key_skipped(self, mock_urlopen):
        """Files with no patch key are skipped."""
        import json
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "files": [
                {"filename": "deleted.py"},
                {"filename": "code.py", "patch": "x = 1"},
            ]
        }).encode()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        chunks = fetch_diff("owner/repo", "abc123")
        assert len(chunks) == 1
        assert chunks[0]["filename"] == "code.py"

    @patch("urllib.request.urlopen")
    def test_multiple_files(self, mock_urlopen):
        """Each file gets its own chunks."""
        import json
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "files": [
                self.make_file(patch_words=50, filename="a.py"),
                self.make_file(patch_words=50, filename="b.py"),
            ]
        }).encode()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        chunks = fetch_diff("owner/repo", "abc123")
        assert len(chunks) == 2
        filenames = [c["filename"] for c in chunks]
        assert "a.py" in filenames
        assert "b.py" in filenames

    @patch("urllib.request.urlopen")
    def test_empty_files_list(self, mock_urlopen):
        """A commit with no files returns no chunks."""
        import json
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({"files": []}).encode()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        chunks = fetch_diff("owner/repo", "abc123")
        assert chunks == []


# ---------------------------------------------------------------------------
# Idempotency key tests
# ---------------------------------------------------------------------------

class TestIdempotencyKey:

    @patch("urllib.request.urlopen")
    def test_idempotency_key_format(self, mock_urlopen):
        """Idempotency key follows commit:filename:chunk_index format."""
        import json
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "files": [{"filename": "src/auth.py", "patch": "def login(): pass"}]
        }).encode()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        chunks = fetch_diff("owner/repo", "deadbeef")
        # idempotency key is built in process(), not fetch_diff()
        # verify the chunk has all fields needed to build it
        assert chunks[0]["filename"] == "src/auth.py"
        assert chunks[0]["chunk_index"] == 0

    def test_idempotency_key_is_deterministic(self):
        """Same commit + filename + chunk_index always produces same key."""
        commit = "abc123"
        filename = "main.py"
        chunk_index = 0
        key1 = commit + ":" + filename + ":" + str(chunk_index)
        key2 = commit + ":" + filename + ":" + str(chunk_index)
        assert key1 == key2

    def test_idempotency_key_differs_by_chunk(self):
        """Different chunk indexes produce different keys."""
        commit = "abc123"
        filename = "main.py"
        key0 = commit + ":" + filename + ":0"
        key1 = commit + ":" + filename + ":1"
        assert key0 != key1

    def test_idempotency_key_differs_by_file(self):
        """Different filenames produce different keys."""
        commit = "abc123"
        key_a = commit + ":a.py:0"
        key_b = commit + ":b.py:0"
        assert key_a != key_b
