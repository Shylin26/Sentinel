"""
Unit tests for result publisher — comment formatting and idempotency logic.
Run: pytest tests/test_publisher.py -v
"""
import pytest
from unittest.mock import MagicMock, patch
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import os
os.environ["GITHUB_TOKEN"] = "test_token"


with patch("kafka.KafkaConsumer"), patch("kafka.KafkaProducer"), patch("redis.Redis"), patch("prometheus_client.start_http_server"):
    from services.publisher.result_publisher import (
        format_comment,
        is_duplicate,
        mark_posted,
        SEVERITY_EMOJI,
    )


# ---------------------------------------------------------------------------
# format_comment tests
# ---------------------------------------------------------------------------

class TestFormatComment:

    def make_review(self, severity="bug", category="security", confidence=0.95, filename="main.py"):
        return {
            "severity": severity,
            "category": category,
            "message": "Potential security vulnerability — review immediately.",
            "confidence": confidence,
            "filename": filename,
        }

    def test_contains_severity_label(self):
        review = self.make_review(severity="bug")
        comment = format_comment(review)
        assert "BUG" in comment

    def test_contains_category(self):
        review = self.make_review(category="security")
        comment = format_comment(review)
        assert "security" in comment

    def test_contains_message(self):
        review = self.make_review()
        comment = format_comment(review)
        assert "Potential security vulnerability" in comment

    def test_contains_confidence(self):
        review = self.make_review(confidence=0.95)
        comment = format_comment(review)
        assert "0.95" in comment

    def test_contains_filename(self):
        review = self.make_review(filename="auth/login.py")
        comment = format_comment(review)
        assert "auth/login.py" in comment

    def test_nit_emoji(self):
        review = self.make_review(severity="nit")
        comment = format_comment(review)
        assert SEVERITY_EMOJI["nit"] in comment

    def test_suggestion_emoji(self):
        review = self.make_review(severity="suggestion")
        comment = format_comment(review)
        assert SEVERITY_EMOJI["suggestion"] in comment

    def test_bug_emoji(self):
        review = self.make_review(severity="bug")
        comment = format_comment(review)
        assert SEVERITY_EMOJI["bug"] in comment

    def test_critical_emoji(self):
        review = self.make_review(severity="critical")
        comment = format_comment(review)
        assert SEVERITY_EMOJI["critical"] in comment

    def test_unknown_severity_gets_default_emoji(self):
        review = self.make_review(severity="unknown")
        comment = format_comment(review)
        assert "💬" in comment

    def test_severity_uppercased(self):
        review = self.make_review(severity="suggestion")
        comment = format_comment(review)
        assert "SUGGESTION" in comment


# ---------------------------------------------------------------------------
# idempotency cache tests
# ---------------------------------------------------------------------------

class TestIdempotencyCache:

    def test_is_duplicate_returns_false_when_cache_unavailable(self):
        import services.publisher.result_publisher as pub
        original = pub.CACHE_AVAILABLE
        pub.CACHE_AVAILABLE = False
        result = is_duplicate("any_key")
        pub.CACHE_AVAILABLE = original
        assert result is False

    def test_is_duplicate_checks_redis(self):
        import services.publisher.result_publisher as pub
        original_cache = pub.cache
        original_available = pub.CACHE_AVAILABLE

        mock_cache = MagicMock()
        mock_cache.exists.return_value = 1
        pub.cache = mock_cache
        pub.CACHE_AVAILABLE = True

        result = is_duplicate("abc:file.py:0")
        assert result is True
        mock_cache.exists.assert_called_once_with("review:abc:file.py:0")

        pub.cache = original_cache
        pub.CACHE_AVAILABLE = original_available

    def test_is_not_duplicate_when_key_missing(self):
        import services.publisher.result_publisher as pub
        original_cache = pub.cache
        original_available = pub.CACHE_AVAILABLE

        mock_cache = MagicMock()
        mock_cache.exists.return_value = 0
        pub.cache = mock_cache
        pub.CACHE_AVAILABLE = True

        result = is_duplicate("abc:file.py:0")
        assert result is False

        pub.cache = original_cache
        pub.CACHE_AVAILABLE = original_available

    def test_mark_posted_sets_redis_key(self):
        import services.publisher.result_publisher as pub
        original_cache = pub.cache
        original_available = pub.CACHE_AVAILABLE

        mock_cache = MagicMock()
        pub.cache = mock_cache
        pub.CACHE_AVAILABLE = True

        mark_posted("abc:file.py:0")
        mock_cache.set.assert_called_once_with("review:abc:file.py:0", 1, ex=86400)

        pub.cache = original_cache
        pub.CACHE_AVAILABLE = original_available

    def test_mark_posted_does_nothing_when_cache_unavailable(self):
        import services.publisher.result_publisher as pub
        original_cache = pub.cache
        original_available = pub.CACHE_AVAILABLE

        mock_cache = MagicMock()
        pub.cache = mock_cache
        pub.CACHE_AVAILABLE = False

        mark_posted("abc:file.py:0")
        mock_cache.set.assert_not_called()

        pub.cache = original_cache
        pub.CACHE_AVAILABLE = original_available
