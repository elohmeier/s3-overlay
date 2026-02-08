"""Unit tests for S3 overlay proxy core functionality."""

from __future__ import annotations

import logging
from typing import AsyncGenerator
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError
from s3_overlay import LocalSettings, RemoteSettings, S3OverlayProxy
from s3_overlay.proxy import load_local_settings_from_env, load_remote_settings_from_env


class TestLocalSettings:
    """Test LocalSettings configuration."""

    def test_default_settings(self):
        """Test that LocalSettings has sensible defaults."""
        settings = LocalSettings()
        assert settings.endpoint == "http://127.0.0.1:9000"
        assert settings.access_key == "minioadmin"
        assert settings.secret_key == "minioadmin"
        assert settings.region == "us-east-1"

    def test_load_from_env(self, local_env_vars):
        """Test that local settings load from environment."""
        settings = load_local_settings_from_env()
        assert settings.endpoint == local_env_vars["S3_OVERLAY_LOCAL_ENDPOINT"]
        assert settings.access_key == local_env_vars["S3_OVERLAY_LOCAL_ACCESS_KEY"]
        assert settings.secret_key == local_env_vars["S3_OVERLAY_LOCAL_SECRET_KEY"]
        assert settings.region == local_env_vars["S3_OVERLAY_LOCAL_REGION"]


class TestRemoteSettings:
    """Test RemoteSettings configuration."""

    def test_default_settings(self):
        """Test that RemoteSettings has sensible defaults."""
        settings = RemoteSettings()
        assert settings.endpoint is None
        assert settings.access_key is None
        assert settings.secret_key is None
        assert settings.region is None
        assert settings.enabled is False

    def test_load_from_env(self, remote_env_vars):
        """Test that remote settings load from environment."""
        settings = load_remote_settings_from_env()
        assert settings.endpoint == remote_env_vars["S3_OVERLAY_REMOTE_ENDPOINT"]
        assert settings.access_key == remote_env_vars["S3_OVERLAY_REMOTE_ACCESS_KEY_ID"]
        assert (
            settings.secret_key
            == remote_env_vars["S3_OVERLAY_REMOTE_SECRET_ACCESS_KEY"]
        )
        assert settings.region == remote_env_vars["S3_OVERLAY_REMOTE_REGION"]

    def test_enabled_when_configured(self, remote_env_vars):
        """Test that remote is enabled when credentials are set."""
        settings = load_remote_settings_from_env()
        assert settings.enabled is True

    def test_addressing_style_default(self):
        """Test that addressing_style defaults to virtual."""
        settings = RemoteSettings()
        assert settings.addressing_style == "virtual"

    def test_addressing_style_from_env(self):
        """Test that addressing_style can be configured from environment."""
        import os

        original = os.environ.get("S3_OVERLAY_REMOTE_ADDRESSING_STYLE")
        try:
            os.environ["S3_OVERLAY_REMOTE_ADDRESSING_STYLE"] = "path"
            settings = load_remote_settings_from_env()
            assert settings.addressing_style == "path"
        finally:
            if original is None:
                os.environ.pop("S3_OVERLAY_REMOTE_ADDRESSING_STYLE", None)
            else:
                os.environ["S3_OVERLAY_REMOTE_ADDRESSING_STYLE"] = original

    def test_bucket_mapping_parsing(self):
        """Test that bucket mapping is parsed correctly from environment."""
        import os

        original = os.environ.get("S3_OVERLAY_BUCKET_MAPPING")
        try:
            os.environ["S3_OVERLAY_BUCKET_MAPPING"] = "local1:remote1,local2:remote2"
            settings = load_remote_settings_from_env()
            assert settings.bucket_mapping is not None
            assert settings.bucket_mapping["local1"] == "remote1"
            assert settings.bucket_mapping["local2"] == "remote2"
        finally:
            if original is None:
                os.environ.pop("S3_OVERLAY_BUCKET_MAPPING", None)
            else:
                os.environ["S3_OVERLAY_BUCKET_MAPPING"] = original

    def test_bucket_mapping_with_spaces(self):
        """Test that bucket mapping handles spaces correctly."""
        import os

        original = os.environ.get("S3_OVERLAY_BUCKET_MAPPING")
        try:
            os.environ["S3_OVERLAY_BUCKET_MAPPING"] = (
                "  local1 : remote1 ,  local2:remote2  "
            )
            settings = load_remote_settings_from_env()
            assert settings.bucket_mapping is not None
            assert settings.bucket_mapping["local1"] == "remote1"
            assert settings.bucket_mapping["local2"] == "remote2"
        finally:
            if original is None:
                os.environ.pop("S3_OVERLAY_BUCKET_MAPPING", None)
            else:
                os.environ["S3_OVERLAY_BUCKET_MAPPING"] = original


class TestS3OverlayProxy:
    """Test S3OverlayProxy functionality."""

    @pytest.fixture
    async def proxy(self, overlay_env) -> AsyncGenerator[S3OverlayProxy, None]:
        """Create and initialize a proxy instance."""
        proxy = S3OverlayProxy.from_env()
        await proxy.startup()
        yield proxy
        await proxy.shutdown()

    def test_extract_bucket_and_key(self, overlay_env):
        """Test bucket and key extraction from paths."""
        proxy = S3OverlayProxy.from_env()

        # Test normal path
        bucket, key = proxy._extract_bucket_and_key("/mybucket/path/to/object.pdf")
        assert bucket == "mybucket"
        assert key == "path/to/object.pdf"

        # Test bucket-only path
        bucket, key = proxy._extract_bucket_and_key("/mybucket")
        assert bucket == "mybucket"
        assert key == ""

        # Test root path
        bucket, key = proxy._extract_bucket_and_key("/")
        assert bucket is None
        assert key is None

    def test_map_to_remote_bucket(self):
        """Test bucket name mapping."""
        import os

        original = os.environ.get("S3_OVERLAY_BUCKET_MAPPING")
        try:
            # Set up bucket mapping
            os.environ["S3_OVERLAY_BUCKET_MAPPING"] = "cellgrab:convexio-k8s-cellgrab"
            proxy = S3OverlayProxy.from_env()

            # Test mapped bucket
            assert proxy._map_to_remote_bucket("cellgrab") == "convexio-k8s-cellgrab"

            # Test unmapped bucket (should return original)
            assert proxy._map_to_remote_bucket("other-bucket") == "other-bucket"
        finally:
            if original is None:
                os.environ.pop("S3_OVERLAY_BUCKET_MAPPING", None)
            else:
                os.environ["S3_OVERLAY_BUCKET_MAPPING"] = original

    def test_map_to_remote_bucket_no_mapping(self, overlay_env):
        """Test bucket mapping when no mapping is configured."""
        import os

        # Temporarily clear bucket mapping
        original = os.environ.pop("S3_OVERLAY_BUCKET_MAPPING", None)
        try:
            proxy = S3OverlayProxy.from_env()

            # Without mapping, should return original name
            assert proxy._map_to_remote_bucket("cellgrab") == "cellgrab"
            assert proxy._map_to_remote_bucket("any-bucket") == "any-bucket"
        finally:
            if original is not None:
                os.environ["S3_OVERLAY_BUCKET_MAPPING"] = original

    def test_prepare_response_headers(self, overlay_env):
        """Test that response headers are properly filtered and formatted."""
        proxy = S3OverlayProxy.from_env()

        # Simulate httpx.Headers.raw format (list of byte tuples)
        raw_headers = [
            (b"content-type", b"application/pdf"),
            (b"content-length", b"12345"),
            (b"etag", b'"abc123"'),
            (b"connection", b"keep-alive"),  # Should be filtered
            (b"transfer-encoding", b"chunked"),  # Should be filtered
            (b"x-custom-header", b"custom-value"),
        ]

        result = proxy._prepare_response_headers(raw_headers)

        # Check that allowed headers are present (decoded to str)
        assert result["content-type"] == "application/pdf"
        assert result["content-length"] == "12345"
        assert result["etag"] == '"abc123"'
        assert result["x-custom-header"] == "custom-value"

        # Check that hop-by-hop headers are filtered
        assert "connection" not in result
        assert "transfer-encoding" not in result

    def test_prepare_response_headers_with_trailers(self, overlay_env):
        """Test that response headers handle 'trailers' hop-by-hop header."""
        proxy = S3OverlayProxy.from_env()

        # Simulate headers with trailers (which caused the original bug)
        raw_headers = [
            (b"content-type", b"application/pdf"),
            (b"trailers", b"x-custom-trailer"),  # Should be filtered
        ]

        result = proxy._prepare_response_headers(raw_headers)

        # Check that allowed headers are present (decoded to str)
        assert result["content-type"] == "application/pdf"

        # Check that trailers header is filtered
        assert "trailers" not in result

    @pytest.mark.anyio
    async def test_startup_shutdown(self, overlay_env):
        """Test proxy startup and shutdown lifecycle."""
        proxy = S3OverlayProxy.from_env()
        assert proxy._http_client is None

        await proxy.startup()
        assert proxy._http_client is not None

        await proxy.shutdown()
        assert proxy._http_client is None

    @pytest.mark.anyio
    async def test_ensure_bucket_creates_missing_bucket(
        self,
        proxy: S3OverlayProxy,
        local_s3_client,
        s3_helpers,
    ):
        """Test that _ensure_bucket creates buckets that don't exist."""
        bucket_name = "test-auto-create"

        # Ensure bucket doesn't exist
        if s3_helpers["bucket_exists"](local_s3_client, bucket_name):
            local_s3_client.delete_bucket(Bucket=bucket_name)

        # Call _ensure_bucket
        await proxy._ensure_bucket(bucket_name)

        # Verify bucket was created
        assert s3_helpers["bucket_exists"](local_s3_client, bucket_name)

        # Cleanup
        local_s3_client.delete_bucket(Bucket=bucket_name)

    @pytest.mark.anyio
    async def test_ensure_bucket_idempotent(
        self,
        proxy: S3OverlayProxy,
        local_s3_client,
        s3_helpers,
    ):
        """Test that _ensure_bucket is idempotent."""
        bucket_name = "test-idempotent"

        # Create bucket first
        s3_helpers["ensure_bucket"](local_s3_client, bucket_name)

        # Call _ensure_bucket again - should not fail
        await proxy._ensure_bucket(bucket_name)

        assert s3_helpers["bucket_exists"](local_s3_client, bucket_name)

        # Cleanup
        local_s3_client.delete_bucket(Bucket=bucket_name)


class TestRemoteErrorHandling:
    """Test that non-404 remote errors are handled gracefully."""

    @pytest.mark.anyio
    async def test_handle_read_miss_remote_403_returns_false(self, overlay_env, caplog):
        """A remote 403 on HeadObject should log a warning and return False."""
        proxy = S3OverlayProxy.from_env()
        await proxy.startup()
        try:
            # Build a 404 ClientError for the initial local miss
            local_error = ClientError(
                {
                    "Error": {"Code": "404", "Message": "Not Found"},
                    "ResponseMetadata": {"HTTPStatusCode": 404},
                },
                "HeadObject",
            )

            # Build a 403 ClientError for the remote head_object call
            remote_403 = ClientError(
                {
                    "Error": {"Code": "403", "Message": "Forbidden"},
                    "ResponseMetadata": {"HTTPStatusCode": 403},
                },
                "HeadObject",
            )

            # Mock the remote client's head_object to raise 403
            mock_remote = MagicMock()
            mock_remote.head_object.side_effect = remote_403
            proxy._remote_client = mock_remote

            request = MagicMock()
            request.method = "GET"
            request.headers = {}

            with caplog.at_level(logging.WARNING, logger="s3_overlay.proxy"):
                result = await proxy._handle_read_miss(
                    local_error, request, "/mybucket/mykey.pdf", "mybucket", "mykey.pdf"
                )

            assert result is False
            assert any("remote error" in r.message for r in caplog.records)
            assert any(
                "403" in r.message or "Forbidden" in r.message for r in caplog.records
            )
        finally:
            await proxy.shutdown()

    @pytest.mark.anyio
    async def test_backfill_remote_403_returns_false(self, overlay_env, caplog):
        """A remote 403 on get_object during backfill should log and return False."""
        proxy = S3OverlayProxy.from_env()
        await proxy.startup()
        try:
            remote_403 = ClientError(
                {
                    "Error": {"Code": "403", "Message": "Forbidden"},
                    "ResponseMetadata": {"HTTPStatusCode": 403},
                },
                "GetObject",
            )

            mock_remote = MagicMock()
            mock_remote.get_object.side_effect = remote_403
            proxy._remote_client = mock_remote

            request = MagicMock()
            request.method = "GET"
            request.headers = {}

            with caplog.at_level(logging.WARNING, logger="s3_overlay.proxy"):
                result = await proxy._backfill_from_remote(
                    request, "/mybucket/mykey.pdf"
                )

            assert result is False
            assert any("remote backfill failed" in r.message for r in caplog.records)
        finally:
            await proxy.shutdown()
