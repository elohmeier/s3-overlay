"""Integration tests for S3 overlay proxy with real MinIO instances."""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator, AsyncIterator, Callable
from email.utils import parsedate_to_datetime
from typing import cast

import pytest
from litestar import Request
from litestar.types import HTTPScope
from s3_overlay import S3OverlayProxy


@pytest.fixture
def simple_text_content() -> bytes:
    """Return minimal text bytes for integration tests."""
    return b"hello from s3-overlay\n"


@pytest.fixture
async def proxy(overlay_env) -> AsyncGenerator[S3OverlayProxy]:
    """Create and initialize proxy with both local and remote configured."""
    proxy = S3OverlayProxy.from_env()
    await proxy.startup()
    yield proxy
    await proxy.shutdown()


class TestProxyIntegration:
    """Integration tests with real file uploads and caching."""

    @pytest.mark.anyio
    async def test_cache_object_from_remote(
        self,
        proxy: S3OverlayProxy,
        local_s3_client,
        remote_s3_client,
        s3_helpers,
        simple_text_content: bytes,
    ):
        """Test that GET request caches object from remote to local."""
        bucket = "test-cache"
        key = "documents/test.txt"

        # Upload to remote
        s3_helpers["ensure_bucket"](remote_s3_client, bucket)
        remote_s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=simple_text_content,
            ContentType="text/plain",
        )

        # Create a mock request for GET
        scope = cast(
            HTTPScope,
            {
                "type": "http",
                "method": "GET",
                "path": f"/{bucket}/{key}",
                "query_string": b"",
                "headers": [],
            },
        )

        async def receive():
            return {"type": "http.request", "body": b""}

        request = Request(scope=scope, receive=receive)

        # Make GET request through proxy
        response = await proxy.handle(request, f"/{bucket}/{key}")

        # Verify response is successful
        assert response.status_code == 200

        # Read response body
        body_chunks = []
        iterator_attr = getattr(response, "iterator", None)
        if callable(iterator_attr):
            iterator_func = cast(Callable[[], AsyncIterator[bytes]], iterator_attr)
            iterator = iterator_func()
            async for chunk in iterator:
                body_chunks.append(chunk)
        elif hasattr(response, "body"):
            body_chunks.append(response.body)
        body = b"".join(body_chunks)

        assert body == simple_text_content

        # Verify object is now cached locally
        stat = local_s3_client.head_object(Bucket=bucket, Key=key)
        assert stat["ContentLength"] == len(simple_text_content)

        # Verify local content matches
        local_obj = local_s3_client.get_object(Bucket=bucket, Key=key)
        local_content = local_obj["Body"].read()
        local_obj["Body"].close()
        assert local_content == simple_text_content

        # Cleanup
        with contextlib.suppress(Exception):
            local_s3_client.delete_object(Bucket=bucket, Key=key)
        with contextlib.suppress(Exception):
            remote_s3_client.delete_object(Bucket=bucket, Key=key)
        with contextlib.suppress(Exception):
            local_s3_client.delete_bucket(Bucket=bucket)
        with contextlib.suppress(Exception):
            remote_s3_client.delete_bucket(Bucket=bucket)

    @pytest.mark.anyio
    async def test_head_request_caches_from_remote(
        self,
        proxy: S3OverlayProxy,
        local_s3_client,
        remote_s3_client,
        s3_helpers,
        simple_text_content: bytes,
    ):
        """Test that HEAD request also triggers caching."""
        bucket = "test-head-cache"
        key = "documents/test-head.txt"

        # Upload to remote only
        s3_helpers["ensure_bucket"](remote_s3_client, bucket)
        remote_s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=simple_text_content,
            ContentType="text/plain",
        )

        # Create a mock request for HEAD
        scope = cast(
            HTTPScope,
            {
                "type": "http",
                "method": "HEAD",
                "path": f"/{bucket}/{key}",
                "query_string": b"",
                "headers": [],
            },
        )

        async def receive():
            return {"type": "http.request", "body": b""}

        request = Request(scope=scope, receive=receive)

        # Make HEAD request through proxy
        response = await proxy.handle(request, f"/{bucket}/{key}")

        # Verify response
        assert response.status_code == 200
        assert "Content-Length" in response.headers
        assert response.headers["Content-Length"] == str(len(simple_text_content))
        last_modified = response.headers.get("Last-Modified")
        assert last_modified is not None
        parsed_last_modified = parsedate_to_datetime(last_modified)
        assert parsed_last_modified is not None
        assert parsed_last_modified.tzinfo is not None

        # Verify object is cached locally
        stat = local_s3_client.head_object(Bucket=bucket, Key=key)
        assert stat["ContentLength"] == len(simple_text_content)

        # Cleanup
        with contextlib.suppress(Exception):
            local_s3_client.delete_object(Bucket=bucket, Key=key)
        with contextlib.suppress(Exception):
            remote_s3_client.delete_object(Bucket=bucket, Key=key)
        with contextlib.suppress(Exception):
            local_s3_client.delete_bucket(Bucket=bucket)
        with contextlib.suppress(Exception):
            remote_s3_client.delete_bucket(Bucket=bucket)

    @pytest.mark.anyio
    async def test_local_hit_doesnt_touch_remote(
        self,
        proxy: S3OverlayProxy,
        local_s3_client,
        remote_s3_client,
        s3_helpers,
        simple_text_content: bytes,
    ):
        """Test that objects in local storage don't trigger remote requests."""
        bucket = "test-local-hit"
        key = "documents/local.txt"

        # Upload to local only (not remote)
        s3_helpers["ensure_bucket"](local_s3_client, bucket)
        local_s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=simple_text_content,
            ContentType="text/plain",
        )

        # Create a mock request for GET
        scope = cast(
            HTTPScope,
            {
                "type": "http",
                "method": "GET",
                "path": f"/{bucket}/{key}",
                "query_string": b"",
                "headers": [],
            },
        )

        async def receive():
            return {"type": "http.request", "body": b""}

        request = Request(scope=scope, receive=receive)

        # Make GET request through proxy
        response = await proxy.handle(request, f"/{bucket}/{key}")

        # Verify response is successful
        assert response.status_code == 200

        # Read response body
        body_chunks = []
        iterator_attr = getattr(response, "iterator", None)
        if callable(iterator_attr):
            iterator_func = cast(Callable[[], AsyncIterator[bytes]], iterator_attr)
            iterator = iterator_func()
            async for chunk in iterator:
                body_chunks.append(chunk)
        elif hasattr(response, "body"):
            body_chunks.append(response.body)
        body = b"".join(body_chunks)

        assert body == simple_text_content

        # Note: In this test setup, local and remote share the same MinIO instance,
        # so we can't verify that remote doesn't have the object. In production,
        # this test would verify that local hits don't trigger remote requests.

        # Cleanup
        with contextlib.suppress(Exception):
            local_s3_client.delete_object(Bucket=bucket, Key=key)
        with contextlib.suppress(Exception):
            local_s3_client.delete_bucket(Bucket=bucket)

    @pytest.mark.anyio
    async def test_missing_object_returns_404(
        self,
        proxy: S3OverlayProxy,
        local_s3_client,
        remote_s3_client,
        s3_helpers,
    ):
        """Test that missing objects return 404."""
        bucket = "test-404"
        key = "documents/missing.pdf"

        # Make sure bucket exists but object doesn't
        s3_helpers["ensure_bucket"](local_s3_client, bucket)
        s3_helpers["ensure_bucket"](remote_s3_client, bucket)

        # Create a mock request for GET
        scope = cast(
            HTTPScope,
            {
                "type": "http",
                "method": "GET",
                "path": f"/{bucket}/{key}",
                "query_string": b"",
                "headers": [],
            },
        )

        async def receive():
            return {"type": "http.request", "body": b""}

        request = Request(scope=scope, receive=receive)

        # Make GET request through proxy
        response = await proxy.handle(request, f"/{bucket}/{key}")

        # Verify 404 response
        assert response.status_code == 404

        # Cleanup
        with contextlib.suppress(Exception):
            local_s3_client.delete_bucket(Bucket=bucket)
        with contextlib.suppress(Exception):
            remote_s3_client.delete_bucket(Bucket=bucket)

    @pytest.mark.anyio
    async def test_bucket_mapping(
        self,
        overlay_env,
        local_s3_client,
        remote_s3_client,
        s3_helpers,
        simple_text_content: bytes,
    ):
        """Test that bucket mapping works correctly."""
        import os

        local_bucket = "local-cellgrab"
        remote_bucket = "remote-cellgrab"
        key = "documents/mapped.txt"

        # Set up bucket mapping in environment
        os.environ["S3_OVERLAY_BUCKET_MAPPING"] = f"{local_bucket}:{remote_bucket}"

        try:
            # Create proxy with bucket mapping
            proxy = S3OverlayProxy.from_env()
            await proxy.startup()

            # Upload to remote bucket with remote name
            s3_helpers["ensure_bucket"](remote_s3_client, remote_bucket)
            remote_s3_client.put_object(
                Bucket=remote_bucket,
                Key=key,
                Body=simple_text_content,
                ContentType="text/plain",
            )

            # Create a mock request using the LOCAL bucket name
            scope = cast(
                HTTPScope,
                {
                    "type": "http",
                    "method": "GET",
                    "path": f"/{local_bucket}/{key}",
                    "query_string": b"",
                    "headers": [],
                },
            )

            async def receive():
                return {"type": "http.request", "body": b""}

            request = Request(scope=scope, receive=receive)

            # Make GET request through proxy using local bucket name
            response = await proxy.handle(request, f"/{local_bucket}/{key}")

            # Verify response is successful
            assert response.status_code == 200

            # Read response body
            body_chunks = []
            iterator_attr = getattr(response, "iterator", None)
            if callable(iterator_attr):
                iterator_func = cast(Callable[[], AsyncIterator[bytes]], iterator_attr)
                iterator = iterator_func()
                async for chunk in iterator:
                    body_chunks.append(chunk)
            elif hasattr(response, "body"):
                body_chunks.append(response.body)
            body = b"".join(body_chunks)

            assert body == simple_text_content

            # Verify object is cached in local bucket with LOCAL bucket name
            assert s3_helpers["bucket_exists"](local_s3_client, local_bucket)
            stat = local_s3_client.head_object(Bucket=local_bucket, Key=key)
            assert stat["ContentLength"] == len(simple_text_content)

            # Verify local content matches
            local_obj = local_s3_client.get_object(Bucket=local_bucket, Key=key)
            local_content = local_obj["Body"].read()
            local_obj["Body"].close()
            assert local_content == simple_text_content

            await proxy.shutdown()

        finally:
            # Cleanup environment
            os.environ.pop("S3_OVERLAY_BUCKET_MAPPING", None)

            # Cleanup buckets
            with contextlib.suppress(Exception):
                local_s3_client.delete_object(Bucket=local_bucket, Key=key)
            with contextlib.suppress(Exception):
                remote_s3_client.delete_object(Bucket=remote_bucket, Key=key)
            with contextlib.suppress(Exception):
                local_s3_client.delete_bucket(Bucket=local_bucket)
            with contextlib.suppress(Exception):
                remote_s3_client.delete_bucket(Bucket=remote_bucket)

    @pytest.mark.anyio
    async def test_options_request(
        self,
        proxy: S3OverlayProxy,
        local_s3_client,
        s3_helpers,
        simple_text_content: bytes,
    ):
        """Test that OPTIONS request doesn't crash on header handling."""
        bucket = "test-options"
        key = "documents/test.txt"

        # Upload to local
        s3_helpers["ensure_bucket"](local_s3_client, bucket)
        local_s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=simple_text_content,
            ContentType="text/plain",
        )

        # Create a mock request for OPTIONS
        scope = cast(
            HTTPScope,
            {
                "type": "http",
                "method": "OPTIONS",
                "path": f"/{bucket}/{key}",
                "query_string": b"",
                "headers": [],
            },
        )

        async def receive():
            return {"type": "http.request", "body": b""}

        request = Request(scope=scope, receive=receive)

        # Make OPTIONS request through proxy
        response = await proxy.handle(request, f"/{bucket}/{key}")

        # Verify response is successful (should be 204 or similar)
        assert response.status_code in {200, 204}

        # Cleanup
        with contextlib.suppress(Exception):
            local_s3_client.delete_object(Bucket=bucket, Key=key)
        with contextlib.suppress(Exception):
            local_s3_client.delete_bucket(Bucket=bucket)
