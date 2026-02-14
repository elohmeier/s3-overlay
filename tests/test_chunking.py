"""Integration tests for chunk-based caching."""

from __future__ import annotations

import contextlib
import os
from collections.abc import AsyncGenerator, AsyncIterator, Callable
from typing import TYPE_CHECKING, cast

import pytest
from litestar import Request
from litestar.types import HTTPScope
from s3_overlay import S3OverlayProxy

if TYPE_CHECKING:
    from botocore.client import BaseClient


@pytest.fixture
def chunking_env(overlay_env) -> dict[str, str]:
    """Environment variables with low chunk threshold."""
    env = overlay_env.copy()
    # Set threshold to 1KB to force chunking for small files in test
    env["S3_OVERLAY_CHUNK_THRESHOLD"] = "1024"
    # Set chunk size to 512 bytes so we have multiple chunks for a 5KB file
    env["S3_OVERLAY_CHUNK_SIZE"] = "512"
    env["S3_OVERLAY_BUCKET_MAPPING"] = "test-chunking:test-chunking-remote"
    return env


@pytest.fixture
async def chunking_proxy(chunking_env) -> AsyncGenerator[S3OverlayProxy]:
    """Create proxy with chunking configuration."""
    # Apply env vars for this test
    original_environ = os.environ.copy()
    os.environ.update(chunking_env)

    proxy = S3OverlayProxy.from_env()
    await proxy.startup()
    yield proxy
    await proxy.shutdown()

    os.environ.clear()
    os.environ.update(original_environ)


class TestChunking:
    """Tests for partial/chunk-based caching."""

    @pytest.mark.anyio
    async def test_large_file_uses_chunking(
        self,
        chunking_proxy: S3OverlayProxy,
        local_s3_client: BaseClient,
        remote_s3_client: BaseClient,
        s3_helpers,
    ):
        """Test that a file larger than threshold is cached in chunks."""
        bucket = "test-chunking"
        remote_bucket = "test-chunking-remote"
        key = "large-file.bin"
        content = b"x" * 5000  # 5KB file, > 1KB threshold

        # Upload to remote
        s3_helpers["ensure_bucket"](remote_s3_client, remote_bucket)
        remote_s3_client.put_object(
            Bucket=remote_bucket,
            Key=key,
            Body=content,
            ContentType="application/octet-stream",
        )

        # Request bytes 0-100 (part of first chunk 0-512)
        scope = cast(
            HTTPScope,
            {
                "type": "http",
                "method": "GET",
                "path": f"/{bucket}/{key}",
                "query_string": b"",
                "headers": [(b"range", b"bytes=0-100")],
            },
        )

        async def receive():
            return {"type": "http.request", "body": b""}

        request = Request(scope=scope, receive=receive)
        response = await chunking_proxy.handle(request, f"/{bucket}/{key}")

        assert response.status_code == 206

        # Read response body
        body_chunks = []
        iterator_attr = getattr(response, "iterator", None)
        if callable(iterator_attr):
            iterator_func = cast(Callable[[], AsyncIterator[bytes]], iterator_attr)
            iterator = iterator_func()
            async for chunk in iterator:
                body_chunks.append(chunk)
        body = b"".join(body_chunks)

        # Verify content
        assert len(body) == 101
        assert body == content[0:101]

        # VERIFICATION

        # 1. Main bucket should NOT contain the file (because it's "large")
        # Note: If logic isn't implemented yet, this assertion will fail because it will backfill full file.
        # But wait, if it backfills full file, it will be there.
        # With chunking implementation, it should NOT be there.
        # Check if bucket exists first (it might be created by _ensure_bucket)
        if s3_helpers["bucket_exists"](local_s3_client, bucket):
            # It shouldn't have the object
            # (In standard MinIO, ListObjects might not show it, checking stat_object)
            try:
                local_s3_client.head_object(Bucket=bucket, Key=key)
                exists = True
            except Exception:
                exists = False
            assert not exists, "Large file should not be backfilled to main bucket"

        # 2. Cache bucket should contain chunks
        cache_bucket = os.getenv("S3_OVERLAY_CACHE_BUCKET", "s3-overlay-cache")
        assert s3_helpers["bucket_exists"](local_s3_client, cache_bucket)

        # Chunk 0 should be there (0-512)
        # Key format: v1/{bucket}/{key}/{chunk_size}/{index}
        # key has no slashes in this test, but handled generically
        chunk_key = f"v1/{bucket}/{key}/512/0"

        try:
            stat = local_s3_client.head_object(Bucket=cache_bucket, Key=chunk_key)
            assert stat["ContentLength"] == 512
        except Exception as e:
            pytest.fail(f"Chunk 0 not found in cache bucket: {e}")

        # Cleanup
        with contextlib.suppress(Exception):
            local_s3_client.delete_object(Bucket=cache_bucket, Key=chunk_key)
            local_s3_client.delete_bucket(Bucket=cache_bucket)
            remote_s3_client.delete_object(Bucket=remote_bucket, Key=key)
            remote_s3_client.delete_bucket(Bucket=remote_bucket)
