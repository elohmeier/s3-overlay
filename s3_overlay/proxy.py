from __future__ import annotations

import logging
from datetime import UTC, datetime
from email.utils import format_datetime
from functools import partial
from typing import TYPE_CHECKING, Any, Literal

import httpx
from anyio import to_thread
from boto3.session import Session
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError
from litestar.response import Response, Stream
from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterable, Mapping

    from litestar import Request
else:  # pragma: no cover
    AsyncIterator = Iterable = Mapping = Any

LOG = logging.getLogger("s3_overlay.proxy")


async def _run_sync(func: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Any:
    return await to_thread.run_sync(func, *args, **kwargs)


class LocalSettings(BaseSettings):
    """Configuration for local S3 storage."""

    model_config = SettingsConfigDict(
        env_prefix="", case_sensitive=False, extra="ignore"
    )

    endpoint: str = Field(
        default="http://127.0.0.1:9000",
        validation_alias="S3_OVERLAY_LOCAL_ENDPOINT",
    )
    access_key: str = Field(
        default="minioadmin",
        validation_alias=AliasChoices(
            "S3_OVERLAY_LOCAL_ACCESS_KEY",
            "AWS_ACCESS_KEY_ID",
        ),
    )
    secret_key: str = Field(
        default="minioadmin",
        validation_alias=AliasChoices(
            "S3_OVERLAY_LOCAL_SECRET_KEY",
            "AWS_SECRET_ACCESS_KEY",
        ),
    )
    session_token: str | None = Field(
        default=None,
        validation_alias="S3_OVERLAY_LOCAL_SESSION_TOKEN",
    )
    region: str = Field(
        default="us-east-1",
        validation_alias="S3_OVERLAY_LOCAL_REGION",
    )
    bucket_location: str = Field(
        default="us-east-1",
        validation_alias="S3_OVERLAY_DEFAULT_BUCKET_LOCATION",
    )
    chunk_threshold: int = Field(
        default=50 * 1024 * 1024,
        validation_alias="S3_OVERLAY_CHUNK_THRESHOLD",
    )
    chunk_size: int = Field(
        default=16 * 1024 * 1024,
        validation_alias="S3_OVERLAY_CHUNK_SIZE",
    )
    cache_bucket_name: str = Field(
        default="s3-overlay-cache",
        validation_alias="S3_OVERLAY_CACHE_BUCKET",
    )
    cache_enabled: bool = Field(
        default=False,
        validation_alias="S3_OVERLAY_CACHE_ENABLED",
    )


class RemoteSettings(BaseSettings):
    """Configuration for remote S3 storage."""

    model_config = SettingsConfigDict(
        env_prefix="", case_sensitive=False, extra="ignore"
    )

    endpoint: str | None = Field(
        default=None,
        validation_alias="S3_OVERLAY_REMOTE_ENDPOINT",
    )
    access_key: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "S3_OVERLAY_REMOTE_ACCESS_KEY_ID",
            "AWS_REMOTE_ACCESS_KEY_ID",
        ),
    )
    secret_key: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "S3_OVERLAY_REMOTE_SECRET_ACCESS_KEY",
            "AWS_REMOTE_SECRET_ACCESS_KEY",
        ),
    )
    session_token: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "S3_OVERLAY_REMOTE_SESSION_TOKEN",
            "AWS_REMOTE_SESSION_TOKEN",
        ),
    )
    region: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "S3_OVERLAY_REMOTE_REGION",
            "AWS_REGION",
        ),
    )
    bucket_mapping: dict[str, str] | None = Field(
        default=None,
        validation_alias="S3_OVERLAY_BUCKET_MAPPING",
    )
    addressing_style: Literal["auto", "virtual", "path"] = Field(
        default="virtual",
        validation_alias="S3_OVERLAY_REMOTE_ADDRESSING_STYLE",
    )

    @field_validator("bucket_mapping", mode="before")
    @classmethod
    def _parse_bucket_mapping(cls, value: object) -> dict[str, str] | None:
        if value is None:
            return None
        if isinstance(value, dict):
            return {str(k).strip(): str(v).strip() for k, v in value.items()}
        if isinstance(value, str):
            mapping: dict[str, str] = {}
            for pair in value.split(","):
                if ":" in pair:
                    local, remote = pair.split(":", 1)
                    mapping[local.strip()] = remote.strip()
            return mapping or None
        msg = "Invalid bucket mapping format"
        raise ValueError(msg)

    @property
    def enabled(self) -> bool:
        """Check if remote storage is enabled based on configuration."""
        return bool(self.endpoint or self.access_key or self.secret_key or self.region)


def load_local_settings_from_env() -> LocalSettings:
    """Load local S3 settings from environment variables.

    Returns:
        LocalSettings instance populated from environment variables.
    """
    return LocalSettings()


def load_remote_settings_from_env() -> RemoteSettings:
    """Load remote S3 settings from environment variables.

    Returns:
        RemoteSettings instance populated from environment variables.
    """
    return RemoteSettings()


class S3OverlayProxy:
    def __init__(self, local: LocalSettings, remote: RemoteSettings):
        self._local_settings = local
        self._remote_settings = remote
        self._http_client: httpx.AsyncClient | None = None
        self._local_client = self._build_local_client()
        self._remote_client = self._build_remote_client() if remote.enabled else None

    async def startup(self) -> None:
        self._http_client = httpx.AsyncClient(
            base_url=self._local_settings.endpoint,
            timeout=httpx.Timeout(60.0, read=300.0),
            trust_env=False,
        )
        LOG.info(
            "S3 overlay ready (local=%s, remote=%s, cache=%s)",
            self._local_settings.endpoint,
            self._describe_remote(),
            "enabled" if self._local_settings.cache_enabled else "disabled",
        )

    async def shutdown(self) -> None:
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None

    async def handle(self, request: Request, path: str) -> Response:
        LOG.debug("handle method=%s path=%s", request.method, path)
        if request.method in {"GET", "HEAD"}:
            response = await self._handle_read(request, path)
            if response is not None:
                LOG.debug("handled via read path=%s", path)
                return response
        LOG.debug("falling back to direct proxy path=%s", path)

        if self._http_client is None:
            message = "proxy not initialised"
            raise RuntimeError(message)

        local_request = await self._build_httpx_request(request, path)
        response = await self._http_client.send(local_request, stream=True)

        if response.status_code == 404 and self._should_backfill(request, path):
            await response.aclose()
            if await self._backfill_from_remote(request, path):
                local_request = await self._build_httpx_request(request, path)
                response = await self._http_client.send(local_request, stream=True)

        return self._to_streaming_response(response)

    def _build_local_client(self):
        session = Session(
            aws_access_key_id=self._local_settings.access_key,
            aws_secret_access_key=self._local_settings.secret_key,
            aws_session_token=self._local_settings.session_token,
            region_name=self._local_settings.region,
        )
        return session.client(
            "s3",
            endpoint_url=self._local_settings.endpoint,
            config=BotoConfig(signature_version="s3v4", retries={"max_attempts": 3}),
        )

    def _build_remote_client(self):
        session = Session(
            aws_access_key_id=self._remote_settings.access_key,
            aws_secret_access_key=self._remote_settings.secret_key,
            aws_session_token=self._remote_settings.session_token,
            region_name=self._remote_settings.region,
        )
        return session.client(
            "s3",
            endpoint_url=self._remote_settings.endpoint,
            config=BotoConfig(
                signature_version="s3v4",
                retries={"max_attempts": 3},
                s3={"addressing_style": self._remote_settings.addressing_style},
            ),
        )

    async def _build_httpx_request(self, request: Request, path: str) -> httpx.Request:
        assert self._http_client is not None
        url = path or "/"
        if request.scope.get("query_string"):
            query = request.scope["query_string"].decode("latin-1")
            url = f"{url}?{query}"

        headers = self._prepare_outgoing_headers(request.headers)
        content = None
        if request.method in {"POST", "PUT", "PATCH"}:
            content = await self._build_content_bytes(request)

        return self._http_client.build_request(
            method=request.method,
            url=url,
            headers=headers,
            content=content,
        )

    def _prepare_outgoing_headers(self, headers: Mapping[str, str]) -> dict[str, str]:
        hop_by_hop = {
            "connection",
            "keep-alive",
            "proxy-authenticate",
            "proxy-authorization",
            "te",
            "trailers",
            "transfer-encoding",
            "upgrade",
        }
        prepared: dict[str, str] = {}
        for key, value in headers.items():
            lowered = key.lower()
            if lowered in hop_by_hop:
                continue
            prepared[lowered if lowered == "host" else key] = value
        return prepared

    async def _build_content_bytes(self, request: Request) -> bytes:
        """Read the entire request body as bytes directly from ASGI scope."""
        body_parts = []
        receive = request.receive

        while True:
            message = await receive()
            if message["type"] == "http.request":
                body = message.get("body", b"")
                if body:
                    body_parts.append(body)
                if not message.get("more_body", False):
                    break
            elif message["type"] == "http.disconnect":
                break

        return b"".join(body_parts)

    def _should_backfill(self, request: Request, path: str) -> bool:
        if request.method not in {"GET", "HEAD"}:
            return False
        if not self._remote_client:
            return False
        bucket, key = self._extract_bucket_and_key(path)
        return bool(bucket and key is not None and key != "")

    async def _handle_read(self, request: Request, path: str) -> Response | None:
        LOG.debug("handle_read method=%s path=%s", request.method, path)
        bucket, key = self._extract_bucket_and_key(path)
        if not bucket or key is None:
            LOG.debug("missing bucket/key for path %s", path)
            return None
        if key == "":
            # Empty key means bucket listing, not object operation
            LOG.debug("empty key (bucket listing) for path %s", path)
            return None

        range_header = request.headers.get("range")

        try:
            if request.method == "HEAD":
                result = await _run_sync(
                    partial(self._local_client.head_object, Bucket=bucket, Key=key)
                )
                headers = self._object_headers(result)
                LOG.debug("HEAD hit for s3://%s/%s", bucket, key)
                return Response(content=b"", headers=headers, status_code=200)

            get_kwargs = {"Bucket": bucket, "Key": key}
            if range_header:
                get_kwargs["Range"] = range_header
            result = await _run_sync(
                partial(self._local_client.get_object, **get_kwargs)
            )
        except ClientError as error:
            miss_result = await self._handle_read_miss(
                error, request, path, bucket, key
            )
            if isinstance(miss_result, Response):
                return miss_result
            if not miss_result:
                return self._from_client_error(error)

            if request.method == "HEAD":
                result = await _run_sync(
                    partial(self._local_client.head_object, Bucket=bucket, Key=key)
                )
                headers = self._object_headers(result)
                return Response(content=b"", headers=headers, status_code=200)

            get_kwargs = {"Bucket": bucket, "Key": key}
            if range_header:
                get_kwargs["Range"] = range_header
            result = await _run_sync(
                partial(self._local_client.get_object, **get_kwargs)
            )

        headers = self._object_headers(result)
        streaming_body = result["Body"]

        async def iterator() -> AsyncIterator[bytes]:
            try:
                while True:
                    chunk = await _run_sync(streaming_body.read, 1024 * 64)
                    if not chunk:
                        break
                    yield chunk
            finally:
                await _run_sync(streaming_body.close)

        status_code = 206 if range_header else 200
        LOG.debug("GET hit for s3://%s/%s status=%s", bucket, key, status_code)
        return Stream(content=iterator, status_code=status_code, headers=headers)

    async def _handle_read_miss(
        self,
        error: ClientError,
        request: Request,
        path: str,
        bucket: str,
        key: str,
    ) -> bool | Response:
        code = error.response.get("Error", {}).get("Code")
        if code not in {"404", "NoSuchKey", "NotFound", "NoSuchBucket"}:
            raise error
        if not self._remote_client:
            return False

        remote_bucket = self._map_to_remote_bucket(bucket)

        # Check remote object metadata first
        try:
            remote_head = await _run_sync(
                partial(self._remote_client.head_object, Bucket=remote_bucket, Key=key)
            )
        except ClientError as e:
            err_code = e.response.get("Error", {}).get("Code")
            if err_code in {"404", "NoSuchKey", "NotFound"}:
                LOG.debug(
                    "remote miss for s3://%s/%s (remote: %s)",
                    bucket,
                    key,
                    remote_bucket,
                )
                return False
            LOG.warning(
                "remote error for s3://%s/%s (remote bucket: %s): %s",
                bucket,
                key,
                remote_bucket,
                e,
            )
            return False

        size = remote_head.get("ContentLength", 0)
        range_header = request.headers.get("range")

        # When caching is disabled, always stream directly from remote
        if not self._local_settings.cache_enabled:
            LOG.debug(
                "caching disabled, streaming from remote s3://%s/%s (size=%d)",
                bucket,
                key,
                size,
            )
            return self._stream_from_remote(
                key, size, range_header, remote_bucket, remote_head
            )

        # If file is large, use chunked caching
        if size > self._local_settings.chunk_threshold:
            LOG.info(
                "using chunked caching for s3://%s/%s (size=%d, threshold=%d)",
                bucket,
                key,
                size,
                self._local_settings.chunk_threshold,
            )
            return await self._serve_chunked(
                bucket,
                key,
                size,
                range_header,
                remote_bucket,
                remote_head,
            )

        return await self._backfill_from_remote(request, path)

    def _object_headers(self, result: Mapping[str, Any]) -> dict[str, str]:
        headers: dict[str, str] = {}
        mapping = {
            "Accept-Ranges": "AcceptRanges",
            "Cache-Control": "CacheControl",
            "Content-Disposition": "ContentDisposition",
            "Content-Encoding": "ContentEncoding",
            "Content-Language": "ContentLanguage",
            "Content-Length": "ContentLength",
            "Content-Range": "ContentRange",
            "Content-Type": "ContentType",
            "ETag": "ETag",
            "Expires": "Expires",
            "Last-Modified": "LastModified",
            "x-amz-delete-marker": "DeleteMarker",
            "x-amz-version-id": "VersionId",
            "x-amz-storage-class": "StorageClass",
        }

        for header, key in mapping.items():
            value = result.get(key)
            if value is None:
                continue
            headers[header] = self._format_header_value(value)

        metadata = result.get("Metadata") or {}
        for meta_key, meta_value in metadata.items():
            headers[f"x-amz-meta-{meta_key}"] = meta_value

        return headers

    @staticmethod
    def _format_header_value(value: Any) -> str:
        if isinstance(value, datetime):
            aware = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
            aware = aware.astimezone(UTC)
            return format_datetime(aware, usegmt=True)
        return str(value)

    def _from_client_error(self, error: ClientError) -> Response:
        status_code = int(
            error.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 500)
        )
        message = error.response.get("Error", {}).get(
            "Message", "Internal Server Error"
        )
        return Response(content=message, status_code=status_code)

    def _map_to_remote_bucket(self, local_bucket: str) -> str:
        """Map a local bucket name to its remote equivalent."""
        if not self._remote_settings.bucket_mapping:
            return local_bucket
        mapped = self._remote_settings.bucket_mapping.get(local_bucket)
        if mapped is None:
            LOG.debug(
                "no remote mapping for local bucket %r, using as-is "
                "(configured mappings: %s)",
                local_bucket,
                ", ".join(
                    f"{k}->{v}" for k, v in self._remote_settings.bucket_mapping.items()
                ),
            )
            return local_bucket
        return mapped

    async def _backfill_from_remote(self, request: Request, path: str) -> bool:
        if not self._remote_client:
            return False

        bucket, key = self._extract_bucket_and_key(path)
        if not bucket or key is None:
            return False

        remote_bucket = self._map_to_remote_bucket(bucket)
        range_header = request.headers.get("range")
        try:
            remote_obj = await _run_sync(
                partial(self._remote_client.get_object, Bucket=remote_bucket, Key=key)
            )
        except ClientError as error:
            code = error.response.get("Error", {}).get("Code")
            if code in {"404", "NoSuchKey", "NotFound"}:
                LOG.debug(
                    "remote miss for s3://%s/%s (remote: %s)",
                    bucket,
                    key,
                    remote_bucket,
                )
                return False
            LOG.warning(
                "remote backfill failed for s3://%s/%s (remote bucket: %s): %s",
                bucket,
                key,
                remote_bucket,
                error,
            )
            return False

        body = await _run_sync(remote_obj["Body"].read)
        await _run_sync(remote_obj["Body"].close)

        await self._ensure_bucket(bucket)

        put_kwargs = {
            "Bucket": bucket,
            "Key": key,
            "Body": body,
            "Metadata": remote_obj.get("Metadata", {}),
        }

        for field in (
            "ContentType",
            "ContentEncoding",
            "ContentDisposition",
            "ContentLanguage",
            "CacheControl",
        ):
            if remote_obj.get(field):
                put_kwargs[field] = remote_obj[field]

        if remote_obj.get("Expires"):
            put_kwargs["Expires"] = remote_obj["Expires"]

        try:
            await _run_sync(partial(self._local_client.put_object, **put_kwargs))
        except ClientError:
            LOG.warning(
                "failed to cache remote object s3://%s/%s (non-fatal)",
                bucket,
                key,
                exc_info=True,
            )
            return False
        LOG.info(
            "cached remote object s3://%s/%s (%s bytes)%s (from remote: %s)",
            bucket,
            key,
            len(body),
            " with Range" if range_header else "",
            remote_bucket,
        )
        return True

    async def _ensure_bucket(self, bucket: str) -> None:
        try:
            await _run_sync(partial(self._local_client.head_bucket, Bucket=bucket))
        except ClientError as error:
            code = error.response.get("Error", {}).get("Code")
            if code not in {"404", "NoSuchBucket", "NotFound"}:
                raise
            create_kwargs = {"Bucket": bucket}
            location = self._local_settings.bucket_location
            if location and location != "us-east-1":
                create_kwargs["CreateBucketConfiguration"] = {
                    "LocationConstraint": location
                }
            await _run_sync(partial(self._local_client.create_bucket, **create_kwargs))
            LOG.info("created local bucket %s", bucket)

    def _extract_bucket_and_key(self, path: str) -> tuple[str | None, str | None]:
        trimmed = path.lstrip("/")
        if not trimmed:
            return None, None
        if "/" not in trimmed:
            return trimmed, ""
        bucket, key = trimmed.split("/", 1)
        return bucket, key

    def _to_streaming_response(self, response: httpx.Response) -> Response:
        headers = self._prepare_response_headers(response.headers.raw)

        async def iterator() -> AsyncIterator[bytes]:
            try:
                async for chunk in response.aiter_raw():
                    yield chunk
            finally:
                await response.aclose()

        return Stream(
            content=iterator(), status_code=response.status_code, headers=headers
        )

    def _prepare_response_headers(
        self, headers: list[tuple[bytes, bytes]]
    ) -> dict[str, str]:
        hop_by_hop = {
            "connection",
            "keep-alive",
            "proxy-authenticate",
            "proxy-authorization",
            "te",
            "trailers",
            "transfer-encoding",
            "upgrade",
        }
        prepared: dict[str, str] = {}
        for key_bytes, value_bytes in headers:
            key = key_bytes.decode("latin-1")
            value = value_bytes.decode("latin-1")
            if key.lower() in hop_by_hop:
                continue
            prepared[key] = value
        return prepared

    def _describe_remote(self) -> str:
        if not self._remote_client:
            return "disabled"
        endpoint = self._remote_settings.endpoint or "aws"
        region = self._remote_settings.region or "default"
        return f"{endpoint} ({region})"

    @classmethod
    def from_env(cls) -> S3OverlayProxy:
        """Create an S3OverlayProxy instance from environment variables.

        Returns:
            S3OverlayProxy configured from environment variables.
        """
        return cls(
            local=load_local_settings_from_env(),
            remote=load_remote_settings_from_env(),
        )

    def _stream_from_remote(
        self,
        key: str,
        total_size: int,
        range_header: str | None,
        remote_bucket: str,
        remote_head: dict[str, Any],
    ) -> Response:
        """Stream an object directly from remote without caching locally."""
        assert self._remote_client is not None
        remote_client = self._remote_client
        start, end = self._parse_range(range_header, total_size)
        content_length = end - start + 1

        async def iterator() -> AsyncIterator[bytes]:
            get_kwargs: dict[str, Any] = {"Bucket": remote_bucket, "Key": key}
            request_range = f"bytes={start}-{end}"
            get_kwargs["Range"] = request_range
            result = await _run_sync(partial(remote_client.get_object, **get_kwargs))
            stream = result["Body"]
            try:
                while True:
                    chunk = await _run_sync(stream.read, 1024 * 64)
                    if not chunk:
                        break
                    yield chunk
            finally:
                await _run_sync(stream.close)

        headers = self._object_headers(remote_head)
        if range_header:
            headers.update(
                {
                    "Content-Length": str(content_length),
                    "Content-Range": f"bytes {start}-{end}/{total_size}",
                }
            )
            return Stream(content=iterator, status_code=206, headers=headers)

        headers["Content-Length"] = str(total_size)
        return Stream(content=iterator, status_code=200, headers=headers)

    async def _serve_chunked(
        self,
        bucket: str,
        key: str,
        total_size: int,
        range_header: str | None,
        remote_bucket: str,
        remote_head: dict[str, Any],
    ) -> Response:
        start, end = self._parse_range(range_header, total_size)
        content_length = end - start + 1
        chunk_size = self._local_settings.chunk_size
        cache_bucket = self._local_settings.cache_bucket_name

        await self._ensure_bucket(cache_bucket)

        async def iterator() -> AsyncIterator[bytes]:
            current_pos = start
            while current_pos <= end:
                chunk_index = current_pos // chunk_size
                chunk_start_in_file = chunk_index * chunk_size
                offset_in_chunk = current_pos - chunk_start_in_file

                bytes_left_in_request = end - current_pos + 1
                bytes_left_in_chunk = chunk_size - offset_in_chunk

                # Last chunk might be smaller than chunk_size
                if chunk_start_in_file + chunk_size > total_size:
                    bytes_left_in_chunk = (
                        total_size - chunk_start_in_file - offset_in_chunk
                    )

                bytes_to_read = min(bytes_left_in_request, bytes_left_in_chunk)

                chunk_bytes = await self._fetch_chunk(
                    bucket,
                    key,
                    chunk_index,
                    chunk_size,
                    total_size,
                    remote_bucket,
                    cache_bucket,
                    offset_in_chunk,
                    bytes_to_read,
                )
                yield chunk_bytes
                current_pos += bytes_to_read

        headers = self._object_headers(remote_head)
        headers.update(
            {
                "Content-Length": str(content_length),
                "Content-Range": f"bytes {start}-{end}/{total_size}",
            }
        )
        # Remove ETag as it might correspond to the full file and we are sending partial?
        # S3 usually keeps ETag for ranges.

        status_code = 206
        return Stream(content=iterator, status_code=status_code, headers=headers)

    async def _fetch_chunk(
        self,
        bucket: str,
        key: str,
        index: int,
        chunk_size: int,
        total_size: int,
        remote_bucket: str,
        cache_bucket: str,
        offset_in_chunk: int,
        bytes_to_read: int,
    ) -> bytes:
        """Return chunk data, using cache when available, falling back to remote.

        The local cache write is best-effort: if it fails (e.g. storage full),
        the data is still served from the remote download.
        """
        assert self._remote_client is not None
        chunk_key = f"v1/{bucket}/{key}/{chunk_size}/{index}"
        chunk_range = f"bytes={offset_in_chunk}-{offset_in_chunk + bytes_to_read - 1}"

        # 1. Try reading from cache
        try:
            obj = await _run_sync(
                partial(
                    self._local_client.get_object,
                    Bucket=cache_bucket,
                    Key=chunk_key,
                    Range=chunk_range,
                )
            )
            stream = obj["Body"]
            try:
                return await _run_sync(stream.read)
            finally:
                await _run_sync(stream.close)
        except ClientError:
            pass  # Cache miss — download from remote

        # 2. Download full chunk from remote
        start = index * chunk_size
        end = min(start + chunk_size, total_size) - 1
        remote_range = f"bytes={start}-{end}"

        try:
            remote_obj = await _run_sync(
                partial(
                    self._remote_client.get_object,
                    Bucket=remote_bucket,
                    Key=key,
                    Range=remote_range,
                )
            )
        except ClientError:
            LOG.exception("Failed to download chunk %s from remote", chunk_key)
            raise

        body = await _run_sync(remote_obj["Body"].read)
        await _run_sync(remote_obj["Body"].close)

        # 3. Best-effort cache write — never fail the request
        try:
            await _run_sync(
                partial(
                    self._local_client.put_object,
                    Bucket=cache_bucket,
                    Key=chunk_key,
                    Body=body,
                )
            )
            LOG.debug("cached chunk %s (%d bytes)", chunk_key, len(body))
        except ClientError:
            LOG.warning(
                "failed to cache chunk %s (non-fatal, serving from remote)",
                chunk_key,
                exc_info=True,
            )

        # 4. Slice to the requested range within the chunk
        return body[offset_in_chunk : offset_in_chunk + bytes_to_read]

    def _parse_range(
        self, range_header: str | None, total_size: int
    ) -> tuple[int, int]:
        if not range_header:
            return 0, total_size - 1

        try:
            unit, ranges = range_header.split("=", 1)
            if unit.strip().lower() != "bytes":
                return 0, total_size - 1

            r = ranges.split(",")[0].strip()
            if "-" not in r:
                return 0, total_size - 1

            start_str, end_str = r.split("-", 1)

            if start_str and end_str:
                start = int(start_str)
                end = int(end_str)
            elif start_str:
                start = int(start_str)
                end = total_size - 1
            elif end_str:
                length = int(end_str)
                start = total_size - length
                end = total_size - 1
            else:
                return 0, total_size - 1

            if end >= total_size:
                end = total_size - 1
            if start < 0:
                start = 0
            if start > end:
                # Invalid range, fallback to full
                return 0, total_size - 1
        except ValueError:
            return 0, total_size - 1
        else:
            return start, end
