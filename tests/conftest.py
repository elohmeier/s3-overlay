from __future__ import annotations

import os
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Generator

    from botocore.client import BaseClient
    from pytest_databases._service import DockerService


@dataclass
class MinioService:
    endpoint: str
    access_key: str
    secret_key: str
    secure: bool


@pytest.fixture(scope="session")
def minio_access_key() -> str:
    return os.getenv("MINIO_ACCESS_KEY", "minio")


@pytest.fixture(scope="session")
def minio_secret_key() -> str:
    return os.getenv("MINIO_SECRET_KEY", "minio123")


@pytest.fixture(scope="session")
def minio_secure() -> bool:
    return os.getenv("MINIO_SECURE", "false").lower() in {
        "true",
        "1",
        "yes",
        "y",
        "t",
        "on",
    }


@pytest.fixture(scope="session")
def minio_service(
    docker_service: DockerService,
    minio_access_key: str,
    minio_secret_key: str,
    minio_secure: bool,
    minio_service_name: str,
) -> Generator[MinioService]:
    from urllib.error import URLError
    from urllib.request import Request, urlopen

    from pytest_databases.types import ServiceContainer

    def check(_service: ServiceContainer) -> bool:
        scheme = "https" if minio_secure else "http"
        url = f"{scheme}://{_service.host}:{_service.port}/minio/health/ready"
        if not url.startswith(("http:", "https:")):
            msg = "URL must start with 'http:' or 'https:'"
            raise ValueError(msg)
        try:
            with urlopen(url=Request(url, method="GET"), timeout=10) as response:
                return response.status == 200
        except (URLError, ConnectionError):
            return False

    command = "server /data"
    env = {
        "MINIO_ROOT_USER": minio_access_key,
        "MINIO_ROOT_PASSWORD": minio_secret_key,
    }

    with docker_service.run(
        image="quay.io/minio/minio",
        name=minio_service_name,
        command=command,
        container_port=9000,
        timeout=20,
        pause=0.5,
        env=env,
        check=check,
    ) as service:
        yield MinioService(
            endpoint=f"{service.host}:{service.port}",
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=minio_secure,
        )


def _s3_client_from_service(minio_service: MinioService):
    import boto3
    from botocore.config import Config

    scheme = "https" if minio_service.secure else "http"
    endpoint = f"{scheme}://{minio_service.endpoint}"
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=minio_service.access_key,
        aws_secret_access_key=minio_service.secret_key,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def _bucket_exists(client, bucket: str) -> bool:
    from botocore.exceptions import ClientError

    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in {"404", "NoSuchBucket", "NotFound"}:
            return False
        raise
    else:
        return True


def _ensure_bucket(client, bucket: str) -> None:
    if not _bucket_exists(client, bucket):
        client.create_bucket(Bucket=bucket)


@pytest.fixture(scope="session")
def minio_service_name() -> str:
    """Override to use a custom name for the MinIO service (local)."""
    return "minio-local"


@pytest.fixture(scope="session")
def minio_service_port() -> int:
    """Override to use a custom port for the local MinIO service."""
    return 9100  # Different from default 9000


@pytest.fixture
def local_s3_client(minio_service: MinioService) -> BaseClient:
    """Create a boto3 S3 client for the local MinIO service."""
    return _s3_client_from_service(minio_service)


@pytest.fixture
def remote_s3_client(minio_service: MinioService) -> BaseClient:
    """Create a boto3 S3 client that simulates a remote service.

    For simplicity in testing, we use the same MinIO instance but with
    separate buckets to simulate local vs remote.
    """
    return _s3_client_from_service(minio_service)


@pytest.fixture
def local_env_vars(
    minio_service: MinioService,
) -> Generator[dict[str, str]]:
    """Set up environment variables for local MinIO."""
    scheme = "https" if minio_service.secure else "http"
    endpoint = f"{scheme}://{minio_service.endpoint}"

    env_vars = {
        "S3_OVERLAY_LOCAL_ENDPOINT": endpoint,
        "S3_OVERLAY_LOCAL_ACCESS_KEY": minio_service.access_key,
        "S3_OVERLAY_LOCAL_SECRET_KEY": minio_service.secret_key,
        "S3_OVERLAY_LOCAL_REGION": "us-east-1",
    }

    # Set environment variables
    original_values = {}
    for key, value in env_vars.items():
        original_values[key] = os.environ.get(key)
        os.environ[key] = value

    yield env_vars

    # Restore original values
    for key, original_value in original_values.items():
        if original_value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = original_value


@pytest.fixture
def remote_env_vars(
    minio_service: MinioService,
) -> Generator[dict[str, str]]:
    """Set up environment variables for remote MinIO.

    Uses the same MinIO instance as local for simplicity.
    In real usage, these would point to different servers.
    """
    scheme = "https" if minio_service.secure else "http"
    endpoint = f"{scheme}://{minio_service.endpoint}"

    env_vars = {
        "S3_OVERLAY_REMOTE_ENDPOINT": endpoint,
        "S3_OVERLAY_REMOTE_ACCESS_KEY_ID": minio_service.access_key,
        "S3_OVERLAY_REMOTE_SECRET_ACCESS_KEY": minio_service.secret_key,
        "S3_OVERLAY_REMOTE_REGION": "us-east-1",
    }

    # Set environment variables
    original_values = {}
    for key, value in env_vars.items():
        original_values[key] = os.environ.get(key)
        os.environ[key] = value

    yield env_vars

    # Restore original values
    for key, original_value in original_values.items():
        if original_value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = original_value


@pytest.fixture
def overlay_env(
    local_env_vars: dict[str, str],
    remote_env_vars: dict[str, str],
) -> Generator[dict[str, str]]:
    """Combined environment variables for overlay proxy testing."""
    key = "S3_OVERLAY_CACHE_ENABLED"
    original = os.environ.get(key)
    os.environ[key] = "true"
    yield {**local_env_vars, **remote_env_vars, key: "true"}
    if original is None:
        os.environ.pop(key, None)
    else:
        os.environ[key] = original


@pytest.fixture
def s3_helpers():
    return {
        "bucket_exists": _bucket_exists,
        "ensure_bucket": _ensure_bucket,
    }
