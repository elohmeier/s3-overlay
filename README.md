# S3 Overlay Proxy

[![CD](https://github.com/elohmeier/s3-overlay/actions/workflows/cd.yaml/badge.svg)](https://github.com/elohmeier/s3-overlay/actions/workflows/cd.yaml)
[![PyPI](https://img.shields.io/pypi/v/s3-overlay)](https://pypi.org/project/s3-overlay/)
[![GHCR](https://img.shields.io/badge/ghcr.io-s3--overlay-blue)](https://ghcr.io/elohmeier/s3-overlay)

The S3 overlay proxy is a standalone package that provides transparent caching of S3 objects from a remote bucket into a local MinIO instance. Built with Litestar, it sits in front of the local MinIO process and mirrors objects on demand from a remote S3 bucket.

All local reads and writes still target MinIO; a cache miss transparently downloads the object from the upstream bucket, stores it in MinIO, and returns the payload to the caller.

## Features

- **Transparent caching**: GET/HEAD requests automatically fetch missing objects from remote S3
- **Local-first**: All writes go directly to local MinIO
- **Auto-bucket creation**: Buckets are created automatically when objects are mirrored
- **Range request support**: Partial content requests are properly proxied
- **Partial Caching**: Large files are cached in chunks to avoid downloading the entire object when only a range is requested
- **Zero-config local mode**: Works without remote configuration for local-only development

## Installation

```bash
uv add s3-overlay
```

Or add to your `pyproject.toml`:

```toml
dependencies = [
  "s3-overlay",
]
```

## Docker

```bash
docker run -p 8000:8000 ghcr.io/elohmeier/s3-overlay:latest
```

Pass configuration via environment variables (`-e`), for example:

```bash
docker run -p 8000:8000 \
  -e S3_OVERLAY_REMOTE_ENDPOINT=https://s3.eu-central-1.amazonaws.com \
  -e S3_OVERLAY_REMOTE_REGION=eu-central-1 \
  -e S3_OVERLAY_REMOTE_ACCESS_KEY_ID=AKIA... \
  -e S3_OVERLAY_REMOTE_SECRET_ACCESS_KEY=secret... \
  -e S3_OVERLAY_BUCKET_MAPPING=local-bucket:remote-bucket \
  ghcr.io/elohmeier/s3-overlay:latest
```

## Usage

### Running the Proxy

The proxy is served with Granian in factory mode:

```bash
uv run litestar --app s3_overlay.app:create_app run --host 0.0.0.0
```

### How It Works

- **Writes** (`PUT`, multipart uploads, deletes) are handled only by MinIO
- **Reads** (`GET`, `HEAD`) hit MinIO first and fall back to the remote source if the object is missing locally
- When caching is enabled (`S3_OVERLAY_CACHE_ENABLED=true`), objects fetched from the remote source are written into MinIO so subsequent requests stay local
- When caching is disabled (default), objects are streamed directly from the remote source without storing them locally

## Configuration

Configure the proxy using environment variables:

### Local MinIO (Required)

| Variable                             | Description                            | Default                 |
| ------------------------------------ | -------------------------------------- | ----------------------- |
| `S3_OVERLAY_LOCAL_ENDPOINT`          | Local MinIO endpoint URL               | `http://127.0.0.1:9000` |
| `S3_OVERLAY_LOCAL_ACCESS_KEY`        | Local MinIO access key                 | `minioadmin`            |
| `S3_OVERLAY_LOCAL_SECRET_KEY`        | Local MinIO secret key                 | `minioadmin`            |
| `S3_OVERLAY_LOCAL_REGION`            | AWS region for local MinIO             | `us-east-1`             |
| `S3_OVERLAY_DEFAULT_BUCKET_LOCATION` | Default bucket location constraint     | `us-east-1`             |
| `S3_OVERLAY_CHUNK_THRESHOLD`         | File size threshold for chunking       | `52428800` (50MB)       |
| `S3_OVERLAY_CHUNK_SIZE`              | Chunk size for partial caching         | `16777216` (16MB)       |
| `S3_OVERLAY_CACHE_BUCKET`            | Bucket name for storing chunks         | `s3-overlay-cache`      |
| `S3_OVERLAY_CACHE_ENABLED`           | Enable local caching of remote objects | `false`                 |

### Remote S3 (Optional)

Set the following variables to enable remote backfilling. Leave unset to operate in local-only mode.

| Variable                              | Description                                                                             |
| ------------------------------------- | --------------------------------------------------------------------------------------- |
| `S3_OVERLAY_REMOTE_ENDPOINT`          | Optional custom URL for upstream S3 API (defaults to AWS)                               |
| `S3_OVERLAY_REMOTE_REGION`            | AWS region for the remote bucket (e.g. `eu-central-1`)                                  |
| `S3_OVERLAY_REMOTE_ADDRESSING_STYLE`  | S3 addressing style: `virtual` or `path` (default: `virtual`)                           |
| `S3_OVERLAY_REMOTE_ACCESS_KEY_ID`     | Credentials with read access to the remote bucket                                       |
| `S3_OVERLAY_REMOTE_SECRET_ACCESS_KEY` | Secret key for remote bucket                                                            |
| `S3_OVERLAY_REMOTE_SESSION_TOKEN`     | Optional session token for temporary credentials                                        |
| `S3_OVERLAY_BUCKET_MAPPING`           | Map local bucket names to remote bucket names (format: `local1:remote1,local2:remote2`) |

**Example with AWS S3:**

```yaml
environment:
  - S3_OVERLAY_REMOTE_ENDPOINT=https://s3.eu-central-1.amazonaws.com
  - S3_OVERLAY_REMOTE_REGION=eu-central-1
  - S3_OVERLAY_REMOTE_ADDRESSING_STYLE=path
  - S3_OVERLAY_REMOTE_ACCESS_KEY_ID=AKIA...
  - S3_OVERLAY_REMOTE_SECRET_ACCESS_KEY=secret...
  - S3_OVERLAY_BUCKET_MAPPING=local-bucket:remote-bucket
```

**Example with Hetzner Cloud Object Storage:**

```yaml
environment:
  - S3_OVERLAY_REMOTE_ENDPOINT=https://fsn1.your-objectstorage.com
  - S3_OVERLAY_REMOTE_REGION=us-east-1
  - S3_OVERLAY_REMOTE_ADDRESSING_STYLE=virtual # Uses bucket.endpoint.com format
  - S3_OVERLAY_REMOTE_ACCESS_KEY_ID=YOUR_ACCESS_KEY
  - S3_OVERLAY_REMOTE_SECRET_ACCESS_KEY=YOUR_SECRET_KEY
  - S3_OVERLAY_BUCKET_MAPPING=local-bucket:remote-bucket
```

With virtual host style, the proxy will access remote objects at:
`https://remote-bucket.fsn1.your-objectstorage.com/organizations/...`

Buckets are created automatically in MinIO when the proxy mirrors an object. Override `S3_OVERLAY_DEFAULT_BUCKET_LOCATION` if the MinIO cluster expects a non-`us-east-1` location constraint.

## Development

### Running Tests

Tests use pytest-databases to spin up real MinIO containers:

```bash
uv run pytest tests/ -v
```

Tests verify:

- Object caching from remote to local
- HEAD request handling
- Local cache hits without remote access
- 404 handling for missing objects
- Bucket auto-creation

### Architecture

The package is structured as follows:

```text
s3-overlay/
├── s3_overlay/
│   ├── __init__.py      # Public API exports
│   ├── proxy.py         # S3OverlayProxy, LocalSettings, RemoteSettings
│   └── app.py           # Litestar ASGI application factory
├── tests/
│   ├── conftest.py      # Pytest fixtures for MinIO
│   ├── test_proxy.py    # Unit tests
│   └── test_integration.py  # Integration tests with test.txt
├── pyproject.toml
└── README.md
```

### Key Components

- **S3OverlayProxy**: Core proxy logic with boto3 clients for local and remote S3
- **LocalSettings/RemoteSettings**: Configuration models loaded from environment
- **create_app()**: Litestar application factory for ASGI servers

## License

MIT. See `LICENSE`.
