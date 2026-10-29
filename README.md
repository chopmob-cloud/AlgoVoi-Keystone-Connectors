# AlgoVoi Keystone Connectors

One repository for the `algovoi-keystone-*` connector family: thin, compiled
adapters that bind a third-party SDK's outbound call to an AlgoVoi keystone
decision, so every send carries a content-addressed `execution_ref` that can be
recomputed offline.

Each connector wraps one SDK, binds every send to the `decision_ref` that
authorised it, and emits

```
execution_ref = "sha256:" + SHA-256(JCS({ decision_ref, action_type, scope, outcome, executed_at_ms }))
```

using RFC 8785 (JCS) canonicalisation and SHA-256. Records drop straight into
`algovoi-keystone-validate`. Receives pass through unbound.

## Packages

| Package | Binds | Wraps (no runtime dependency) |
|---|---|---|
| `algovoi-keystone-sqs` | AWS SQS `send_message`, `send_message_batch` | boto3 |
| `algovoi-keystone-sns` | AWS SNS publish | boto3 |
| `algovoi-keystone-dynamodb` | DynamoDB writes | boto3 |
| `algovoi-keystone-azure-blob` | Azure Blob uploads | azure-storage-blob |
| `algovoi-keystone-azure-servicebus` | Azure Service Bus sends | azure-servicebus |
| `algovoi-keystone-pubsub` | Google Pub/Sub publish | google-cloud-pubsub |
| `algovoi-keystone-celery` | Celery task dispatch | celery |
| `algovoi-keystone-httpx` | httpx request sends | httpx |
| `algovoi-keystone-pinecone` | Pinecone upserts | pinecone-client |
| `algovoi-keystone-qdrant` | Qdrant upserts | qdrant-client |

The only runtime dependency of every connector is `algovoi-execution-ref`; the
wrapped SDK is passed in by the caller and is never imported.

Every package in this repository is the **keystone-only edition** (Apache-2.0):
it binds and emits evidence, and makes no policy decisions of its own.

## Install

```bash
pip install algovoi-keystone-sqs
```

Wheels are published for CPython 3.10–3.13 on `manylinux` (x86_64 and aarch64)
and Windows amd64, plus an sdist for everything else.

## Usage

```python
import boto3
from algovoi_keystone_sqs import bind

client = bind(boto3.client("sqs"), keystone=keystone, queue="orders")
client.send_message(QueueUrl=url, MessageBody="...")   # bound; returns execution_ref
```

The exact signature differs per connector — see each package's `README.md`.

## Building from source

```bash
python -m pip install build cibuildwheel
cibuildwheel --output-dir wheelhouse keystone-sqs
python -m build --sdist keystone-sqs
```

Or push a `v*` tag and let `.github/workflows/release.yml` build the full matrix
and publish. That workflow is the only supported publish path: hand-built
single-platform uploads are what left the 0.1.0 releases with no sdist at all.

## Licence

Apache-2.0. See `LICENSE` and each package's `NOTICE`.
