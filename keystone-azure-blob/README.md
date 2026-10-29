# algovoi-keystone-azure-blob

Bind Azure Blob Storage writes to the AlgoVoi keystone. Wrap any `azure-storage-blob`
`ContainerClient` and every `upload_blob` / `delete_blob` is bound to the `decision_ref` that
authorised it, emitting a content-addressed `execution_ref` any party can recompute offline:

```
execution_ref = "sha256:" + SHA-256(JCS({ decision_ref, action_type, scope, outcome, executed_at_ms }))
```

`action_type` is `azureblob:upload` / `azureblob:delete`; `scope` is `container/blob`. Reads
(`download_blob`, `list_blobs`, ...) pass through unbound. No `azure-storage-blob` dependency; the
wrapper is a drop-in. The Azure counterpart to `algovoi-keystone-s3` and `algovoi-keystone-gcs`.

```python
from algovoi_keystone_azure_blob import keystone_container_client

client = keystone_container_client(container_client, decision_ref=decision_ref)
client.upload_blob(name="2026/r1.json", data=b"{}")   # bound
ref = client.execution_ref
```

Records emitted on `client.executions` drop straight into `algovoi-keystone-validate`.

Keystone-only edition, Apache-2.0. Built on the AlgoVoi substrate (RFC 8785 JCS + SHA-256).
