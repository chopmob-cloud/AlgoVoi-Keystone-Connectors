# algovoi-keystone-qdrant

Bind Qdrant vector writes to the AlgoVoi keystone. Agent memory is a data plane: wrap any `QdrantClient` and every `upsert` / `delete` / `set_payload` / `overwrite_payload` is bound to the `decision_ref` that authorised it, emitting a content-addressed `execution_ref` keyed to the collection. Reads (search, retrieve, scroll, query) pass through unbound; no qdrant-client dependency.

```
execution_ref = "sha256:" + SHA-256(JCS({ decision_ref, action_type, scope, outcome, executed_at_ms }))
```

Records emitted on `.executions` drop straight into `algovoi-keystone-validate`.

Keystone-only edition, Apache-2.0. Built on the AlgoVoi substrate (RFC 8785 JCS + SHA-256).
