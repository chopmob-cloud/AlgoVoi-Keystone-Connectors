# algovoi-keystone-httpx

Bind **outbound** HTTP calls to the AlgoVoi keystone. The `asgi` connector binds the requests a
service *receives*; this one binds the calls a service *makes*. Wrap any sync `httpx.Client` or
`requests.Session` and every state-changing call (POST / PUT / PATCH / DELETE) is bound to the
`decision_ref` that authorised it, emitting a content-addressed `execution_ref` any party can
recompute offline:

```
execution_ref = "sha256:" + SHA-256(JCS({ decision_ref, action_type, scope, outcome, executed_at_ms }))
```

`action_type` is `http:<method>`; `scope` is the destination `host/path` (query string dropped for
stability). Outcome comes from the response status: 2xx/3xx COMMITTED, 4xx SKIPPED, 5xx/exception
FAILED. Reads (GET / HEAD / OPTIONS) pass through unbound. No httpx or requests dependency.

```python
from algovoi_keystone_httpx import keystone_http_client

client = keystone_http_client(httpx.Client(), decision_ref=decision_ref)
client.post("https://api.partner.com/v1/pay", json=payload)   # bound
ref = client.execution_ref
```

Records emitted on `client.executions` drop straight into `algovoi-keystone-validate`.

Keystone-only edition, Apache-2.0. Built on the AlgoVoi substrate (RFC 8785 JCS + SHA-256).
