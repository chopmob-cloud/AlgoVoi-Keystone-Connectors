# algovoi-keystone-archive

Bind post-quantum archive writes to the AlgoVoi keystone. Open, Apache-2.0.

The keystone journal binds a commerce **decision** to an **execution**. This connector carries
that binding onward into durable, post-quantum-signed **storage** — joining the agentic-commerce
level to the secure-file-storing level so the whole chain is verifiable offline.

Wrap any async archive client exposing `notarize` (the reference implementation is
[`algovoi-doc-archive`](https://docs.algovoi.co.uk/verifiable-archive)'s `Archive`: ML-KEM-1024 +
AES-256-GCM at rest, Falcon-1024-signed hash-linked chain). Every sealed document produces two
content-addressed refs:

```python
from algovoi_keystone_archive import keystone_archive_client, verify_binding
from algovoi_keystone_runtime import Journal, Runtime

# decision_ref comes from the upstream keystone chain (passport -> mandate -> policy -> decision)
client = keystone_archive_client(archive, decision_ref=decision_ref)
result = await client.notarize(data=receipt_bytes, title="settlement receipt")

rec = client.executions[-1]
rec["execution_ref"]   # sha256:JCS({decision_ref, action_type, scope, outcome, executed_at_ms})
rec["binding_ref"]     # sha256:JCS({action_type:"archive:bind", execution_ref, doc_hash, entry_id, seq})

# anyone can recompute the storage binding offline:
assert verify_binding(rec["binding_ref"], execution_ref=rec["execution_ref"],
                      doc_hash=result["doc_hash"], entry_id=result["entry_id"], seq=result["seq"])
```

- `action_type` is `archive:notarize`; `scope` is the content address (`sha256:<hex>`) of the
  notarized bytes, so the execution is bound to the exact document.
- `binding_ref` ties that execution to the archive entry (`doc_hash`, `entry_id`, `seq`).
- Reads (`retrieve`, `verify_chain`) pass through unbound.
- A notarize that raises is recorded `FAILED` with no storage binding, and the error re-raised.
- No archive dependency — the client is duck-typed; `algovoi-doc-archive` is an optional extra.

Both refs recompute with a stock RFC 8785 (JCS) implementation and SHA-256. `tamper_detected`
and `verify_binding` recompute and compare, so a mutated scope, outcome, timestamp, doc_hash,
entry_id or seq is caught.

## Where this fits: AV-RAILS -> Keystone -> AV-VCS

This connector is the storage binding between AlgoVoi's two operator suites:

- **[AV-RAILS](https://algovoi.co.uk/secure-agent-payments.html)** - the agentic-commerce
  payment rails. A payment or settlement decision is committed upstream as a keystone
  `decision_ref` (passport -> mandate -> policy -> decision); see
  [Agentic Payment Receipts](https://docs.algovoi.co.uk/agentic-payment-receipts) and the
  [e-commerce adapters](https://docs.algovoi.co.uk/integrations/ecommerce).
- **AV-VCS - the [Verifiable Compliance Suite](https://docs.algovoi.co.uk/verifiable-compliance-suite)**
  ([public repo](https://github.com/chopmob-cloud/algovoi-vcs)) - the downstream compliance
  evidence platform. It verifies and reports on the sealed records (Archive Auditor, Verifiable
  Audit Log, Records Vault, Compliance Command Center) built on the
  [Verifiable Archive](https://docs.algovoi.co.uk/verifiable-archive) engine this connector wraps.

```text
AV-RAILS decision_ref --> keystone-archive notarize --> sealed Falcon-1024 entry
                                                           |
                                           AV-VCS verifies -+  (offline, no vendor in the loop)
```

The `execution_ref` carries the AV-RAILS decision into storage; the `binding_ref` ties it to the
sealed entry that AV-VCS audits. Deployment guides:
[AV-RAILS payment rails](https://docs.algovoi.co.uk/payment-rails-deployment-guide) and
[AV-VCS](https://docs.algovoi.co.uk/vcs-deployment-guide).

## Install

```bash
pip install algovoi-keystone-archive
```

## Licence

Apache-2.0. See `LICENSE` and `NOTICE`.
