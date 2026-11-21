# algovoi-keystone-auditor

Bind archive audit reports to the AlgoVoi keystone. Open, Apache-2.0.

The keystone journal binds a commerce **decision** to an **execution**.
[`algovoi-keystone-archive`](../keystone-archive) carries that binding into sealed, post-quantum
**storage**; this connector carries it onward into **reporting** — joining the secure-file-storing
level to the compliance-reporting level so the whole chain is verifiable offline.

Wrap the read-only Archive Auditor
([`algovoi-archive-auditor`](https://docs.algovoi.co.uk/verifiable-compliance-suite)'s `Auditor`,
which reads the Falcon-1024 hash-linked chain built by
[`algovoi-doc-archive`](https://docs.algovoi.co.uk/verifiable-archive)). Every signed audit report,
integrity attestation and evidence pack produces two content-addressed refs:

```python
from algovoi_keystone_auditor import keystone_auditor_client, verify_binding
from algovoi_archive_auditor import Auditor

auditor = Auditor(archive_public_key=pub, storage=backend,
                  report_secret_key=rsk, report_public_key=rpk)

# decision_ref comes from the upstream keystone chain (passport -> mandate -> policy -> decision)
client = keystone_auditor_client(auditor, decision_ref=decision_ref)
result = await client.audit_report()

rec = client.executions[-1]
rec["execution_ref"]   # sha256:JCS({decision_ref, action_type, scope, outcome, executed_at_ms})
rec["binding_ref"]     # sha256:JCS({action_type:"auditor:bind", execution_ref, archive_kid,
                       #             head_receipt_hash, entries, chain_valid})

# anyone can recompute the reporting binding offline:
assert verify_binding(rec["binding_ref"], execution_ref=rec["execution_ref"],
                      archive_kid=rec["archive_kid"], head_receipt_hash=rec["head_receipt_hash"],
                      entries=rec["entries"], chain_valid=rec["chain_valid"])
```

- `action_type` is `auditor:report` / `auditor:attest` / `auditor:evidence_pack`; `scope` is the
  content address (`sha256:<hex>`) of the signed report payload (the inner `report` / `attestation`
  / `manifest` object, RFC 8785 canonicalised), so the execution is bound to the exact report.
- `binding_ref` ties that report to the sealed archive state it covers (`archive_kid`,
  `head_receipt_hash`, `entries`, `chain_valid`).
- The adapter refuses to bind unless the report attests a valid `head_receipt_hash` and
  `chain_valid` is true: a report over a broken or unidentifiable chain is recorded `FAILED` with
  no binding and the error re-raised.
- Reads (`verify`, `inventory`, `retention_report`, `siem_events`, `metrics_inputs`) pass through
  unbound. A call that raises is recorded `FAILED` with no binding, and the error re-raised.
- No auditor dependency — the client is duck-typed; `algovoi-archive-auditor` is an optional extra.

Both refs recompute with a stock RFC 8785 (JCS) implementation and SHA-256. `tamper_detected` and
`verify_binding` recompute and compare, so a mutated scope, outcome, timestamp, archive_kid,
head_receipt_hash, entries or chain_valid is caught. The refs are content addresses, not signatures:
authenticity of the report comes from the auditor's own Falcon-1024 envelope, which the binding
points at.

## Where this fits: AV-RAILS -> Keystone -> AV-VCS

This connector is the reporting binding between AlgoVoi's two operator suites, downstream of
[`algovoi-keystone-archive`](../keystone-archive) (the storage binding):

- **[AV-RAILS](https://algovoi.co.uk/secure-agent-payments.html)** - the agentic-commerce
  payment rails. A payment or settlement decision is committed upstream as a keystone
  `decision_ref` (passport -> mandate -> policy -> decision); see
  [Agentic Payment Receipts](https://docs.algovoi.co.uk/agentic-payment-receipts) and the
  [e-commerce adapters](https://docs.algovoi.co.uk/integrations/ecommerce).
- **AV-VCS - the [Verifiable Compliance Suite](https://docs.algovoi.co.uk/verifiable-compliance-suite)**
  ([public repo](https://github.com/chopmob-cloud/algovoi-vcs)) - the downstream compliance
  evidence platform. Its Archive Auditor verifies and reports on the sealed records built on the
  [Verifiable Archive](https://docs.algovoi.co.uk/verifiable-archive) engine; this connector binds
  those reports back to the authorising decision and the archive head they cover.

```text
AV-RAILS decision_ref --> keystone-archive notarize --> sealed Falcon-1024 entry
                                                           |
                                  keystone-auditor report -+--> archive_kid + head_receipt_hash
                                                           |
                                           AV-VCS reports -+  (offline, no vendor in the loop)
```

The `execution_ref` carries the AV-RAILS decision into the report; the `binding_ref` ties the report
to the sealed archive head that AV-VCS audits. Deployment guides:
[AV-RAILS payment rails](https://docs.algovoi.co.uk/payment-rails-deployment-guide) and
[AV-VCS](https://docs.algovoi.co.uk/vcs-deployment-guide).

## Install

```bash
pip install algovoi-keystone-auditor
```

## Licence

Apache-2.0. See `LICENSE` and `NOTICE`.
