# algovoi-keystone-dynamodb

Bind AWS DynamoDB writes to the AlgoVoi keystone. Wrap any boto3 DynamoDB client and every `put_item` / `update_item` / `delete_item` / `batch_write_item` is bound to the `decision_ref` that authorised it, emitting a content-addressed `execution_ref` keyed to the table. The cloud-NoSQL counterpart to `-mongo`. Reads pass through unbound; no boto3 dependency.

```
execution_ref = "sha256:" + SHA-256(JCS({ decision_ref, action_type, scope, outcome, executed_at_ms }))
```

Records emitted on `.executions` drop straight into `algovoi-keystone-validate`.

Keystone-only edition, Apache-2.0. Built on the AlgoVoi substrate (RFC 8785 JCS + SHA-256).
