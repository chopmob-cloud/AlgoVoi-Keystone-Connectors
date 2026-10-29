# algovoi-keystone-sns

Bind AWS SNS publishes to the AlgoVoi keystone. Wrap any boto3 SNS client and every `publish` is bound to the `decision_ref` that authorised it, emitting a content-addressed `execution_ref` keyed to the topic. Reads pass through unbound; no boto3 dependency.

```
execution_ref = "sha256:" + SHA-256(JCS({ decision_ref, action_type, scope, outcome, executed_at_ms }))
```

Records emitted on `.executions` drop straight into `algovoi-keystone-validate`.

Keystone-only edition, Apache-2.0. Built on the AlgoVoi substrate (RFC 8785 JCS + SHA-256).
