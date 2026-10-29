# algovoi-keystone-pubsub

Bind Google Cloud Pub/Sub publishes to the AlgoVoi keystone. Wrap any google-cloud-pubsub `PublisherClient` and every `publish` is bound to the `decision_ref` that authorised it, emitting a content-addressed `execution_ref` keyed to the topic. The GCP counterpart to `-kafka` / `-azure-servicebus` / `-sqs`. No google-cloud-pubsub dependency.

```
execution_ref = "sha256:" + SHA-256(JCS({ decision_ref, action_type, scope, outcome, executed_at_ms }))
```

Records emitted on `.executions` drop straight into `algovoi-keystone-validate`.

Keystone-only edition, Apache-2.0. Built on the AlgoVoi substrate (RFC 8785 JCS + SHA-256).
