# algovoi-keystone-azure-servicebus

Bind Azure Service Bus sends to the AlgoVoi keystone. Wrap any `azure-servicebus`
`ServiceBusSender` and every `send_messages` / `schedule_messages` is bound to the `decision_ref`
that authorised it, emitting a content-addressed `execution_ref` any party can recompute offline:

```
execution_ref = "sha256:" + SHA-256(JCS({ decision_ref, action_type, scope, outcome, executed_at_ms }))
```

`action_type` is `servicebus:send`; `scope` is the entity (queue or topic name). Receivers and
management calls pass through unbound. No `azure-servicebus` dependency; the wrapper is a drop-in.
The Azure cloud counterpart to `algovoi-keystone-kafka`, `algovoi-keystone-amqp` and
`algovoi-keystone-nats`.

```python
from algovoi_keystone_azure_servicebus import keystone_sender

sender = keystone_sender(sb_sender, decision_ref=decision_ref, entity="orders")
sender.send_messages(message)   # bound
ref = sender.execution_ref
```

Records emitted on `sender.executions` drop straight into `algovoi-keystone-validate`.

Keystone-only edition, Apache-2.0. Built on the AlgoVoi substrate (RFC 8785 JCS + SHA-256).
