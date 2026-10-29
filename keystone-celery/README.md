# algovoi-keystone-celery

Bind Celery task dispatch to the AlgoVoi keystone. When a keystone decision authorises
asynchronous work, the dispatch of the task is bound to the `decision_ref` that authorised it,
emitting a content-addressed `execution_ref` any party can recompute offline:

```
execution_ref = "sha256:" + SHA-256(JCS({ decision_ref, action_type, scope, outcome, executed_at_ms }))
```

`action_type` is `celery:dispatch`; `scope` is the task name. Wrap a single task
(`keystone_task` binds `apply_async` / `delay`) or an app (`keystone_celery_app` binds
`send_task`). A dispatch that raises is recorded FAILED; otherwise COMMITTED. No Celery dependency;
the wrappers are drop-ins.

```python
from algovoi_keystone_celery import keystone_task

bound = keystone_task(charge_task, decision_ref=decision_ref)
bound.apply_async(args=[order_id])   # bound
ref = bound.execution_ref
```

Records emitted on `.executions` drop straight into `algovoi-keystone-validate`.

Keystone-only edition, Apache-2.0. Built on the AlgoVoi substrate (RFC 8785 JCS + SHA-256).
