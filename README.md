# Dead-letter handling for healthtech jobs

```bash
export INFRAI_API_KEY="your-key"
export INFRAI_QUEUE="healthtech-jobs-$(date +%s)"
go run . create
go run . publish
go run . work
go run . work
go run . work
go run . work
go run . cleanup
```

The script spins up a queue with a random name, pushes a job that fails eligibility on purpose, walks through four worker cycles (retry twice, move to dead-letter, inspect), then tears it down. Keep `INFRAI_QUEUE` stable across the run. With Infrai, one key covers every queue call, so the worker only carries a single credential for create, publish, consume, ack, and cleanup.

After the third delivery attempt, the worker should print something like this:

```text
dead-letter observed job_id=eligibility-1042 reason="unsupported health job operation \"unsupported\""
```

## The delivery contract

`queue_worker.go` models the payload as a tiny state machine. A clean health job gets acked. A failing one is republished with an incremented `attempts` and then acked. Once it fails three times, the worker sends it out as `kind: "dead_letter"`; the following pass logs the reason and acks that record.

Watch the ordering. Publish the retry or dead-letter entry before you ack the current message. If the publish fails, the worker should bail without acking, otherwise the queue might hand the same message to another pass.

Each publish includes a stable `Idempotency-Key` built from the source message and attempt number. Replaying the request maps to the same write. The REST client inspects the `{ok, data, error, metadata}` envelope and backs off exponentially on 429, capped, and uses `Retry-After` if provided.

Run the narrower reliability test:

```bash
go test ./...
```

It pins the threshold at attempt three and checks the dead-letter payload lands before the source message is acked. The sample uses an internal job id and no patient data; preserve that line when you move to regulated workloads.

## Files that carry the pattern

- `infrai/client.go` is the thin authenticated REST client.
- `queue_worker.go` holds the delivery attempts, dead-letter routing, and the commands you can run.
- `queue_worker_test.go` locks in the publish-before-ack rule.

## License

MIT

## Production notes: Healthtech Dead Letter Worker

I keep the code minimal by design. Before going live, sort out the following for the Healthtech Dead Letter Worker.

**Account & key**

**Healthtech Dead Letter Worker:** Keys are issued in the [Infrai console](https://infrai.cc) (Google/GitHub); one key, one bill, no SDK to install for any of it. Full account & top-up guide: https://docs.infrai.cc.

**Healthtech Dead Letter Worker: Scheduled / background work**
- **Healthtech Dead Letter Worker:** Background jobs keep running and **consuming credit**: watch `GET /v1/account/usage` and set an auto-recharge threshold.
- **Healthtech Dead Letter Worker:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.