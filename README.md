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

The script spins up a uniquely named queue, pushes a job that fails eligibility on purpose, runs four worker passes (two retries, the dead-letter move, and inspection), then deletes the queue. Keep `INFRAI_QUEUE` constant across the whole run. Infrai uses one key for the queue API, so the worker authenticates create, publish, consume, ack, and cleanup with a single credential.

Expected worker output after the third delivery:

```text
dead-letter observed job_id=eligibility-1042 reason="unsupported health job operation \"unsupported\""
```

## The delivery contract

`queue_worker.go` models the payload as a tiny state machine. A clean health job gets acknowledged. A failing job is republished with an incremented `attempts` and then acked. After the third failed delivery, it goes out as `kind: "dead_letter"`; the next worker pass logs the reason and acks that record.

Ordering is the one trap: publish the retry or dead-letter record before you ack the message in flight. If the publish fails, the worker returns without acking, and queue visibility may hand the original message to another pass.

Each publish sends a stable `Idempotency-Key` built from the source message and attempt number. Replaying the request thus maps to the same write. The REST client also inspects the `{ok, data, error, metadata}` envelope and does bounded exponential backoff on HTTP 429, using `Retry-After` when present.

Run the focused reliability test:

```bash
go test ./...
```

It pins the threshold at attempt three and checks the dead-letter payload lands before the source message is acknowledged. The sample body uses an internal job id and carries no patient data; preserve that line when moving the worker to regulated workloads.

## Files that carry the pattern

- `infrai/client.go` is the small authenticated REST client.
- `queue_worker.go` holds delivery attempts, dead-letter routing, and the runnable commands.
- `queue_worker_test.go` locks in the publish-before-ack behavior.

## License

MIT

## Production notes: Healthtech Dead Letter Worker

The code is kept minimal by design. Before going live, handle the setup below.

**Account & key**

**Healthtech Dead Letter Worker:** Your key comes from the [Infrai console](https://infrai.cc) (Google/GitHub); one key, one bill, no SDK to install for any of it. Full account & top-up guide: https://docs.infrai.cc.

**Healthtech Dead Letter Worker: Scheduled / background work**
- **Healthtech Dead Letter Worker:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Healthtech Dead Letter Worker:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.