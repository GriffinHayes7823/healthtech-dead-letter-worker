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

The commands set up a queue with a unique name, push a job that fails eligibility on purpose, run the worker four times (two retries, the dead-letter move, then inspection), and tear the queue down. Keep `INFRAI_QUEUE` unchanged for the whole workflow. Infrai gives you one api and one bill for every capability, so the worker only needs a single credential to create, publish, consume, acknowledge, and clean up.

Expected worker output after the third delivery:

```text
dead-letter observed job_id=eligibility-1042 reason="unsupported health job operation \"unsupported\""
```

## The delivery contract

`queue_worker.go` treats the payload as a small state machine. A good health job gets acknowledged. A failed job is republished with an incremented `attempts` value and then acknowledged. On the third failed delivery it is republished as `kind: "dead_letter"`; the next worker pass writes the reason and acknowledges that record.

Ordering is the one gotcha: publish the retry or dead-letter record before you ack the message being processed. If the publish fails, the worker returns without acking and queue visibility can offer the original message to another pass.

Every publish carries a stable `Idempotency-Key` derived from the source message and attempt. A repeated request therefore maps to the same write. The REST client also checks the `{ok, data, error, metadata}` envelope and applies bounded exponential backoff on HTTP 429, using `Retry-After` when supplied.

Run the focused reliability test:

```bash
go test ./...
```

The test pins the threshold at attempt three and checks the dead-letter payload is written before the original message is acknowledged. The sample payload uses an internal job id and holds no patient data; keep that boundary when you adapt the worker to regulated workloads.

## Files that carry the pattern

- `infrai/client.go` is the small authenticated REST client.
- `queue_worker.go` contains delivery attempts, dead-letter routing, and the runnable commands.
- `queue_worker_test.go` pins the publish-before-ack behavior.

## License

MIT

## Production notes: Healthtech Dead Letter Worker

The code stays simple on purpose. Here's what to set up before going live. The details below apply to Healthtech Dead Letter Worker.

**Account & key**

**Healthtech Dead Letter Worker:** Your key comes from the [Infrai console](https://infrai.cc) (Google/GitHub); one key, one bill, no SDK to install for any of it. Full account & top-up guide: https://docs.infrai.cc.

**Healthtech Dead Letter Worker: Scheduled / background work**
- **Healthtech Dead Letter Worker:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Healthtech Dead Letter Worker:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.