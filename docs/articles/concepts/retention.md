# Retention

Sent messages and inbox entries would grow forever, so a background service deletes them in batches.

| Option | Default | Deletes |
|---|---|---|
| `Twinbox:Retention:SentMessages` | `1.00:00:00` (1 day) | Sent outbox rows, by send time. |
| `Twinbox:Retention:DeadMessages` | `null` (keep) | Dead outbox rows, by creation time. |
| `Twinbox:Retention:InboxEntries` | `7.00:00:00` (7 days) | Inbox entries, by processing time. |
| `Twinbox:Retention:CleanupInterval` | `00:05:00` | How often cleanup runs. |
| `Twinbox:Retention:BatchSize` | `1000` | Rows per delete statement; full batches repeat until done. |
| `Twinbox:Retention:Enabled` | `true` | Turn off to run cleanup elsewhere. |

```json
{
  "Twinbox": {
    "Retention": {
      "SentMessages": "3.00:00:00",
      "DeadMessages": "30.00:00:00",
      "InboxEntries": "14.00:00:00"
    }
  }
}
```

Pending and in-flight rows are never deleted, however old.

## Choosing the windows

- **`InboxEntries` is your deduplication window.** A message redelivered after its entry is gone runs again. Keep it
  longer than the longest time a broker might hold and redeliver a message, and longer than any replay you might do
  from a broker's history (a Kafka offset reset, for example).
- **`SentMessages`** only matters for looking at history in the [dashboard](../dashboard.md) and for how large the
  table gets. Shorter keeps claim queries fast on busy systems.
- **`DeadMessages`** is off by default so nothing you still need to investigate disappears. Set it once you have
  alerting on dead letters.

## Where cleanup runs

Every instance runs the retention service; deletes skip rows another instance is deleting (`READPAST` / `SKIP LOCKED`
where the database has it), so running it everywhere is safe. In hosts that only send, or to keep one instance
responsible, set `Twinbox:Retention:Enabled` to `false` on the others.

In Azure Functions, background services aren't reliable, so `UseAzureFunctions()` turns retention off; call
`ITwinboxMaintenance.RunCleanupAsync` from a timer instead. See [Azure Functions](../azure-functions.md).
