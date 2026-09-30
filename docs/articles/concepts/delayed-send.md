# Delayed send

Set `SendOptions.Delay` to hold a message in the outbox until later:

```csharp
outbox.Send(new PaymentReminder(invoice.Id), new SendOptions { Delay = TimeSpan.FromDays(3) });
```

The row is saved right away with `AvailableAt = now + Delay`, and the dispatcher ignores it until then. The delay is
kept by the outbox, not the broker, so it works with every transport, including [local delivery](../transports/local.md)
and [HTTP](../transports/http.md).

Things to know:

- **Precision.** A due row is picked up on the next dispatcher pass. When the outbox is idle, passes back off to
  `Dispatcher:MaxPollInterval` (30 seconds by default), so a delayed message can go out up to that much late.
- **Durability.** The message is in your database from the start. A restart or deployment doesn't lose it.
- **Cancellation.** There is no cancel API. Delete the row through `IOutboxAdmin` or the [dashboard](../dashboard.md),
  or have the handler check whether the message still applies when it arrives.
- **Ordering.** A delayed message with a partition key holds back later messages with the same key until it is sent.
  See [Ordering](ordering.md).
- **Retention.** Pending rows are never purged, however far in the future they are due.
- **Health checks.** The pending-age check measures from the time a row became due, so a long delay never looks like
  a stuck backlog.
