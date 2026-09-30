# Migrating from another outbox

This guide moves a system from an existing outbox (hand-rolled tables, or your current messaging library) to Twinbox
**one service at a time**, with no big-bang cutover. It relies on three features:

- **Header profiles** keep the wire format compatible in both directions, so migrated and unmigrated services keep
  talking to each other.
- **`ImportFromExistingOutbox`** drains messages the old outbox saved but never sent into Twinbox's outbox.
- **`SeedInboxFromExisting`** tells Twinbox's inbox which messages the old system already processed, so redeliveries
  after the switch are skipped.

```csharp
builder.Services.AddTwinbox(twinbox => twinbox
    .UseSqlServer(connectionString)
    .UseRabbitMQ(ConfigureRabbit)
    // Write and read the old system's header names too.
    .UseHeaderProfile(HeaderProfile.Prefixed("legacy"))          // legacy-msg-id, legacy-msg-name
    // Keep draining messages the old outbox never sent.
    .ImportFromExistingOutbox(o =>
    {
        o.CreateConnection = _ => new SqlConnection(connectionString);
        o.SelectPending = "SELECT TOP (@batch) Id, Name, Content FROM old.Published WHERE Status = 'Scheduled' ORDER BY Id";
        o.MarkImported = "UPDATE old.Published SET Status = 'Migrated' WHERE Id = @id";
    })
    // Remember what the old inbox already processed.
    .SeedInboxFromExisting(o =>
    {
        o.CreateConnection = _ => new SqlConnection(connectionString);
        o.SelectProcessed = """
            SELECT Id AS MessageId, 'shipping.ship-order' AS Consumer
            FROM old.Received WHERE Status = 'Succeeded' AND Added > DATEADD(day, -7, SYSUTCDATETIME())
            """;
    })
    .Route<OrderPlaced>().To("orders")
    .AddHandler<ShipOrder, OrderPlaced>(consumerName: "shipping.ship-order"));
```

The rest of this page explains each piece, then gives a [cutover checklist](#cutover-checklist) and a
[rollback plan](#rollback).

## 1. Take inventory

For the service you're moving, write down:

| What | Why |
|---|---|
| The header (or property) names the old system uses for message id, message type and partition/ordering key | To build the header profile. |
| How the type is written on the wire (short name, full CLR name, a custom string) | To give Twinbox types the same names. |
| The payload format: JSON casing, enum format, envelopes | To configure the serializer to match. |
| Destinations it sends to and queues/subscriptions it consumes | To map routes and listeners onto the same topology. |
| The old outbox table: key, type name and body columns, the status that means "not sent yet" | For the import. |
| The old inbox or "received" table: processed message ids and which consumer processed them | For inbox seeding. |
| Correlation, tenant or other headers peers rely on | To carry them with filters. |

## 2. Speak the old wire format

### Header names

A [header profile](concepts/headers.md#header-profiles) makes Twinbox write the old header names on every outgoing
message, next to its own, and read them on incoming messages:

```csharp
twinbox.UseHeaderProfile(HeaderProfile.Prefixed("legacy"));   // "{prefix}-msg-id", "{prefix}-msg-name"
```

`Prefixed` covers a naming scheme that is common among outbox implementations. For anything else, spell the names out:

```csharp
twinbox.UseHeaderProfile(new HeaderProfile(
    MessageId: "x-message-id",
    MessageName: "x-message-type",
    PartitionKey: "x-ordering-key")
{
    SentTime = "x-sent-at",
    Constants = new Dictionary<string, string> { ["x-producer"] = "orders-service" },
});
```

On the way in, when the profile's id header is present, its id, name and partition key win over what the transport
derived. That matters: Twinbox's inbox deduplicates on that id, so it must be the same id the old system used for the
same message.

### Message names

Give each Twinbox message type the name the old system puts on the wire:

```csharp
[MessageName("Contoso.Orders.Contracts.OrderPlaced")]   // whatever the old type header carries
public record OrderPlaced(Guid OrderId, decimal Total);
```

Incoming messages are matched by this name, and so are imported outbox rows.

### Payloads

Twinbox writes camelCase JSON by default. If the old system writes PascalCase, string enums or something else, match
it:

```csharp
twinbox.UseSerializer(new SystemTextJsonMessageSerializer(new JsonSerializerOptions
{
    PropertyNamingPolicy = null,                      // PascalCase, as written
    PropertyNameCaseInsensitive = true,               // read either
    Converters = { new JsonStringEnumConverter() },
}));
```

If the old payloads are wrapped in an envelope, implement `IMessageSerializer` to unwrap on read and wrap on write.
See [Serialization](concepts/serialization.md).

### Other headers

Header profiles cover id, name, partition key, send time and constants. Carry anything else with filters. For
example, to mirror Twinbox's correlation id into the old header and read it back:

```csharp
public sealed class LegacyCorrelationOut : IOutgoingMessageFilter
{
    public void OnSending(object message, IDictionary<string, string> headers)
    {
        if (headers.TryGetValue(TransportHeaders.CorrelationId, out var id))
        {
            headers["legacy-corr-id"] = id;
        }
    }
}

twinbox.AddOutgoingFilter<LegacyCorrelationOut>();
```

Incoming headers are all available in `MessageContext.Headers`.

### Topology

Route to the same destinations the old system sends to, and listen on the same queues or subscriptions it consumes
(or on new ones bound to the same exchanges and topics). Check each [transport page](transports/index.md) for how a
destination maps onto the broker, for example RabbitMQ publishes to an exchange with the message name as routing key.

## 3. Drain the old outbox

Messages the old outbox saved but never sent must still go out. `ImportFromExistingOutbox` polls the old table and
copies each pending row into Twinbox's outbox, where the dispatcher sends it like any other message.

```csharp
twinbox.ImportFromExistingOutbox(o =>
{
    o.CreateConnection = sp => new NpgsqlConnection(connectionString);
    o.SelectPending = """
        SELECT id AS "Id", message_type AS "Name", payload AS "Content"
        FROM legacy.outbox
        WHERE processed_at IS NULL AND created_at < now() - interval '30 seconds'
        ORDER BY id
        LIMIT @batch
        """;
    o.MarkImported = "UPDATE legacy.outbox SET processed_at = now() WHERE id = @id::uuid";
    o.BatchSize = 100;                               // default
    o.PollInterval = TimeSpan.FromSeconds(10);       // default
});
```

| Option | Description |
|---|---|
| `CreateConnection` | Opens a connection to the database holding the old table. Required. |
| `SelectPending` | Returns up to `@batch` unsent rows with columns `Id`, `Name` and `Content`. Required. |
| `MarkImported` | Marks one row so it isn't selected again; receives `@id`. Required. |
| `BatchSize` | Rows per select; a full batch is followed right away by the next. Default 100. |
| `PollInterval` | Time between polls. Default 10 seconds. |

How it behaves:

- **It keeps polling** for as long as the app runs, because old instances may still be writing during a rolling
  deploy. Remove it once the old table stays empty.
- **`Name` is matched against Twinbox's message names** (step 2). Rows with an unknown name are skipped and stay
  unmarked; a warning is logged once per name, telling you which `[MessageName]` to add.
- **`Content` is the message body**, as bytes or text (text is encoded as UTF-8). It is delivered as is, so it must be
  in the format your serializer reads.
- **Routing uses Twinbox's routes** for the type, not whatever destination the old row recorded. A type with two routes
  produces two outbox rows.
- **Ids are deterministic**, derived from the old `Id` and the destination. Importing the same row twice produces the
  same id, which the outbox's unique index rejects (a warning is logged; mark that row by hand if it keeps coming
  back), and which receivers deduplicate.
- **Imported rows carry no headers**: no partition key, no tenant, no correlation id. Ordering isn't preserved across
  the switch for them.
- **Parameters:** `@batch` is bound as an integer and `@id` as a string, so cast in SQL when the old key is a number or
  a UUID (`@id::uuid` on PostgreSQL). On Oracle, write the placeholders as `:batch` and `:id`.
- The import writes to the app's outbox store (with several EF Core contexts, the last registered), runs without a
  tenant, and needs a registered transport for each route.

> [!WARNING]
> A row the old system's dispatcher is sending at the same moment can go out twice: once from the old dispatcher with
> its old id, once from Twinbox with the derived id. Receivers can't match the two. Select only rows older than the old
> dispatcher's normal latency (as above), or stop the old dispatcher before the import starts where you can.

## 4. Seed the inbox

After the switch, the broker may redeliver messages the old consumer already processed (unacknowledged at shutdown,
replayed from a log, a retry from a peer). Twinbox's inbox has never seen them. `SeedInboxFromExisting` copies the
old system's processed ids in at startup:

```csharp
twinbox.SeedInboxFromExisting(o =>
{
    o.CreateConnection = sp => new SqlConnection(connectionString);
    o.SelectProcessed = """
        SELECT MessageId,
               CASE Consumer WHEN 'ShippingService' THEN 'shipping.ship-order'
                             WHEN 'Billing'         THEN 'billing.invoice-order' END AS Consumer
        FROM old.Received
        WHERE Status = 'Succeeded' AND ReceivedAt > DATEADD(day, -3, SYSUTCDATETIME())
        """;
});
```

| Option | Description |
|---|---|
| `CreateConnection` | Opens a connection to the database holding the old inbox. Required. |
| `SelectProcessed` | Returns columns `MessageId` and `Consumer`. Required. |

- **`MessageId`** must be the id Twinbox will see for a redelivery: the value of the profile's id header, or the
  broker's message id. Usually that's what the old system stored.
- **`Consumer`** is the Twinbox consumer name of the handler that takes over, one row per handler that should skip
  the message. Pin consumer names when you register handlers (`AddHandler<ShipOrder, OrderPlaced>("shipping.ship-order")`)
  so they're easy to write here and stable afterwards.
- It runs **once, at startup, before the app starts serving**, and inserts one entry at a time. Existing entries are
  left alone, so restarts are harmless. Select only the redelivery window you care about to keep startup quick.
- Seeded entries count as processed now, so they're kept for `Retention:InboxEntries` from the time of seeding.
- It uses the app's inbox store and runs without a tenant.

## Cutover checklist

Per service, in any order across services:

1. **Inventory** the wire format, topology and old tables (step 1).
2. **Add Twinbox** next to the old code path: store, transport, header profile, serializer settings, `[MessageName]`
   on every message type, routes and listeners on the existing topology.
3. **Pin consumer names** for every handler.
4. **Configure the import and seeding** (steps 3 and 4). Test the SQL against a copy of production data.
5. **Test both directions** in a staging environment: an old-system peer sending to the migrated service, and the
   migrated service sending to an old-system peer. Check ids, names, payload casing and partition keys on both sides.
6. **Remove the old library's sending and receiving** from this service in the same release, so each message is sent
   and handled by exactly one system.
7. **Deploy.** Watch the logs for "Imported N message(s) from the existing outbox", unknown-name warnings, the
   seeding count, and the `twinbox.outbox.dead_lettered` and `twinbox.inbox.duplicates` metrics.
8. **Wait for the old outbox to stay empty** (no rows matching `SelectPending`) after the last old instance is gone,
   then remove `ImportFromExistingOutbox`. Remove `SeedInboxFromExisting` once the redelivery window has passed.
9. **Drop the old tables** after a safety period.
10. **Keep the header profile** until every peer that reads or writes the old headers has migrated. Then remove it
    from all services.

## Rollback

Until step 8, rolling back is redeploying the previous version, with two things to take care of:

- **Twinbox's unsent messages.** The old version doesn't read Twinbox's outbox. Before (or while) rolling back, let
  Twinbox's dispatcher finish: keep one instance of the new version running as a sender until the Twinbox outbox has no
  pending rows (the [health check](observability.md#health-checks) and the [dashboard](dashboard.md) show the count).
  An instance with no listeners and the dispatcher on does exactly that.
- **Deduplication after rollback.** The old inbox doesn't know what Twinbox processed. Redeliveries of messages handled
  by Twinbox may run again in the old code; keep rollback windows short and old handlers idempotent where possible.

Rows the import already marked in the old table are Twinbox's responsibility: they are sent from Twinbox's outbox, so
don't reset their markers when you roll back. Because the header profile makes Twinbox's messages readable by the old
system, the rest of the fleet keeps working in either direction.
