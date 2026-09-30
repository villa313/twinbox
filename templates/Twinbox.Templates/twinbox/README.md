# TwinboxApp

An ASP.NET Core app wired with [Twinbox](https://villa313.github.io/twinbox/): `POST /orders` saves an order and an
`OrderPlaced` message in one transaction, the outbox delivers the message, and `OrderPlacedHandler` handles it through
the inbox, once per message id.

## Run it

<!--#if (HasDatabase || HasBroker || AmazonSqs) -->
1. Point `appsettings.json` at your infrastructure. The values there are local-development placeholders.
<!--#if (MongoDB) -->
   MongoDB transactions need a replica set; a single-node one (`--replSet rs0`) is fine for development.
<!--#endif -->
<!--#if (AzureServiceBus) -->
   Create an `orders` queue first; Twinbox does not create Service Bus entities.
<!--#endif -->
<!--#if (AmazonSqs) -->
   `ServiceUrl` points at LocalStack; remove it to use AWS. Credentials come from the AWS SDK's default chain.
<!--#endif -->
<!--#if (Kafka) -->
   Create an `orders` topic unless the broker creates topics automatically.
<!--#endif -->
2. `dotnet run --project TwinboxApp`
3. Send a request from `TwinboxApp/TwinboxApp.http`, or:
<!--#else -->
1. `dotnet run --project TwinboxApp`
2. Send a request from `TwinboxApp/TwinboxApp.http`, or:
<!--#endif -->

   ```bash
   curl -X POST http://localhost:5080/orders -H "Content-Type: application/json" -d '{"total": 42.50}'
   ```

The log shows `Order ... placed` once the handler has run.
<!--#if (tests) -->

Run the tests with `dotnet test`; they need no database or broker.
<!--#endif -->

## Where things are

| File | What it shows |
|---|---|
| `Program.cs` | `AddTwinbox(...)`: the store, the transport, the route for `OrderPlaced` and the handler. |
<!--#if (EfCore) -->
| `Data/AppDbContext.cs` | `modelBuilder.AddTwinbox()` maps the outbox and inbox tables into your context. |
<!--#elif (AdoNet) -->
| `Data/OrderDatabase.cs` | Plain ADO.NET access to the sample's `orders` table. |
<!--#endif -->
| `OrderPlacedHandler.cs` | An `IHandle<OrderPlaced>`; it runs once per message id even when a message is delivered again. |
<!--#if (webhooks) -->
| `PaymentWebhook.cs` | A webhook handled through the inbox. `POST /webhooks/payments` checks a Standard Webhooks signature with `Webhooks:PaymentsSecret`. |
<!--#endif -->
<!--#if (tests) -->
| `TwinboxApp.Tests` | Tests with `UseTestHarness()`: no database or broker, and dispatch runs when you call `DrainAsync()`. |
<!--#endif -->

<!--#if (EfCore) -->
In development the app creates its tables with `EnsureCreated`. For anything else, add a migration:
`dotnet ef migrations add AddTwinbox`.

<!--#elif (InMemoryStore) -->
The in-memory store loses everything on restart. Switch to a database store before this app goes anywhere.

<!--#endif -->
<!--#if (LocalDelivery) -->
With local delivery the outbox is the queue: the dispatcher runs this app's handlers directly, with retries.

<!--#endif -->
<!--#if (dashboard) -->
The dashboard is at `/twinbox`. It allows anonymous access in development only; attach a policy with
`.RequireAuthorization("your-policy")` for other environments, where it refuses every request until you do.

<!--#endif -->
## Next steps

- [Documentation](https://villa313.github.io/twinbox/): stores, transports, retries, ordering and more.
- [Configuration reference](https://villa313.github.io/twinbox/articles/configuration.html): every `Twinbox:*` setting.
