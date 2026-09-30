using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Twinbox;
using Twinbox.AotSmoke;
using Twinbox.InMemory;
using Twinbox.Serialization;
using Twinbox.Transport;

const int Orders = 20;
const int Invoices = 10;
const int Shipments = 5;
var timeout = TimeSpan.FromSeconds(30);

var ledger = new Ledger();

var builder = Host.CreateApplicationBuilder(args);
builder.Logging.SetMinimumLevel(LogLevel.Warning);
builder.Services.AddSingleton(ledger);
builder.Services.AddTwinbox(twinbox => twinbox
    .UseInMemoryStore()
    .UseLocalDelivery()
    .UseHttp(http => http.AddEndpoint("shipping-webhook", endpoint =>
    {
        endpoint.Url = new Uri("https://shipping.example/hooks/{messageId}");
        endpoint.WebhookSecret = WebhookReceiver.Secret;
        endpoint.ConfigureHttpClient = client => client.ConfigurePrimaryHttpMessageHandler(() => new WebhookReceiver(ledger));
    }))
    .UseJsonTypeInfoResolver(SmokeJsonContext.Default)
    .Route<OrderPlaced>().To("orders", LocalTransport.TransportName)
    .Route<InvoiceRequested>().To("invoices", LocalTransport.TransportName)
    .Route<OrderShipped>().To("shipping-webhook", "http")
    .AddFilter<CountingFilter>()
    .AddBatchFilter<CountingBatchFilter>()
    .AddHandlersFromTwinboxAotSmoke());

using var host = builder.Build();
await host.StartAsync();

for (var i = 1; i <= Orders; i++)
{
    await using var scope = host.Services.CreateAsyncScope();
    var outbox = scope.ServiceProvider.GetRequiredService<IOutbox>();
    outbox.Send(new OrderPlaced(i, $"SKU-{i}", i * 10.5m));
    if (i <= Invoices)
    {
        outbox.Send(new InvoiceRequested(i, $"customer-{i}@example.com"));
    }

    if (i <= Shipments)
    {
        outbox.Send(new OrderShipped(i, "TRACK-" + i), new SendOptions { PartitionKey = $"order-{i}" });
    }

    await scope.ServiceProvider.GetRequiredService<InMemoryUnitOfWork>().CommitAsync();
}

var expected = new Dictionary<string, int>
{
    ["order"] = Orders,
    ["invoice"] = Invoices,
    ["shipment"] = Shipments,
};
var failures = new List<string>();
if (!await ledger.WaitForAsync(expected, timeout))
{
    failures.Add($"timed out waiting for delivery: {ledger.Describe()}");
}

else
{
    var pipeline = host.Services.GetRequiredService<IInboundPipeline>();
    var serializer = host.Services.GetRequiredService<IMessageSerializer>();
    await ReplayAsync(pipeline, serializer, ledger);
    await FreshBatchAsync(pipeline, serializer);
    expected["invoice"] = Invoices + 3;
}

await host.StopAsync();

failures.AddRange(ledger.Verify(expected));
failures.AddRange(WebhookReceiver.Errors);

// Message filters wrap IHandle calls and batch filters wrap IHandleBatch calls; replayed duplicates reach neither.
if (ledger.FilterCalls != Orders)
{
    failures.Add($"filter ran {ledger.FilterCalls} times, expected {Orders}");
}

if (ledger.BatchFilterItems != Invoices + 3)
{
    failures.Add($"batch filter saw {ledger.BatchFilterItems} messages, expected {Invoices + 3}");
}

if (failures.Count > 0)
{
    foreach (var failure in failures)
    {
        Console.Error.WriteLine($"FAIL: {failure}");
    }

    return 1;
}

Console.WriteLine($"OK: {Orders} orders, {Invoices + 3} invoices and {Shipments} webhooks handled exactly once (dynamic code: {RuntimeFeature.IsDynamicCodeSupported})");
return 0;

// A redelivery of already-handled messages must be absorbed by the inbox, one at a time and as a batch.
static async Task ReplayAsync(IInboundPipeline pipeline, IMessageSerializer serializer, Ledger ledger)
{
    var order = ledger.HandledMessageIds("order")[0];
    await pipeline.ProcessAsync(Incoming(order, "OrderPlaced", serializer, new OrderPlaced(1, "SKU-1", 10.5m)), CancellationToken.None);

    var invoices = ledger.HandledMessageIds("invoice")
        .Select(id => Incoming(id, "InvoiceRequested", serializer, new InvoiceRequested(0, "replay@example.com")))
        .ToList();
    await pipeline.ProcessBatchAsync(invoices, CancellationToken.None);
}

static Task FreshBatchAsync(IInboundPipeline pipeline, IMessageSerializer serializer)
{
    var batch = Enumerable.Range(101, 3)
        .Select(i => Incoming(Guid.NewGuid().ToString(), "InvoiceRequested", serializer, new InvoiceRequested(i, $"batch-{i}@example.com")))
        .ToList();
    return pipeline.ProcessBatchAsync(batch, CancellationToken.None);
}

static IncomingMessage Incoming<TMessage>(string id, string name, IMessageSerializer serializer, TMessage message)
    where TMessage : class =>
    new(id, name, "replay", serializer.Serialize(message, typeof(TMessage)), serializer.ContentType, new Dictionary<string, string>(), 2, null);
