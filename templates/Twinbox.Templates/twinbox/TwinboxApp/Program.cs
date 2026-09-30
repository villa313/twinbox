#if (EfCore)
using Microsoft.EntityFrameworkCore;
#endif
#if (MongoDB)
using MongoDB.Driver;
#endif
using Twinbox;
#if (InMemoryStore)
using Twinbox.InMemory;
#endif
#if (webhooks)
using Twinbox.Webhooks;
#endif
using TwinboxApp;
#if (EfCore || AdoNet)
using TwinboxApp.Data;
#endif

var builder = WebApplication.CreateBuilder(args);

#if (HasDatabase || HasBroker)
string ConnectionString(string name) => builder.Configuration.GetConnectionString(name)
    ?? throw new InvalidOperationException($"Set ConnectionStrings:{name} in appsettings.json or the environment.");

#endif
#if (EfSqlServer)
builder.Services.AddDbContext<AppDbContext>(options => options.UseSqlServer(ConnectionString("Database")));
#elif (EfPostgres)
builder.Services.AddDbContext<AppDbContext>(options => options.UseNpgsql(ConnectionString("Database")));
#elif (EfMySql)
builder.Services.AddDbContext<AppDbContext>(options => options.UseMySQL(ConnectionString("Database")));
#elif (EfSqlite)
builder.Services.AddDbContext<AppDbContext>(options => options.UseSqlite(ConnectionString("Database")));
#elif (AdoNet)
builder.Services.AddSingleton(new OrderDatabase(ConnectionString("Database")));
#elif (MongoDB)
var mongoDatabase = builder.Configuration["MongoDB:Database"] ?? "app";
builder.Services.AddSingleton<IMongoClient>(new MongoClient(ConnectionString("Database")));
#endif

builder.Services.AddTwinbox(twinbox => twinbox
#if (EfCore)
    .UseEntityFrameworkCore<AppDbContext>()
#elif (AdoSqlServer)
    .UseSqlServer(ConnectionString("Database"))
#elif (AdoPostgres)
    .UsePostgreSql(ConnectionString("Database"))
#elif (AdoMySql)
    .UseMySql(ConnectionString("Database"))
#elif (MongoDB)
    .UseMongoDB(mongo =>
    {
        mongo.ClientFactory = services => services.GetRequiredService<IMongoClient>();
        mongo.DatabaseName = mongoDatabase;
    })
#else
    .UseInMemoryStore()
#endif
#if (AzureServiceBus)
    .UseAzureServiceBus(ConnectionString("ServiceBus"), serviceBus => serviceBus.Listen("orders"))
#elif (RabbitMQ)
    .UseRabbitMQ(ConnectionString("RabbitMQ"), rabbit => rabbit.Listen("orders.handler", exchange: "orders"))
#elif (Kafka)
    .UseKafka(ConnectionString("Kafka"), kafka => kafka.Listen("orders", groupId: "orders-handler"))
#elif (AmazonSqs)
    .UseAmazonSqs(sqs =>
    {
        sqs.Region = builder.Configuration["AmazonSqs:Region"];
        if (builder.Configuration["AmazonSqs:ServiceUrl"] is { Length: > 0 } serviceUrl)
        {
            sqs.ServiceUrl = new Uri(serviceUrl);
        }
        sqs.AutoCreate = true;
        sqs.Listen("orders");
    })
#elif (Nats)
    .UseNats(ConnectionString("Nats"), nats =>
    {
        nats.AutoCreateStreams = true;
        nats.AddStream("ORDERS", "orders");
        nats.Listen("ORDERS", durableConsumer: "orders-handler");
    })
#elif (Redis)
    .UseRedisStreams(ConnectionString("Redis"), redis => redis.Listen("orders", group: "orders-handler"))
#else
    .UseLocalDelivery()
#endif
#if (webhooks && !LocalDelivery)
    .AddWebhooks()
    .AddHandler<PaymentWebhookHandler, PaymentWebhook>()
    // AddWebhooks() also registers local delivery, so routes to the broker name their transport.
    .Route<OrderPlaced>().To("orders", transport: "TRANSPORT_NAME")
#elif (webhooks)
    .AddWebhooks()
    .AddHandler<PaymentWebhookHandler, PaymentWebhook>()
    .Route<OrderPlaced>().To("orders")
#else
    .Route<OrderPlaced>().To("orders")
#endif
    .AddHandler<OrderPlacedHandler, OrderPlaced>());

var app = builder.Build();

#if (EfCore)
if (app.Environment.IsDevelopment())
{
    // Creates the orders and Twinbox tables on first run; use EF Core migrations beyond local development.
    await using var scope = app.Services.CreateAsyncScope();
    await scope.ServiceProvider.GetRequiredService<AppDbContext>().Database.EnsureCreatedAsync();
}

#elif (AdoNet)
// Twinbox creates its own tables on startup; this creates the sample's orders table.
await app.Services.GetRequiredService<OrderDatabase>().EnsureCreatedAsync();

#endif
#if (EfCore)
app.MapPost("/orders", async (PlaceOrder request, AppDbContext db, IOutbox outbox, CancellationToken cancellationToken) =>
{
    var order = new Order(Guid.NewGuid(), request.Total);
    db.Orders.Add(order);
    outbox.Send(new OrderPlaced(order.Id, order.Total));
    await db.SaveChangesAsync(cancellationToken);   // the order and the message commit together, then the message is sent
    return Results.Created($"/orders/{order.Id}", order);
});
#elif (AdoNet)
app.MapPost("/orders", async (PlaceOrder request, OrderDatabase database, IOutbox outbox, CancellationToken cancellationToken) =>
{
    var order = new Order(Guid.NewGuid(), request.Total);
    await using var connection = await database.OpenConnectionAsync(cancellationToken);
    await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
    await OrderDatabase.InsertAsync(transaction, order, cancellationToken);
    outbox.Send(new OrderPlaced(order.Id, order.Total));
    await outbox.CommitAsync(transaction, cancellationToken);   // saves the message, commits, wakes the dispatcher
    return Results.Created($"/orders/{order.Id}", order);
});
#elif (MongoDB)
app.MapPost("/orders", async (PlaceOrder request, IMongoClient client, IOutbox outbox, CancellationToken cancellationToken) =>
{
    var order = new Order(Guid.NewGuid(), request.Total);
    using var session = await client.StartSessionAsync(cancellationToken: cancellationToken);
    session.StartTransaction();
    await client.GetDatabase(mongoDatabase).GetCollection<Order>("orders")
        .InsertOneAsync(session, order, cancellationToken: cancellationToken);
    outbox.Send(new OrderPlaced(order.Id, order.Total));
    await outbox.CommitAsync(session, cancellationToken);   // saves the message, commits, wakes the dispatcher
    return Results.Created($"/orders/{order.Id}", order);
});
#else
app.MapPost("/orders", async (PlaceOrder request, IOutbox outbox, InMemoryUnitOfWork unitOfWork, CancellationToken cancellationToken) =>
{
    var order = new Order(Guid.NewGuid(), request.Total);
    outbox.Send(new OrderPlaced(order.Id, order.Total));
    await unitOfWork.CommitAsync(cancellationToken);   // the in-memory store has no database transaction to join
    return Results.Created($"/orders/{order.Id}", order);
});
#endif
#if (dashboard)

// Anonymous in development only; elsewhere it refuses every request until you add .RequireAuthorization("your-policy").
app.MapTwinboxDashboard("/twinbox", dashboard => dashboard.AllowAnonymous = app.Environment.IsDevelopment());
#endif
#if (webhooks)

app.MapWebhookInbox<PaymentWebhook>("/webhooks/payments",
    webhook => webhook.VerifyStandardWebhooks(WebhookSecrets.FromConfiguration("Webhooks:PaymentsSecret")));
#endif

app.Run();
