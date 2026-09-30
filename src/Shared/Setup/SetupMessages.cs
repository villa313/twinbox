namespace Twinbox;

/// <summary>Startup and first-use error text that tells users which package to install and what to call.</summary>
internal static class SetupMessages
{
    private static readonly Dictionary<string, string> KnownTransports = new(StringComparer.OrdinalIgnoreCase)
    {
        ["rabbitmq"] = "Install Twinbox.RabbitMQ and call UseRabbitMQ(...).",
        ["azureservicebus"] = "Install Twinbox.AzureServiceBus and call UseAzureServiceBus(...).",
        ["kafka"] = "Install Twinbox.Kafka and call UseKafka(...).",
        ["amazonsqs"] = "Install Twinbox.AmazonSqs and call UseAmazonSqs(...).",
        ["nats"] = "Install Twinbox.Nats and call UseNats(...).",
        ["redisstreams"] = "Install Twinbox.RedisStreams and call UseRedisStreams(...).",
        ["eventhubs"] = "Install Twinbox.EventHubs and call UseEventHubs(...).",
        ["googlepubsub"] = "Install Twinbox.GooglePubSub and call UseGooglePubSub(...).",
        ["pulsar"] = "Install Twinbox.Pulsar and call UsePulsar(...).",
        ["http"] = "Install Twinbox.Http and call UseHttp(...).",
        ["local"] = "Call UseLocalDelivery().",
        ["inmemory"] = "Call UseInMemoryTransport().",
    };

    public const string InstallStore =
        "Install a store package and call its method inside AddTwinbox: Twinbox.EntityFrameworkCore (UseEntityFrameworkCore<TContext>()), "
        + "Twinbox.SqlServer (UseSqlServer(...)), Twinbox.PostgreSql (UsePostgreSql(...)), Twinbox.MySql (UseMySql(...)), "
        + "Twinbox.Oracle (UseOracle(...)) or Twinbox.MongoDB (UseMongoDB(...)); or call UseInMemoryStore() for tests and local development.";

    public const string InstallTransport =
        "Install a transport package and call its method inside AddTwinbox, such as Twinbox.RabbitMQ (UseRabbitMQ(...)), "
        + "Twinbox.AzureServiceBus (UseAzureServiceBus(...)), Twinbox.Kafka (UseKafka(...)), Twinbox.AmazonSqs (UseAmazonSqs(...)), "
        + "Twinbox.Nats (UseNats(...)), Twinbox.RedisStreams (UseRedisStreams(...)), Twinbox.EventHubs (UseEventHubs(...)), "
        + "Twinbox.GooglePubSub (UseGooglePubSub(...)), Twinbox.Pulsar (UsePulsar(...)) or Twinbox.Http (UseHttp(...)); "
        + "or call UseLocalDelivery() to deliver to this app's own handlers, or UseInMemoryTransport() for tests.";

    public const string NoOutboxStore = "No Twinbox outbox store is registered. " + InstallStore;

    public const string NoTransport = "No Twinbox transport is registered. " + InstallTransport;

    public static string NoOutboxStoreFor(string feature) =>
        $"{feature} needs an outbox store, but none is registered. {InstallStore}";

    public static string UnknownTransport(string name, IEnumerable<string> registered)
    {
        var names = string.Join(", ", registered);
        var fix = InstallTransportNamed(name)
            ?? "Check the transport name in the route, or install that transport's package and call its method inside AddTwinbox.";
        return $"No transport named '{name}' is registered (registered: {(names.Length == 0 ? "none" : names)}). {fix}";
    }

    /// <summary>How to get a transport by its registered name; null for names no Twinbox package uses.</summary>
    public static string? InstallTransportNamed(string name) =>
        KnownTransports.TryGetValue(name, out var fix) ? fix : null;
}
