using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Twinbox.Storage;
using Twinbox.Tenancy;
using Twinbox.Transport;

namespace Twinbox.UnitTests;

public sealed class TenancyTests
{
    [Fact]
    public async Task Send_StampsTheScopesTenant()
    {
        await using var host = CreateHost(new TenantLog());

        await using (var scope = host.Services.CreateAsyncScope())
        {
            scope.ServiceProvider.GetRequiredService<TenantContext>().Id = "acme";
            scope.ServiceProvider.GetRequiredService<IOutbox>().Send(new OrderPlaced(1));
            await scope.ServiceProvider.GetRequiredService<InMemory.InMemoryUnitOfWork>().CommitAsync();
        }

        await host.Harness.DrainAsync();

        Assert.Equal("acme", Assert.Single(host.Harness.Store.Snapshot()).TenantId);
        Assert.Equal("acme", Assert.Single(host.Harness.Transport.Sent).Headers[TransportHeaders.TenantId]);
    }

    [Fact]
    public async Task Handler_RunsInTheMessagesTenantAndPassesItOn()
    {
        var log = new TenantLog();
        await using var host = CreateHost(log, b => b
            .Route<OrderShipped>().To("shipping")
            .AddHandler<TenantAwareHandler, OrderPlaced>());

        var headers = new Dictionary<string, string> { [TransportHeaders.TenantId] = "globex" };
        await host.Services.GetRequiredService<IInboundPipeline>().ProcessAsync(
            new IncomingMessage("m-1", "OrderPlaced", "orders", Encoding.UTF8.GetBytes("""{"orderId":3}"""), "application/json", headers, 1, null),
            default);
        await host.Harness.DrainAsync();

        Assert.Equal(["globex"], log.Entries);
        Assert.Equal("globex", Assert.Single(host.Harness.Transport.Sent).Headers[TransportHeaders.TenantId]);
    }

    [Fact]
    public async Task Dispatcher_VisitsEveryTenant()
    {
        var log = new TenantLog();
        await using var host = CreateHost(log, services: s => s.AddSingleton<IOutboxStore, TenantRecordingStore>());

        await host.Services.GetRequiredService<IOutboxDispatcher>().DispatchBatchAsync(default);

        Assert.Equal(["acme", "globex"], log.Entries);
    }

    [Fact]
    public void UseTenants_WithMissingCallbacks_Throws()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentException>(() => services.AddTwinbox(b => b.UseTenants(o => o.ListTenants = (_, _) => Task.FromResult<IReadOnlyCollection<string>>([]))));
    }

    private static TestHost CreateHost(TenantLog log, Action<TwinboxBuilder>? configure = null, Action<IServiceCollection>? services = null) =>
        TestHost.Create(
            b =>
            {
                b.Route<OrderPlaced>().To("orders").UseTenants(o =>
                {
                    o.ListTenants = (_, _) => Task.FromResult<IReadOnlyCollection<string>>(["acme", "globex"]);
                    o.EnterTenant = (sp, tenant) => sp.GetRequiredService<TenantContext>().Id = tenant;
                    o.CurrentTenant = sp => sp.GetRequiredService<TenantContext>().Id;
                });
                configure?.Invoke(b);
            },
            s =>
            {
                s.AddScoped<TenantContext>().AddSingleton(log);
                services?.Invoke(s);
            });

    public sealed class TenantContext
    {
        public string? Id { get; set; }
    }

    public sealed class TenantLog
    {
        private readonly List<string?> _entries = [];

        public IReadOnlyList<string?> Entries
        {
            get
            {
                lock (_entries)
                {
                    return [.. _entries];
                }
            }
        }

        public void Add(string? entry)
        {
            lock (_entries)
            {
                _entries.Add(entry);
            }
        }
    }

    public sealed class TenantAwareHandler(TenantContext tenant, TenantLog log, IOutbox outbox) : IHandle<OrderPlaced>
    {
        public Task HandleAsync(OrderPlaced message, MessageContext context, CancellationToken cancellationToken)
        {
            log.Add(tenant.Id);
            outbox.Send(new OrderShipped(message.OrderId));
            return Task.CompletedTask;
        }
    }

    private sealed class TenantRecordingStore(TwinboxScopeFactory scopes, TenantLog log) : IOutboxStore
    {
        public string Name => "TenantRecording";

        public async Task<IReadOnlyList<OutboxMessage>> ClaimAsync(OutboxClaim claim, CancellationToken cancellationToken)
        {
            await using var scope = scopes.CreateAsyncScope();
            log.Add(scope.ServiceProvider.GetRequiredService<TenantContext>().Id);
            return [];
        }

        public Task AppendAsync(IReadOnlyList<OutboxMessage> messages, CancellationToken cancellationToken) => Task.CompletedTask;

        public Task CompleteAsync(string owner, IReadOnlyList<DispatchOutcome> outcomes, CancellationToken cancellationToken) => Task.CompletedTask;

        public Task<int> PurgeAsync(OutboxPurge purge, CancellationToken cancellationToken) => Task.FromResult(0);

        public Task<OutboxStatistics> GetStatisticsAsync(CancellationToken cancellationToken) =>
            Task.FromResult(new OutboxStatistics(0, null, 0));
    }
}
