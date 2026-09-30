using System.Data;
using System.Data.Common;
using Microsoft.Extensions.DependencyInjection;

namespace Twinbox.UnitTests;

public sealed class TransactionalOutboxTests
{
    [Fact]
    public async Task SaveThroughDbTransaction_WithoutAdoNetStore_NamesEveryAdoNetStore()
    {
        await using var host = TestHost.Create(b => b.Route<OrderPlaced>().To("orders"));
        await using var scope = host.Services.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<IOutbox>();
        outbox.Send(new OrderPlaced(1));

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => outbox.SaveAsync(new UnusedTransaction()));

        Assert.All(
            ["UseSqlServer", "UsePostgreSql", "UseMySql", "UseOracle"],
            method => Assert.Contains(method, error.Message, StringComparison.Ordinal));
    }

    private sealed class UnusedTransaction : DbTransaction
    {
        public override IsolationLevel IsolationLevel => IsolationLevel.ReadCommitted;

        protected override DbConnection? DbConnection => null;

        public override void Commit() => throw new NotSupportedException();

        public override void Rollback() => throw new NotSupportedException();
    }
}
