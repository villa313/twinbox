using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Twinbox.EntityFrameworkCore;

namespace Twinbox.Benchmarks.Throughput;

public sealed class BenchContext(DbContextOptions<BenchContext> options) : DbContext(options)
{
    public DbSet<Order> Orders => Set<Order>();

    public async Task ResetAsync()
    {
        foreach (var entity in new[] { Model.FindEntityType(typeof(Order))!, Model.FindEntityType(TwinboxEntities.Outbox)!, Model.FindEntityType(TwinboxEntities.Inbox)! })
        {
            var table = this.GetService<ISqlGenerationHelper>().DelimitIdentifier(entity.GetTableName()!, entity.GetSchema());
#pragma warning disable EF1003 // The table name comes from the model, not from input.
            await Database.ExecuteSqlRawAsync("DELETE FROM " + table);
#pragma warning restore EF1003
        }
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder) => modelBuilder.AddTwinbox();
}

public sealed class Order
{
    public int Id { get; set; }

    public required string Reference { get; set; }
}
