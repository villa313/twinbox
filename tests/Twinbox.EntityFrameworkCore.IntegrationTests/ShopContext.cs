using Microsoft.EntityFrameworkCore;

namespace Twinbox.EntityFrameworkCore.IntegrationTests;

public sealed class ShopContext(DbContextOptions<ShopContext> options) : DbContext(options)
{
    public DbSet<Order> Orders => Set<Order>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>().HasIndex(o => o.Reference).IsUnique();

        // A custom schema proves the hand-written SQL follows the model's names.
        modelBuilder.AddTwinbox(o => o.Schema = "messaging");
    }
}

public sealed class Order
{
    public int Id { get; set; }

    public required string Reference { get; set; }
}

public sealed record PlaceOrder(string Reference, bool Fail = false);

public sealed record ReserveStock(string Reference);

public sealed record OrderPlaced(string Reference);

/// <summary>Second module with its own outbox table, as in a modular monolith.</summary>
public sealed class BillingContext(DbContextOptions<BillingContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder) =>
        modelBuilder.AddTwinbox(o =>
        {
            // SQLite ignores schemas, so the names must differ from the shop's tables too.
            o.Schema = "billing";
            o.OutboxTable = "BillingOutbox";
            o.InboxTable = "BillingInbox";
        });
}
