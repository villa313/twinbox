using Microsoft.EntityFrameworkCore;

namespace TwinboxApp.Data;

public sealed class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
    public DbSet<Order> Orders => Set<Order>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>().Property(order => order.Total).HasPrecision(18, 2);

        // Maps the outbox and inbox tables into this context, so they commit with your own data.
        modelBuilder.AddTwinbox();
    }
}
