using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Twinbox.EntityFrameworkCore;
using Twinbox.Serialization;
using Twinbox.Storage;

namespace Microsoft.EntityFrameworkCore;

public static class TwinboxModelBuilderExtensions
{
    internal const string SequenceProperty = "Sequence";

    /// <summary>Adds the outbox and inbox tables to the model, so they ship in the app's own migrations.</summary>
    public static ModelBuilder AddTwinbox(this ModelBuilder modelBuilder, Action<TwinboxModelOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);
        var options = new TwinboxModelOptions();
        configure?.Invoke(options);

        modelBuilder.Entity<OutboxMessage>(outbox =>
        {
            outbox.ToTable(options.OutboxTable, options.Schema);

            // A sequential key gives insertion order for partitions and an append-only clustered index.
            outbox.Property<long>(SequenceProperty).ValueGeneratedOnAdd();
            outbox.HasKey(SequenceProperty);
            outbox.HasIndex(m => m.Id).IsUnique();
            outbox.HasIndex(m => new { m.Status, m.AvailableAt });
            outbox.HasIndex(m => new { m.PartitionKey, m.Status });

            outbox.Property(m => m.Id).ValueGeneratedNever();
            outbox.Property(m => m.MessageName).HasMaxLength(256);
            outbox.Property(m => m.Transport).HasMaxLength(64);
            outbox.Property(m => m.Destination).HasMaxLength(256);
            outbox.Property(m => m.PartitionKey).HasMaxLength(256);
            outbox.Property(m => m.TenantId).HasMaxLength(128);
            outbox.Property(m => m.ContentType).HasMaxLength(128);
            outbox.Property(m => m.TraceParent).HasMaxLength(64);
            outbox.Property(m => m.LeaseOwner).HasMaxLength(256);
            outbox.Property(m => m.LastError).HasMaxLength(2000);
            outbox.Property(m => m.Status).HasConversion<int>();
            outbox.Property(m => m.Headers)
                .HasConversion(new ValueConverter<IReadOnlyDictionary<string, string>, string>(
                    headers => HeaderCodec.Encode(headers) ?? "{}",
                    json => HeaderCodec.Decode(json)))
                .Metadata.SetValueComparer(new ValueComparer<IReadOnlyDictionary<string, string>>(
                    (a, b) => ReferenceEquals(a, b),
                    headers => headers.Count,
                    headers => headers));
        });

        modelBuilder.Entity<InboxRecord>(inbox =>
        {
            inbox.ToTable(options.InboxTable, options.Schema);
            inbox.HasKey(r => new { r.MessageId, r.Consumer });
            inbox.HasIndex(r => r.ProcessedAt);
            inbox.Property(r => r.MessageId).HasMaxLength(256);
            inbox.Property(r => r.Consumer).HasMaxLength(256);
            inbox.Property(r => r.Source).HasMaxLength(256);
        });

        return modelBuilder;
    }
}
