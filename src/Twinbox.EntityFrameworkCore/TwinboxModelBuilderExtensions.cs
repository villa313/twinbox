using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Twinbox;
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

        modelBuilder.SharedTypeEntity<OutboxMessage>(TwinboxEntities.Outbox, outbox =>
        {
            MapTable(outbox.Metadata, options.OutboxTable, options.Schema);

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

            // Oracle's provider maps unbounded binary and text to RAW(2000) and NVARCHAR2(2000), too small for payloads.
            if (HasProviderAnnotation(modelBuilder, "Oracle:"))
            {
                outbox.Property(m => m.Payload).HasColumnType("BLOB");
                outbox.Property(m => m.Headers).HasColumnType("NCLOB");
            }

            // Microseconds everywhere: some MySQL providers default to whole seconds, making messages look due early.
            outbox.Property(m => m.CreatedAt).HasPrecision(6);
            outbox.Property(m => m.AvailableAt).HasPrecision(6);
            outbox.Property(m => m.LeaseUntil).HasPrecision(6);
            outbox.Property(m => m.SentAt).HasPrecision(6);
        });

        modelBuilder.SharedTypeEntity<InboxRecord>(TwinboxEntities.Inbox, inbox =>
        {
            inbox.Property(r => r.ProcessedAt).HasPrecision(6);
            MapTable(inbox.Metadata, options.InboxTable, options.Schema);
            inbox.HasKey(r => new { r.MessageId, r.Consumer });
            inbox.HasIndex(r => r.ProcessedAt);
            inbox.Property(r => r.MessageId).HasMaxLength(256);
            inbox.Property(r => r.Consumer).HasMaxLength(256);
            inbox.Property(r => r.Source).HasMaxLength(256);
        });

        return modelBuilder;
    }

    private static bool HasProviderAnnotation(ModelBuilder modelBuilder, string prefix) =>
        modelBuilder.Model.GetAnnotations().Any(a => a.Name.StartsWith(prefix, StringComparison.Ordinal));

    // An explicit ToTable would stop naming conventions from rewriting the default name, so only set what was asked for.
    private static void MapTable(IMutableEntityType entityType, string? table, string? schema)
    {
        if (table is not null)
        {
            entityType.SetTableName(table);
        }

        if (schema is not null)
        {
            entityType.SetSchema(schema);
        }
    }
}
