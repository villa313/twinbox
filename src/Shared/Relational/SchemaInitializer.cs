using System.Collections.Concurrent;
using System.Data.Common;

namespace Twinbox.Relational;

/// <summary>Creates the tables once per database, so a tenant's database is prepared on first use.</summary>
internal sealed class SchemaInitializer(RelationalSettings settings, RelationalDialect dialect)
{
    private readonly ConcurrentDictionary<string, Lazy<Task>> _prepared = new(StringComparer.Ordinal);

    public async Task EnsureAsync(DbConnection connection, CancellationToken cancellationToken)
    {
        if (!settings.CreateSchemaIfMissing)
        {
            return;
        }

        var key = $"{connection.DataSource}/{connection.Database}";
        var creation = _prepared.GetOrAdd(key, _ => new Lazy<Task>(() => CreateAsync(connection, cancellationToken)));
        try
        {
            await creation.Value.ConfigureAwait(false);
        }
        catch
        {
            _prepared.TryRemove(key, out _);
            throw;
        }
    }

    private async Task CreateAsync(DbConnection connection, CancellationToken cancellationToken)
    {
        await using var command = connection.Command(dialect.CreateSchema());
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
