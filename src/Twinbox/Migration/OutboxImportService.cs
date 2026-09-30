using System.Data.Common;
using System.Globalization;
using System.Text;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Twinbox.Messaging;
using Twinbox.Serialization;
using Twinbox.Storage;
using Twinbox.Tenancy;

namespace Twinbox.Migration;

internal sealed partial class OutboxImportService(
    OutboxImportOptions options,
    MessageTypeRegistry registry,
    MessagePreparer preparer,
    IEnumerable<IOutboxStore> stores,
    TenantDirectory tenants,
    TwinboxScopeFactory scopes,
    IDispatchSignal signal,
    TimeProvider time,
    ILogger<OutboxImportService> logger) : BackgroundService
{
    private readonly ILogger _logger = logger;
    private readonly HashSet<string> _reportedUnknownNames = new(StringComparer.Ordinal);
    private readonly IOutboxStore _store = OutboxStoreSelector.Select(stores, options.Store, "ImportFromExistingOutbox");

    /// <summary>Imports one batch per tenant; returns how many rows were imported.</summary>
    internal async Task<int> ImportBatchAsync(CancellationToken cancellationToken)
    {
        var imported = 0;
        foreach (var tenant in await tenants.GetTenantsAsync(cancellationToken).ConfigureAwait(false))
        {
            using var _ = TenantScope.Enter(tenant);
            imported += await ImportTenantBatchAsync(tenant, cancellationToken).ConfigureAwait(false);
        }

        if (imported > 0)
        {
            LogImported(imported);
            signal.Notify();
        }

        return imported;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(options.PollInterval, time);
        do
        {
            try
            {
                while (await ImportBatchAsync(stoppingToken).ConfigureAwait(false) >= options.BatchSize)
                {
                }
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                LogImportFailed(ex);
            }
        }
        while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false));
    }

    private async Task<int> ImportTenantBatchAsync(string? tenant, CancellationToken cancellationToken)
    {
        var scope = scopes.CreateAsyncScope();
        await using (scope.ConfigureAwait(false))
        {
            await using var connection = options.CreateConnection!(scope.ServiceProvider);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

            var imported = 0;
            foreach (var row in await SelectAsync(connection, cancellationToken).ConfigureAwait(false))
            {
                if (!registry.TryResolve(row.Name, out var messageType))
                {
                    ReportUnknown(row.Name);
                    continue;
                }

                try
                {
                    var messages = preparer.PrepareImported(
                        messageType, row.Name, row.Id, row.Content, tenant, HeaderCodec.Decode(row.Headers), row.PartitionKey);
                    await AppendMissingAsync(messages, cancellationToken).ConfigureAwait(false);
                    await MarkImportedAsync(connection, row.Key, cancellationToken).ConfigureAwait(false);
                    imported++;
                }
                catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
                {
                    LogRowFailed(ex, row.Id);
                }
            }

            return imported;
        }
    }

    /// <summary>Ids are deterministic, so rows stored before a crash stopped the old row being marked are found and skipped.</summary>
    private async Task AppendMissingAsync(IReadOnlyList<OutboxMessage> messages, CancellationToken cancellationToken)
    {
        if (_store is IOutboxAdmin admin)
        {
            var missing = new List<OutboxMessage>(messages.Count);
            foreach (var message in messages)
            {
                if (await admin.GetAsync(message.Id, cancellationToken).ConfigureAwait(false) is null)
                {
                    missing.Add(message);
                }
            }

            messages = missing;
        }

        if (messages.Count > 0)
        {
            await _store.AppendAsync(messages, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task<List<ImportedRow>> SelectAsync(DbConnection connection, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = options.SelectPending;
        AddParameter(command, "batch", options.BatchSize);

        var rows = new List<ImportedRow>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        var columns = Columns(reader);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var key = reader.GetValue(columns["Id"]);
            var content = reader.GetValue(columns["Content"]);
            rows.Add(new ImportedRow(
                key,
                key is byte[] raw ? Convert.ToHexString(raw) : Convert.ToString(key, CultureInfo.InvariantCulture)!,
                reader.GetString(columns["Name"]),
                content as byte[] ?? Encoding.UTF8.GetBytes(Convert.ToString(content, CultureInfo.InvariantCulture)!),
                OptionalText(reader, columns, "Headers"),
                OptionalText(reader, columns, "PartitionKey")));
        }

        return rows;
    }

    private async Task MarkImportedAsync(DbConnection connection, object key, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = options.MarkImported;
        AddParameter(command, "id", key);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private void ReportUnknown(string name)
    {
        if (_reportedUnknownNames.Add(name))
        {
            LogUnknownName(name);
        }
    }

    private static Dictionary<string, int> Columns(DbDataReader reader)
    {
        var columns = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < reader.FieldCount; i++)
        {
            columns.TryAdd(reader.GetName(i), i);
        }

        foreach (var required in (string[])["Id", "Name", "Content"])
        {
            if (!columns.ContainsKey(required))
            {
                throw new InvalidOperationException($"ImportFromExistingOutbox's SelectPending must return a column named {required}.");
            }
        }

        return columns;
    }

    private static string? OptionalText(DbDataReader reader, Dictionary<string, int> columns, string column) =>
        columns.TryGetValue(column, out var ordinal) && !reader.IsDBNull(ordinal)
            ? Convert.ToString(reader.GetValue(ordinal), CultureInfo.InvariantCulture)
            : null;

    /// <summary>The key goes back with the type it was read as (a PostgreSQL uuid stays a Guid); ODP.NET wants names without '@'.</summary>
    private static void AddParameter(DbCommand command, string name, object value)
    {
        var parameter = command.CreateParameter();
        var oracle = command.GetType().FullName?.StartsWith("Oracle.", StringComparison.Ordinal) == true;
        parameter.ParameterName = oracle ? name : "@" + name;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Imported {Count} message(s) from the existing outbox.")]
    private partial void LogImported(int count);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Skipping existing outbox messages named '{Name}': no message type is registered with that name. Add [MessageName(\"{Name}\")] to the matching type.")]
    private partial void LogUnknownName(string name);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Couldn't import existing outbox row {Id}; it is retried on the next poll.")]
    private partial void LogRowFailed(Exception error, string id);

    [LoggerMessage(Level = LogLevel.Error, Message = "Importing from the existing outbox failed; will retry.")]
    private partial void LogImportFailed(Exception error);

    private sealed record ImportedRow(object Key, string Id, string Name, byte[] Content, string? Headers, string? PartitionKey);
}
