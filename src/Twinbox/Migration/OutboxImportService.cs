using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Twinbox.Messaging;
using Twinbox.Storage;

namespace Twinbox.Migration;

internal sealed partial class OutboxImportService(
    OutboxImportOptions options,
    MessageTypeRegistry registry,
    MessagePreparer preparer,
    IOutboxStore store,
    IDispatchSignal signal,
    IServiceScopeFactory scopes,
    TimeProvider time,
    ILogger<OutboxImportService> logger) : BackgroundService
{
    private readonly ILogger _logger = logger;
    private readonly HashSet<string> _reportedUnknownNames = new(StringComparer.Ordinal);

    internal async Task<int> ImportBatchAsync(CancellationToken cancellationToken)
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
                    var messages = preparer.PrepareImported(messageType, row.Name, row.Id, row.Content);
                    await store.AppendAsync(messages, cancellationToken).ConfigureAwait(false);
                    await MarkImportedAsync(connection, row.Id, cancellationToken).ConfigureAwait(false);
                    imported++;
                }
                catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
                {
                    // Most likely imported before a crash stopped it being marked; the deterministic id rejects the repeat.
                    LogRowFailed(ex, row.Id);
                }
            }

            if (imported > 0)
            {
                LogImported(imported);
                signal.Notify();
            }

            return imported;
        }
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

    private async Task<List<(string Id, string Name, byte[] Content)>> SelectAsync(DbConnection connection, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = options.SelectPending;
        AddParameter(command, "@batch", options.BatchSize, DbType.Int32);

        var rows = new List<(string, string, byte[])>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var content = reader.GetValue(reader.GetOrdinal("Content"));
            rows.Add((
                Convert.ToString(reader.GetValue(reader.GetOrdinal("Id")), CultureInfo.InvariantCulture)!,
                reader.GetString(reader.GetOrdinal("Name")),
                content as byte[] ?? Encoding.UTF8.GetBytes(Convert.ToString(content, CultureInfo.InvariantCulture)!)));
        }

        return rows;
    }

    private async Task MarkImportedAsync(DbConnection connection, string id, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = options.MarkImported;
        AddParameter(command, "@id", id, DbType.String);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private void ReportUnknown(string name)
    {
        if (_reportedUnknownNames.Add(name))
        {
            LogUnknownName(name);
        }
    }

    private static void AddParameter(DbCommand command, string name, object value, DbType type)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.DbType = type;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Imported {Count} message(s) from the existing outbox.")]
    private partial void LogImported(int count);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Skipping existing outbox messages named '{Name}': no message type is registered with that name. Add [MessageName(\"{Name}\")] to the matching type.")]
    private partial void LogUnknownName(string name);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Couldn't import existing outbox row {Id}; if it was already imported, mark it by hand.")]
    private partial void LogRowFailed(Exception error, string id);

    [LoggerMessage(Level = LogLevel.Error, Message = "Importing from the existing outbox failed; will retry.")]
    private partial void LogImportFailed(Exception error);
}
