namespace Twinbox.AzureFunctions;

public interface ITwinboxMaintenance
{
    /// <summary>Purges per <see cref="RetentionOptions"/> for every tenant, even when retention is disabled; returns rows removed.</summary>
    Task<int> RunCleanupAsync(CancellationToken cancellationToken);
}
