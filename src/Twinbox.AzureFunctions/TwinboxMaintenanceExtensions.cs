namespace Twinbox;

public static class TwinboxMaintenanceExtensions
{
    /// <summary>Leaves headroom under the Consumption plan's shortest function timeout.</summary>
    public static readonly TimeSpan DefaultDispatchBudget = TimeSpan.FromSeconds(50);

    /// <summary>Dispatches until the outbox is empty or <see cref="DefaultDispatchBudget"/> is used up; returns messages claimed.</summary>
    public static Task<int> DispatchPendingAsync(this ITwinboxMaintenance maintenance, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(maintenance);
        return maintenance.DispatchPendingAsync(DefaultDispatchBudget, cancellationToken);
    }
}
