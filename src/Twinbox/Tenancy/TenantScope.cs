namespace Twinbox.Tenancy;

/// <summary>Carries the tenant Twinbox is working for across the async flow of a dispatch pass or inbound message.</summary>
internal static class TenantScope
{
    private static readonly AsyncLocal<string?> CurrentTenant = new();

    public static string? Current => CurrentTenant.Value;

    public static Restore Enter(string? tenant)
    {
        var previous = CurrentTenant.Value;
        CurrentTenant.Value = tenant;
        return new Restore(previous);
    }

    internal readonly struct Restore(string? previous) : IDisposable
    {
        public void Dispose() => CurrentTenant.Value = previous;
    }
}
