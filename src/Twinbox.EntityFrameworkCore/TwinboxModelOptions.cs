namespace Twinbox.EntityFrameworkCore;

public sealed class TwinboxModelOptions
{
    public string? Schema { get; set; }

    /// <summary>Null keeps the default name, which your naming conventions (e.g. snake_case) still apply to.</summary>
    public string? OutboxTable { get; set; }

    /// <summary>Null keeps the default name, which your naming conventions (e.g. snake_case) still apply to.</summary>
    public string? InboxTable { get; set; }
}
