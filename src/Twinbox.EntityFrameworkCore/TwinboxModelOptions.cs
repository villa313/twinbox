namespace Twinbox.EntityFrameworkCore;

public sealed class TwinboxModelOptions
{
    public string? Schema { get; set; }

    public string OutboxTable { get; set; } = "TwinboxOutbox";

    public string InboxTable { get; set; } = "TwinboxInbox";
}
