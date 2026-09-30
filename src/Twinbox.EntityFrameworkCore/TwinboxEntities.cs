using Microsoft.EntityFrameworkCore;

namespace Twinbox.EntityFrameworkCore;

/// <summary>Entity names for Twinbox's tables: stable strings, so moving a type inside Twinbox never changes your migrations.</summary>
public static class TwinboxEntities
{
    public const string Outbox = "TwinboxOutbox";

    public const string Inbox = "TwinboxInbox";

    internal static DbSet<InboxRecord> TwinboxInbox(this DbContext context) => context.Set<InboxRecord>(Inbox);
}
