using Microsoft.EntityFrameworkCore;
using Twinbox.Storage;

namespace Twinbox.EntityFrameworkCore;

/// <summary>
/// Entity names Twinbox maps its tables under. They're stable strings rather than CLR type names, so moving a type
/// inside Twinbox never shows up as a table change in your migrations.
/// </summary>
public static class TwinboxEntities
{
    public const string Outbox = "TwinboxOutbox";

    public const string Inbox = "TwinboxInbox";

    /// <summary>The outbox table, for inspecting or cleaning up messages directly.</summary>
    public static DbSet<OutboxMessage> TwinboxOutbox(this DbContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        return context.Set<OutboxMessage>(Outbox);
    }

    internal static DbSet<InboxRecord> TwinboxInbox(this DbContext context) => context.Set<InboxRecord>(Inbox);
}
