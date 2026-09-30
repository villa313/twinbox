using Twinbox;
using Twinbox.EntityFrameworkCore;
using Twinbox.Storage;

namespace Microsoft.EntityFrameworkCore;

public static class TwinboxDbContextExtensions
{
    /// <summary>Makes a context you created yourself save the messages sent through <paramref name="outbox"/>; DI-resolved contexts are enlisted already.</summary>
    public static TContext EnlistOutbox<TContext>(this TContext context, IOutbox outbox)
        where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(context);
        var session = outbox as IOutboxSession
            ?? throw new ArgumentException("Pass the IOutbox resolved from the container.", nameof(outbox));
        OutboxEnlistment.Enlist(context, session);

        // A context built by hand may lack Twinbox's interceptor, so hook its own events; flushing twice is harmless.
        context.SavingChanges += (_, _) => OutboxInterceptor.Flush(context);
        context.SavedChanges += (_, _) => OutboxInterceptor.NotifyIfCommitted(context);
        return context;
    }

    /// <summary>The outbox table, for inspecting or cleaning up messages directly.</summary>
    public static DbSet<OutboxMessage> TwinboxOutbox(this DbContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        return context.Set<OutboxMessage>(TwinboxEntities.Outbox);
    }
}
