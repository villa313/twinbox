using Microsoft.CodeAnalysis;

namespace Twinbox.SourceGenerators;

internal static class Diagnostics
{
    public static readonly DiagnosticDescriptor HandlerNeedsManualRegistration = new(
        id: "TWBX001",
        title: "Handler must be registered manually",
        messageFormat: "'{0}' is {1}, so the generated AddHandlersFrom method skips it; register its concrete types with AddHandler<THandler, TMessage>()",
        category: "Twinbox",
        defaultSeverity: DiagnosticSeverity.Info,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor UseTwinboxOutbox = new(
        id: "TWBX002",
        title: "Query the outbox with TwinboxOutbox()",
        messageFormat: "Twinbox maps OutboxMessage as a shared-type entity, so Set<OutboxMessage>() throws at runtime; use TwinboxOutbox() instead",
        category: "Twinbox",
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true);
}
