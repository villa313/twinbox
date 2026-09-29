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
}
