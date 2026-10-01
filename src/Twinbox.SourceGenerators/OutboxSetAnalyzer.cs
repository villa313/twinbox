using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

namespace Twinbox.SourceGenerators;

/// <summary>EF Core's own error for this ("shared-type entity") doesn't say which method to call instead.</summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class OutboxSetAnalyzer : DiagnosticAnalyzer
{
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } = ImmutableArray.Create(Diagnostics.UseTwinboxOutbox);

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterCompilationStartAction(start =>
        {
            var outboxMessage = start.Compilation.GetTypeByMetadataName("Twinbox.OutboxMessage");
            var dbContext = start.Compilation.GetTypeByMetadataName("Microsoft.EntityFrameworkCore.DbContext");
            if (outboxMessage is not null && dbContext is not null)
            {
                start.RegisterOperationAction(operation => Analyze(operation, outboxMessage, dbContext), OperationKind.Invocation);
            }
        });
    }

    private static void Analyze(OperationAnalysisContext context, INamedTypeSymbol outboxMessage, INamedTypeSymbol dbContext)
    {
        var method = ((IInvocationOperation)context.Operation).TargetMethod;
        if (method.Name == "Set"
            && method.Parameters.Length == 0
            && method.TypeArguments.Length == 1
            && SymbolEqualityComparer.Default.Equals(method.TypeArguments[0], outboxMessage)
            && DerivesFrom(method.ContainingType, dbContext))
        {
            context.ReportDiagnostic(Diagnostic.Create(Diagnostics.UseTwinboxOutbox, context.Operation.Syntax.GetLocation()));
        }
    }

    private static bool DerivesFrom(INamedTypeSymbol? type, INamedTypeSymbol baseType)
    {
        for (; type is not null; type = type.BaseType)
        {
            if (SymbolEqualityComparer.Default.Equals(type, baseType))
            {
                return true;
            }
        }

        return false;
    }
}
