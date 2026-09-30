using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Twinbox.SourceGenerators;

/// <summary>Emits an AddHandlersFrom{Assembly}() builder extension that registers every IHandle implementation without reflection.</summary>
[Generator(LanguageNames.CSharp)]
public sealed class HandlerRegistrationGenerator : IIncrementalGenerator
{
    internal const string HandlersTrackingName = "Handlers";
    internal const string CompilationTrackingName = "Compilation";

    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var compilation = context.CompilationProvider
            .Select(static (c, _) => new CompilationModel(
                RegistrationEmitter.ToIdentifier(c.AssemblyName),
                ReferencesTwinbox(c)))
            .WithTrackingName(CompilationTrackingName);

        var handlers = context.SyntaxProvider
            .CreateSyntaxProvider(
                static (node, _) => node is ClassDeclarationSyntax { BaseList: not null } or RecordDeclarationSyntax { BaseList: not null },
                static (ctx, ct) => ToHandlerModel(ctx, ct))
            .Where(static h => h is not null)
            .Select(static (h, _) => h!)
            .WithTrackingName(HandlersTrackingName);

        context.RegisterSourceOutput(handlers.Collect().Combine(compilation), static (spc, source) => Emit(spc, source.Left, source.Right));
    }

    private static void Emit(SourceProductionContext context, ImmutableArray<HandlerModel> handlers, CompilationModel compilation)
    {
        if (!compilation.ShouldGenerate)
        {
            return;
        }

        // Partial classes surface once per declaration.
        var distinct = handlers
            .GroupBy(h => h.FullyQualifiedName)
            .Select(g => g.First())
            .ToList();

        foreach (var handler in distinct.Where(h => h.Kind is HandlerKind.Generic or HandlerKind.Abstract))
        {
            context.ReportDiagnostic(Diagnostic.Create(
                Diagnostics.HandlerNeedsManualRegistration,
                handler.Location?.ToLocation(),
                handler.DisplayName,
                handler.Kind == HandlerKind.Generic ? "generic" : "abstract"));
        }

        var registrations = distinct
            .Where(h => h.Kind == HandlerKind.Registrable)
            .SelectMany(h => h.MessageTypes.Select(m => (Handler: h.FullyQualifiedName, Message: m)))
            .OrderBy(r => r.Handler, StringComparer.Ordinal)
            .ThenBy(r => r.Message, StringComparer.Ordinal)
            .ToList();

        context.AddSource("TwinboxGeneratedRegistrations.g.cs", RegistrationEmitter.Emit(compilation.AssemblyIdentifier, registrations));
    }

    private static bool ReferencesTwinbox(Compilation compilation)
    {
        // Twinbox itself must not get a copy, or assemblies it grants internals to would see two.
        var builder = compilation.GetTypeByMetadataName("Twinbox.TwinboxBuilder");
        return builder is not null && !SymbolEqualityComparer.Default.Equals(builder.ContainingAssembly, compilation.Assembly);
    }

    private static HandlerModel? ToHandlerModel(GeneratorSyntaxContext context, CancellationToken cancellationToken)
    {
        if (context.SemanticModel.GetDeclaredSymbol(context.Node, cancellationToken) is not INamedTypeSymbol { TypeKind: TypeKind.Class } type)
        {
            return null;
        }

        var handled = type.AllInterfaces
            .Where(i => IsTwinboxInterface(i, "IHandle") || IsTwinboxInterface(i, "IHandleBatch"))
            .ToList();
        var messages = handled.Select(i => i.TypeArguments[0]).ToList();
        if (messages.Count == 0)
        {
            return null;
        }

        var kind = type.IsGenericType ? HandlerKind.Generic
            : type.IsAbstract ? HandlerKind.Abstract
            : !IsAccessible(type) || !messages.All(IsAccessible) ? HandlerKind.Inaccessible
            : HandlerKind.Registrable;

        return new HandlerModel(
            type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
            type.ToDisplayString(),
            kind,
            // Each entry is "{registration method}|{message type}" so batch handlers get AddBatchHandler.
            new EquatableArray<string>(handled
                .Select(i => (i.Name == "IHandleBatch" ? "AddBatchHandler" : "AddHandler") + "|" + i.TypeArguments[0].ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat))
                .ToArray()),
            LocationInfo.From(((BaseTypeDeclarationSyntax)context.Node).Identifier.GetLocation()));
    }

    private static bool IsTwinboxInterface(INamedTypeSymbol type, string name) =>
        type.Name == name && type is { Arity: 1, ContainingNamespace: { Name: "Twinbox", ContainingNamespace.IsGlobalNamespace: true } };

    private static bool IsAccessible(ITypeSymbol type) => type switch
    {
        IArrayTypeSymbol array => IsAccessible(array.ElementType),
        INamedTypeSymbol { TypeKind: not TypeKind.Error, IsFileLocal: false } named =>
            IsAccessibleChain(named) && named.TypeArguments.All(IsAccessible),
        _ => false,
    };

    private static bool IsAccessibleChain(INamedTypeSymbol type)
    {
        for (var current = type; current is not null; current = current.ContainingType)
        {
            if (current.DeclaredAccessibility is not (Accessibility.Public or Accessibility.Internal or Accessibility.ProtectedOrInternal))
            {
                return false;
            }
        }

        return true;
    }
}
