using System.Collections.Immutable;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace Twinbox.SourceGenerators.Tests;

internal static class GeneratorHarness
{
    private static readonly Lazy<ImmutableArray<MetadataReference>> AllReferences = new(LoadReferences);

    public static readonly CSharpParseOptions ParseOptions = new(LanguageVersion.CSharp12);

    public static GeneratorResult Run(string source, string assemblyName = "MyApp", bool referenceTwinbox = true)
    {
        var compilation = CreateCompilation(source, assemblyName, referenceTwinbox);
        var driver = CreateDriver().RunGeneratorsAndUpdateCompilation(compilation, out var output, out var diagnostics);
        var generated = driver.GetRunResult().GeneratedTrees.SingleOrDefault()?.ToString();
        return new GeneratorResult(generated, diagnostics, output);
    }

    public static CSharpCompilation CreateCompilation(string source, string assemblyName = "MyApp", bool referenceTwinbox = true)
    {
        var references = referenceTwinbox
            ? AllReferences.Value
            : AllReferences.Value.RemoveAll(r => Path.GetFileName(r.Display) is "Twinbox.dll");
        return CSharpCompilation.Create(
            assemblyName,
            [CSharpSyntaxTree.ParseText(source, ParseOptions, path: "Handlers.cs")],
            references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary, nullableContextOptions: NullableContextOptions.Enable));
    }

    public static GeneratorDriver CreateDriver() =>
        CSharpGeneratorDriver.Create(
            [new HandlerRegistrationGenerator().AsSourceGenerator()],
            parseOptions: ParseOptions,
            driverOptions: new GeneratorDriverOptions(IncrementalGeneratorOutputKind.None, trackIncrementalGeneratorSteps: true));

    private static ImmutableArray<MetadataReference> LoadReferences()
    {
        // The test host's trusted assemblies include the BCL plus Twinbox and its dependencies.
        var paths = ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!).Split(Path.PathSeparator);
        return [.. paths.Select(p => (MetadataReference)MetadataReference.CreateFromFile(p))];
    }
}

internal sealed record GeneratorResult(string? Source, ImmutableArray<Diagnostic> Diagnostics, Compilation Output);
