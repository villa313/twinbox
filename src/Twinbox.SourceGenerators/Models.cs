using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Text;

namespace Twinbox.SourceGenerators;

internal enum HandlerKind
{
    Registrable,
    Generic,
    Abstract,
    Inaccessible,
}

internal sealed record HandlerModel(
    string FullyQualifiedName,
    string DisplayName,
    HandlerKind Kind,
    EquatableArray<string> MessageTypes,
    LocationInfo? Location);

internal sealed record CompilationModel(string AssemblyIdentifier, bool ShouldGenerate);

/// <summary>Location without the syntax tree reference, which would defeat pipeline caching.</summary>
internal sealed record LocationInfo(string FilePath, TextSpan Span, LinePositionSpan LineSpan)
{
    public static LocationInfo? From(Location location) =>
        location.SourceTree is null ? null : new(location.SourceTree.FilePath, location.SourceSpan, location.GetLineSpan().Span);

    public Location ToLocation() => Location.Create(FilePath, Span, LineSpan);
}
