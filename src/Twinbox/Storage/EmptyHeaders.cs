using System.Collections.ObjectModel;

namespace Twinbox.Storage;

internal static class EmptyHeaders
{
    public static readonly IReadOnlyDictionary<string, string> Instance =
        new ReadOnlyDictionary<string, string>(new Dictionary<string, string>());
}
