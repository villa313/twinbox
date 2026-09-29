using System.Text.Json;
using System.Text.Json.Serialization;
using Twinbox.Storage;

namespace Twinbox.Serialization;

/// <summary>Encodes headers as a JSON object for stores that keep them in a single column.</summary>
public static class HeaderCodec
{
    public static string? Encode(IReadOnlyDictionary<string, string> headers)
    {
        ArgumentNullException.ThrowIfNull(headers);
        return headers.Count == 0
            ? null
            : JsonSerializer.Serialize(new Dictionary<string, string>(headers), HeaderJsonContext.Default.DictionaryStringString);
    }

    public static IReadOnlyDictionary<string, string> Decode(string? json) =>
        string.IsNullOrEmpty(json)
            ? EmptyHeaders.Instance
            : JsonSerializer.Deserialize(json, HeaderJsonContext.Default.DictionaryStringString) ?? EmptyHeaders.Instance;
}

[JsonSerializable(typeof(Dictionary<string, string>))]
internal sealed partial class HeaderJsonContext : JsonSerializerContext;
