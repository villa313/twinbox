using System.Security.Cryptography;
using System.Text;

namespace Twinbox.Migration;

internal static class ImportedMessageIds
{
    /// <summary>Deterministic, so importing the same row twice yields the same id and the inbox drops the repeat.</summary>
    public static Guid For(string legacyId, string destination)
    {
        Span<byte> hash = stackalloc byte[32];
        SHA256.HashData(Encoding.UTF8.GetBytes($"twinbox-import:{legacyId}:{destination}"), hash);
        var bytes = hash[..16];
        bytes[6] = (byte)((bytes[6] & 0x0F) | 0x80);
        bytes[8] = (byte)((bytes[8] & 0x3F) | 0x80);
        return new Guid(bytes, bigEndian: true);
    }
}
