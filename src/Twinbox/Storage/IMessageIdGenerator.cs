using System.Security.Cryptography;

namespace Twinbox.Storage;

/// <summary>Stores can swap this for an index-friendly layout, e.g. SQL Server's byte ordering.</summary>
public interface IMessageIdGenerator
{
    Guid NewId(DateTimeOffset now);
}

public sealed class Uuid7MessageIdGenerator : IMessageIdGenerator
{
    public Guid NewId(DateTimeOffset now)
    {
#if NET9_0_OR_GREATER
        return Guid.CreateVersion7(now);
#else
        Span<byte> bytes = stackalloc byte[16];
        RandomNumberGenerator.Fill(bytes);
        var milliseconds = now.ToUnixTimeMilliseconds();
        for (var i = 0; i < 6; i++)
        {
            bytes[i] = (byte)(milliseconds >> (8 * (5 - i)));
        }

        bytes[6] = (byte)((bytes[6] & 0x0F) | 0x70);
        bytes[8] = (byte)((bytes[8] & 0x3F) | 0x80);
        return new Guid(bytes, bigEndian: true);
#endif
    }
}
