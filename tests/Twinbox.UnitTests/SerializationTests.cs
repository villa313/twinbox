using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Twinbox.Serialization;
using Twinbox.Storage;

namespace Twinbox.UnitTests;

public sealed class SerializationTests
{
    [Fact]
    public void HeaderCodec_RoundTrips()
    {
        var headers = new Dictionary<string, string> { ["a"] = "1", ["b"] = "" };

        var decoded = HeaderCodec.Decode(HeaderCodec.Encode(headers));

        Assert.Equal(headers, decoded);
    }

    [Fact]
    public void HeaderCodec_EmptyHeaders_EncodeToNull()
    {
        Assert.Null(HeaderCodec.Encode(new Dictionary<string, string>()));
        Assert.Empty(HeaderCodec.Decode(null));
    }

    [Fact]
    public void JsonSerializer_RoundTripsWithCamelCase()
    {
        var serializer = new SystemTextJsonMessageSerializer();

        var bytes = serializer.Serialize(new OrderPlaced(12), typeof(OrderPlaced));

        Assert.Equal("""{"orderId":12}""", System.Text.Encoding.UTF8.GetString(bytes));
        Assert.Equal(new OrderPlaced(12), serializer.Deserialize(bytes, typeof(OrderPlaced)));
    }

    [Fact]
    public void UseJsonTypeInfoResolver_UsesOnlyTheResolverWithCamelCase()
    {
        using var services = new ServiceCollection()
            .AddTwinbox(b => b.UseJsonTypeInfoResolver(SerializationTestsJsonContext.Default))
            .BuildServiceProvider();
        var serializer = services.GetRequiredService<IMessageSerializer>();

        var bytes = serializer.Serialize(new OrderPlaced(12), typeof(OrderPlaced));

        Assert.Equal("""{"orderId":12}""", System.Text.Encoding.UTF8.GetString(bytes));
        Assert.Equal(new OrderPlaced(12), serializer.Deserialize(bytes, typeof(OrderPlaced)));
        Assert.Throws<NotSupportedException>(() => serializer.Serialize(new OrderShipped(1), typeof(OrderShipped)));
    }

    [Fact]
    public void Uuid7_IsVersion7AndTimeOrdered()
    {
        var generator = new Uuid7MessageIdGenerator();
        var t0 = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

        var first = generator.NewId(t0);
        var second = generator.NewId(t0.AddMilliseconds(1));

        Assert.Equal('7', first.ToString()[14]);
        Assert.True(string.CompareOrdinal(first.ToString(), second.ToString()) < 0);
    }
}

[JsonSerializable(typeof(OrderPlaced))]
internal sealed partial class SerializationTestsJsonContext : JsonSerializerContext;
