using Twinbox.Messaging;

namespace Twinbox.UnitTests;

public sealed class MessageTypeRegistryTests
{
    [Fact]
    public void GetOrAdd_WithAttribute_UsesDeclaredName()
    {
        var registry = new MessageTypeRegistry();

        Assert.Equal("invoice-issued.v1", registry.GetOrAdd(typeof(InvoiceIssued)));
        Assert.True(registry.TryResolve("invoice-issued.v1", out var type));
        Assert.Equal(typeof(InvoiceIssued), type);
    }

    [Fact]
    public void GetOrAdd_WithoutAttribute_UsesTypeName()
    {
        Assert.Equal("OrderPlaced", new MessageTypeRegistry().GetOrAdd(typeof(OrderPlaced)));
    }

    [Fact]
    public void GetOrAdd_TwoTypesWithSameName_Throws()
    {
        var registry = new MessageTypeRegistry();
        registry.GetOrAdd(typeof(OrderPlaced));

        var error = Assert.Throws<InvalidOperationException>(() => registry.GetOrAdd(typeof(Other.OrderPlaced)));
        Assert.Contains("OrderPlaced", error.Message, StringComparison.Ordinal);
    }
}
