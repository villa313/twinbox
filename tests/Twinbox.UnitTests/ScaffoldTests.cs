namespace Twinbox.UnitTests;

public sealed class ScaffoldTests
{
    [Fact]
    public void CoreAssembly_IsLoadable() =>
        Assert.Equal("Twinbox", typeof(Twinbox.TwinboxMarker).Assembly.GetName().Name);
}
