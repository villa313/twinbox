using Twinbox.InMemory;
using Twinbox.Storage;
using Twinbox.Testing.Conformance;

namespace Twinbox.UnitTests;

public sealed class OutboxStoreConformanceTests
{
    public static TheoryData<ConformanceCase<IOutboxStore>> Cases => [.. OutboxStoreConformance.Cases];

    [Theory]
    [MemberData(nameof(Cases))]
    public Task InMemoryStore_MeetsStorageContract(ConformanceCase<IOutboxStore> conformanceCase) =>
        conformanceCase.RunAsync(new InMemoryOutboxStore());
}
