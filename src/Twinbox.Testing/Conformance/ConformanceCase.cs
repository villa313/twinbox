namespace Twinbox.Testing.Conformance;

/// <summary>One storage-contract check. Each run receives a fresh, empty store.</summary>
public sealed record ConformanceCase<TStore>(string Name, Func<TStore, Task> RunAsync)
{
    public override string ToString() => Name;
}

public sealed class ConformanceException(string message) : Exception(message);
