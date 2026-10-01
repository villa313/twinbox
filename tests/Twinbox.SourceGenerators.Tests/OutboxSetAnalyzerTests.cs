using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;

namespace Twinbox.SourceGenerators.Tests;

public sealed class OutboxSetAnalyzerTests
{
    // Stands in for EF Core, which this test project doesn't reference.
    private const string EfCore = """
        namespace Microsoft.EntityFrameworkCore
        {
            public class DbSet<T> { }
            public class DbContext
            {
                public virtual DbSet<T> Set<T>() where T : class => new();
                public virtual DbSet<T> Set<T>(string name) where T : class => new();
            }
        }
        """;

    [Fact]
    public async Task SetOfOutboxMessage_OnADbContext_IsReported()
    {
        var diagnostics = await AnalyzeAsync("""
            class Shop : Microsoft.EntityFrameworkCore.DbContext { }
            class Query { object Run(Shop shop) => shop.Set<Twinbox.OutboxMessage>(); }
            """);

        Assert.Equal("TWBX002", Assert.Single(diagnostics).Id);
    }

    [Fact]
    public async Task NamedSetAndOtherEntities_AreNotReported()
    {
        var diagnostics = await AnalyzeAsync("""
            class Order { }
            class Shop : Microsoft.EntityFrameworkCore.DbContext { }
            class Query
            {
                object A(Shop shop) => shop.Set<Order>();
                object B(Shop shop) => shop.Set<Twinbox.OutboxMessage>("TwinboxOutbox");
            }
            """);

        Assert.Empty(diagnostics);
    }

    private static async Task<ImmutableArray<Diagnostic>> AnalyzeAsync(string source)
    {
        var compilation = GeneratorHarness.CreateCompilation(source + EfCore);
        return await compilation
            .WithAnalyzers([new OutboxSetAnalyzer()])
            .GetAnalyzerDiagnosticsAsync();
    }
}
