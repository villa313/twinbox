using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Twinbox.EntityFrameworkCore.IntegrationTests;

/// <summary>
/// Apps generate migrations from Twinbox's model, so any change to its tables is a schema change for them. A failure here
/// means the release notes must say so; rerun with TWINBOX_UPDATE_MODEL_SNAPSHOTS=1 to accept the new shape.
/// </summary>
public sealed class ModelSnapshotTests
{
    public static TheoryData<string> Providers => ["SqlServer", "PostgreSql", "MySql", "Sqlite", "Oracle"];

    [Theory]
    [MemberData(nameof(Providers))]
    public void TwinboxTables_MatchTheSnapshot(string provider)
    {
        var actual = Describe(ModelFor(provider));
        var path = SnapshotPath(provider);

        if (Environment.GetEnvironmentVariable("TWINBOX_UPDATE_MODEL_SNAPSHOTS") == "1")
        {
            File.WriteAllText(path, actual);
        }

        Assert.True(File.Exists(path), $"No snapshot at {path}; run with TWINBOX_UPDATE_MODEL_SNAPSHOTS=1 to create it.");
        Assert.Equal(File.ReadAllText(path).ReplaceLineEndings(), actual.ReplaceLineEndings());
    }

    private static IModel ModelFor(string provider)
    {
        var options = new DbContextOptionsBuilder<ShopContext>();
        _ = provider switch
        {
            "SqlServer" => options.UseSqlServer("Server=localhost;Database=twinbox"),
            "PostgreSql" => options.UseNpgsql("Host=localhost;Database=twinbox"),
            "MySql" => options.UseMySQL("server=localhost;database=twinbox;user=twinbox;password=twinbox"),
            "Sqlite" => options.UseSqlite("Data Source=:memory:"),
            "Oracle" => options.UseOracle("User Id=twinbox;Password=twinbox;Data Source=localhost/FREEPDB1"),
            _ => throw new ArgumentOutOfRangeException(nameof(provider), provider, null),
        };
        using var context = new ShopContext(options.Options);
        return context.GetService<IDesignTimeModel>().Model;
    }

    private static string Describe(IModel model)
    {
        var text = new StringBuilder();
        foreach (var table in model.GetRelationalModel().Tables
            .Where(t => t.EntityTypeMappings.Any(m => m.TypeBase.Name is TwinboxEntities.Outbox or TwinboxEntities.Inbox))
            .OrderBy(t => t.Name, StringComparer.Ordinal))
        {
            text.AppendLine(CultureInfo.InvariantCulture, $"table {table.SchemaQualifiedName}");
            foreach (var column in table.Columns.OrderBy(c => c.Name, StringComparer.Ordinal))
            {
                text.AppendLine(CultureInfo.InvariantCulture, $"  column {column.Name} {column.StoreType}{(column.IsNullable ? " null" : string.Empty)}");
            }

            text.AppendLine(CultureInfo.InvariantCulture, $"  key {table.PrimaryKey!.Name} ({string.Join(", ", table.PrimaryKey.Columns.Select(c => c.Name))})");
            foreach (var index in table.Indexes.OrderBy(i => i.Name, StringComparer.Ordinal))
            {
                text.AppendLine(CultureInfo.InvariantCulture, $"  index {index.Name} ({string.Join(", ", index.Columns.Select(c => c.Name))}){(index.IsUnique ? " unique" : string.Empty)}");
            }
        }

        return text.ToString();
    }

    private static string SnapshotPath(string provider, [CallerFilePath] string thisFile = "") =>
        Path.Combine(Path.GetDirectoryName(thisFile)!, "ModelSnapshots", $"{provider}.txt");
}
