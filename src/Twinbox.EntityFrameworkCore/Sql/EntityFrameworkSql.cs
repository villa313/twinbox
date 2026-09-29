using System.Runtime.CompilerServices;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Twinbox.Sql;

namespace Twinbox.EntityFrameworkCore.Sql;

/// <summary>Builds the shared statements from the EF model, so naming conventions and custom table names are honoured.</summary>
internal static class EntityFrameworkSql
{
    public const string SqliteProvider = "Microsoft.EntityFrameworkCore.Sqlite";

    private static readonly ConditionalWeakTable<IModel, TwinboxSql> Cache = [];

    public static TwinboxSql For(DbContext context) =>
        Cache.GetValue(context.Model, _ => Create(context));

    /// <summary>Oracle's MySql.EntityFrameworkCore and Pomelo.EntityFrameworkCore.MySql.</summary>
    public static bool IsMySql(string? providerName) =>
        providerName is "MySql.EntityFrameworkCore" or "Pomelo.EntityFrameworkCore.MySql";

    private static TwinboxSql Create(DbContext context)
    {
        var provider = context.Database.ProviderName switch
        {
            "Microsoft.EntityFrameworkCore.SqlServer" => SqlProvider.SqlServer,
            "Npgsql.EntityFrameworkCore.PostgreSQL" => SqlProvider.PostgreSql,
            SqliteProvider => SqlProvider.Sqlite,
            var name when IsMySql(name) => SqlProvider.MySql,
            var other => throw new NotSupportedException(
                $"Twinbox.EntityFrameworkCore supports SQL Server, PostgreSQL, MySQL and SQLite; '{other}' is not supported yet."),
        };

        var helper = context.GetService<ISqlGenerationHelper>();
        var outbox = FindEntityType(context, TwinboxEntities.Outbox);
        var inbox = FindEntityType(context, TwinboxEntities.Inbox);
        return new TwinboxSql(
            provider,
            helper.DelimitIdentifier(outbox.GetTableName()!, outbox.GetSchema()),
            helper.DelimitIdentifier(inbox.GetTableName()!, inbox.GetSchema()),
            ColumnResolver(helper, outbox),
            ColumnResolver(helper, inbox));
    }

    private static IEntityType FindEntityType(DbContext context, string name) =>
        context.Model.FindEntityType(name)
            ?? throw new InvalidOperationException(
                $"{context.GetType().Name} has no Twinbox tables. Call modelBuilder.AddTwinbox() in OnModelCreating and add a migration.");

    private static Func<string, string> ColumnResolver(ISqlGenerationHelper helper, IEntityType entityType)
    {
        var table = StoreObjectIdentifier.Table(entityType.GetTableName()!, entityType.GetSchema());
        return property => helper.DelimitIdentifier(entityType.FindProperty(property)!.GetColumnName(table)!);
    }
}
