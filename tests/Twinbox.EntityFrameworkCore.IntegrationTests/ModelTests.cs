using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Twinbox.EntityFrameworkCore.IntegrationTests;

public sealed class ModelTests
{
    [Fact]
    public void DefaultTableNames_AreLeftToNamingConventions()
    {
        var outbox = (IConventionEntityType)BuildModel(_ => { }).FindEntityType(TwinboxEntities.Outbox)!;

        Assert.Equal("TwinboxOutbox", outbox.GetTableName());
        Assert.NotEqual(ConfigurationSource.Explicit, outbox.GetTableNameConfigurationSource());
    }

    [Fact]
    public void Options_OverrideTableAndSchema()
    {
        var inbox = BuildModel(o =>
        {
            o.InboxTable = "processed_messages";
            o.Schema = "messaging";
        }).FindEntityType(TwinboxEntities.Inbox)!;

        Assert.Equal("processed_messages", inbox.GetTableName());
        Assert.Equal("messaging", inbox.GetSchema());
    }

    [Fact]
    public void EntityNames_DoNotDependOnClrTypeNames()
    {
        var names = BuildModel(_ => { }).GetEntityTypes().Select(e => e.Name).Order();

        Assert.Equal([TwinboxEntities.Inbox, TwinboxEntities.Outbox], names);
    }

    [Fact]
    public void Timestamps_KeepMicrosecondsOnEveryProvider()
    {
        var mySql = ModelFor(o => o.UseMySQL("server=localhost;database=twinbox;user=twinbox;password=twinbox"));
        var sqlServer = ModelFor(o => o.UseSqlServer("Server=localhost;Database=twinbox"));

        Assert.Equal(6, mySql.FindEntityType(TwinboxEntities.Outbox)!.FindProperty("AvailableAt")!.GetPrecision());
        Assert.Equal(6, sqlServer.FindEntityType(TwinboxEntities.Outbox)!.FindProperty("AvailableAt")!.GetPrecision());
    }

    private static IModel ModelFor(Action<DbContextOptionsBuilder<ShopContext>> configure)
    {
        var options = new DbContextOptionsBuilder<ShopContext>();
        configure(options);
        using var context = new ShopContext(options.Options);
        return context.Model;
    }

    private static IModel BuildModel(Action<TwinboxModelOptions> configure)
    {
        var builder = new ModelBuilder();
        builder.AddTwinbox(configure);
        return (IModel)builder.Model;
    }
}
