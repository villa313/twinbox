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

    private static IModel BuildModel(Action<TwinboxModelOptions> configure)
    {
        var builder = new ModelBuilder();
        builder.AddTwinbox(configure);
        return (IModel)builder.Model;
    }
}
