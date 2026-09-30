# Multi-tenancy

Twinbox supports a **database per tenant**: each tenant's outbox and inbox live in that tenant's database, next to its
data. You tell Twinbox three things:

```csharp
twinbox.UseTenants(o =>
{
    // Which tenants exist; cached for TenantListCacheDuration (1 minute).
    o.ListTenants = async (sp, ct) => await sp.GetRequiredService<TenantCatalog>().GetIdsAsync(ct);

    // Apply a tenant to a DI scope Twinbox created, e.g. by setting your scoped tenant context.
    o.EnterTenant = (sp, tenant) => sp.GetRequiredService<TenantContext>().Id = tenant;

    // Read the tenant of the scope a message is sent from; null means no tenant.
    o.CurrentTenant = sp => sp.GetRequiredService<TenantContext>().Id;
});
```

All three callbacks are required. Your tenant-aware registrations do the rest, for example a `DbContext` whose
connection string depends on the scoped `TenantContext`:

```csharp
builder.Services.AddScoped<TenantContext>();
builder.Services.AddDbContext<ShopContext>((sp, o) =>
    o.UseSqlServer(sp.GetRequiredService<TenantCatalog>().ConnectionStringFor(sp.GetRequiredService<TenantContext>().Id)));
```

For the ADO.NET and MongoDB stores, use the per-scope factories instead of a fixed connection:

```csharp
twinbox.UseSqlServer(o => o.ConnectionStringFactory =
    sp => catalog.ConnectionStringFor(sp.GetRequiredService<TenantContext>().Id));

twinbox.UseMongoDB(o =>
{
    o.ConnectionString = mongoConnectionString;
    o.DatabaseNameFactory = sp => $"shop_{sp.GetRequiredService<TenantContext>().Id}";
});
```

## What Twinbox does with tenants

- **Sending.** `outbox.Send` stamps the row with `CurrentTenant` (or, inside a handler, the tenant of the message
  being handled). The tenant travels as the `twinbox-tenant` header.
- **Dispatching.** Each pass visits every listed tenant, enters it, and claims from its store.
- **Receiving.** The inbound pipeline reads `twinbox-tenant`, enters that tenant, and runs the handler against the
  tenant's database. Messages the handler sends keep the tenant.
- **Schema creation.** The ADO.NET stores create their tables in every listed tenant database at startup, and in a new
  tenant's database the first time it is used. MongoDB does the same for its indexes.
- **Retention, health checks and the dashboard** go through every tenant.

Twinbox uses its own scope factory for all this (`Twinbox.Tenancy.TwinboxScopeFactory`), which calls `EnterTenant`
on every scope it creates. If you write a custom store, create scopes through it rather than `IServiceScopeFactory`.

## Running code for each tenant

`TenantDirectory` is registered as a singleton and exposes the same loop Twinbox uses:

```csharp
public class NightlyReport(TenantDirectory tenants, TwinboxScopeFactory scopes)
{
    public Task RunAsync(CancellationToken ct) => tenants.ForEachTenantAsync(async token =>
    {
        await using var scope = scopes.CreateAsyncScope();   // EnterTenant already applied
        // ...
    }, ct);
}
```

## Limitations

- Tenancy is database-per-tenant. For a shared database with a tenant column, don't configure `UseTenants`; put the
  tenant in your own headers instead.
- The dispatcher visits tenants one after another in each pass. Thousands of tenant databases mean thousands of claim
  queries per pass; tune `Dispatcher:MinPollInterval` and `MaxPollInterval` accordingly.
- A message arriving with a tenant that isn't listed is still processed in that tenant; Twinbox doesn't validate
  inbound tenant ids against the list. Only trust tenant headers from your own services.
- [Webhook endpoints](../webhooks.md#endpoint-options) must say which tenant a request belongs to with
  `WithTenant(...)`; mapping one without it throws while tenancy is configured.
- The outbox import and inbox seeding used for [migration](../migration.md) run once per listed tenant, with that
  tenant entered, so `CreateConnection` can pick the tenant's old database.
