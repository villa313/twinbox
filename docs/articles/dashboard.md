# Dashboard

`Twinbox.Dashboard` serves a small operations page and JSON API from your ASP.NET Core app: pending and dead counts per
store and tenant, how long the oldest pending message has been due, a filterable message list with details, and replay or
removal of dead messages.

```csharp
builder.Services.AddAuthorizationBuilder()
    .AddPolicy("ops", p => p.RequireRole("ops"));

var app = builder.Build();
app.UseAuthentication();
app.UseAuthorization();

app.MapTwinboxDashboard("/twinbox", o =>
    {
        o.ReadOnly = false;      // true hides replay and delete, and refuses them on the API
        o.ShowPayloads = false;  // payloads can hold personal data
    })
    .RequireAuthorization("ops");
```

## Options

| Option | Default | Description |
|---|---|---|
| `AllowAnonymous` | `false` | Serve the dashboard without an authorization policy. For local development only; logs a warning. |
| `ReadOnly` | `false` | Hide replay and delete, and refuse them on the API (403). |
| `ShowPayloads` | `false` | Show message payloads as UTF-8 text in the details view, capped at 64 KB. |

The prefix must be a literal path starting with `/` (default `/twinbox`).

## Security model

- **Default deny.** Every request is refused with 403 until an authorization policy is attached to the endpoints
  (`RequireAuthorization(...)`), unless `AllowAnonymous` is set. Forgetting auth doesn't expose the dashboard.
- **CSRF protection.** Mutating requests (`POST`) need an `X-Twinbox-Csrf` header. The token comes from `GET api/config`
  and is bound to the signed-in user's name. Requests the browser labels cross-site (`Sec-Fetch-Site`) are refused, and
  bodies must be JSON. The page handles all of this itself.
- **Tokens across instances.** Tokens are protected with ASP.NET Core Data Protection when it is registered. Behind a
  load balancer, share the key ring between instances (the usual `PersistKeysTo...` setup), or a token issued by one
  instance is rejected by another. Without Data Protection, tokens are per process.
- **Payloads hidden by default.** Messages often carry personal data, so the details view shows sizes and metadata
  only unless `ShowPayloads` is on.
- **Locked-down page.** The page makes no external requests and is served with a strict Content Security Policy
  (nonce-based scripts and styles, `connect-src 'self'`, `frame-ancestors 'none'`), `X-Frame-Options: DENY`,
  `Referrer-Policy: no-referrer` and `Cache-Control: no-store`.
- **Audit log.** Every replay and delete is logged at Information level with the user name, count, store and tenant.
- **Tenants.** Only tenants returned by your `ListTenants` callback can be browsed; a request for any other tenant is
  refused before a tenant scope is entered.

```csharp
builder.Services.AddDataProtection()
    .PersistKeysToAzureBlobStorage(blobUri, credential)
    .SetApplicationName("orders");
```

In development, open it up without weakening production:

```csharp
var dashboard = app.MapTwinboxDashboard("/twinbox", o => o.AllowAnonymous = app.Environment.IsDevelopment());
if (!app.Environment.IsDevelopment())
{
    dashboard.RequireAuthorization("ops");
}
```

## JSON API

Everything the page does is available under the same prefix:

| Endpoint | Description |
|---|---|
| `GET api/config` | CSRF token, `readOnly`, `showPayloads`, the stores (with whether each is browsable) and tenants. |
| `GET api/stats` | Pending count, dead count and oldest pending age for every store and tenant. |
| `GET api/messages?store=&tenant=&status=&destination=&name=&search=&take=&cursor=` | Newest first. `status` is `Pending`, `Processing`, `Sent` or `Dead`; `search` matches a message id or partition key exactly; `take` is 1 to 200 (default 50). Pass `nextCursor` back as `cursor`. |
| `GET api/messages/{id}?store=&tenant=` | One message, with headers, last error and (if enabled) payload. |
| `POST api/messages/replay` | Body `{ "store": 0, "tenant": null, "ids": [...] }`, 1 to 1,000 ids. Back to pending with a fresh attempt count. |
| `POST api/messages/delete` | Same body. Deletes the messages. |
| `POST api/dead/replay-all` | Body `{ "store": 0, "tenant": null, "destination": null, "name": null, "search": null }`. Replays every dead message matching the filter. |

`store` is the index from `api/config` (the first browsable store when omitted). Filters are limited to 256
characters. Mutations return `{ "changed": n }`.

```bash
token=$(curl -s --cookie auth.txt https://orders.example.com/twinbox/api/config | jq -r .csrfToken)
curl --cookie auth.txt -X POST https://orders.example.com/twinbox/api/dead/replay-all \
  -H "X-Twinbox-Csrf: $token" -H "Content-Type: application/json" \
  -d '{ "destination": "partner-webhook" }'
```

## Stores

The dashboard lists every registered outbox store. Browsing, replay and delete need the store to implement
`IOutboxAdmin`; every built-in store does. Stats need only `IOutboxStore`. With several EF Core contexts, each
context is its own store in the list.

Replaying a message sets it back to `Pending` with zero attempts, due now. Sent messages can be replayed too (through
`api/messages/replay`), to send one again. It keeps its place in the outbox: with a
partition key, it goes out before later messages of that key that are still pending (and holds them back until it is
sent), but after the ones already sent while it was dead. See [Ordering](concepts/ordering.md).
