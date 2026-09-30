# Webhooks

Twinbox handles webhooks in both directions:

- **Incoming** (`Twinbox.Webhooks`): verify the signature, store the webhook durably, answer 200 at once, and run your
  handler afterwards with retries, once per provider event id.
- **Outgoing** (`Twinbox.Http`): send your own webhooks from the outbox, signed with Standard Webhooks. See the
  [HTTP transport](transports/http.md).

## Receiving webhooks

```csharp
builder.Services.AddTwinbox(twinbox => twinbox
    .UseSqlServer(connectionString)
    .AddWebhooks()
    .AddHandler<StripeEvents, StripeEvent>());

var app = builder.Build();

app.MapWebhookInbox<StripeEvent>("/webhooks/stripe",
    w => w.VerifyStripe(WebhookSecrets.FromConfiguration("Stripe:WebhookSecret")));

app.MapWebhookInbox("/webhooks/github", w => w.VerifyGitHub(WebhookSecrets.Of(gitHubSecret)));
```

```csharp
public sealed record StripeEvent : WebhookReceived;

public class StripeEvents : IHandle<StripeEvent>
{
    public Task HandleAsync(StripeEvent webhook, MessageContext context, CancellationToken ct)
    {
        // webhook.Provider, webhook.EventId, webhook.EventType, webhook.Body (raw JSON text),
        // webhook.ContentType, webhook.Headers, webhook.ReceivedAt
        using var json = JsonDocument.Parse(webhook.Body);
        return Task.CompletedTask;
    }
}
```

`AddWebhooks()` turns on [local delivery](transports/local.md), which is how stored webhooks reach your handlers.
Endpoints mapped without a type parameter deliver the base `WebhookReceived`; handle it with
`IHandle<WebhookReceived>`. Derive a record per endpoint (as above) to give each its own handler.

### What happens to a request

1. The body is read up to the size limit (1 MB by default; `WithMaxBodySize(bytes)`). Larger bodies get **413**
   without being fully read.
2. The verifier checks the signature over the raw bytes. A missing, malformed, stale or wrong signature gets **401**;
   a correctly signed body without an event id gets **400**. Reasons are logged, never returned.
3. The webhook is written to the outbox as a `WebhookReceived` (or your derived type) with an id derived from the
   message name, provider and event id. A provider retry of the same event maps to the same row: it is recognized and
   answered **200** without storing a second copy.
4. The dispatcher wakes and delivers it to your handler through the inbox, with the outbox's retries and backoff.
5. If the store is down, or a secret can't be resolved, the answer is **500** so the provider retries later.

Bodies and secrets are never logged.

## Verifiers

Every endpoint needs exactly one verifier; unsigned webhooks are never accepted.

| Method | Scheme | Event id and type from |
|---|---|---|
| `VerifyStripe(secrets, tolerance?)` | `Stripe-Signature: t=..., v1=...`, HMAC-SHA256 of `{t}.{body}` | The signed JSON body (`id`, `type`) |
| `VerifyStandardWebhooks(secrets, tolerance?)` | [Standard Webhooks](https://www.standardwebhooks.com/) (`webhook-*` or `svix-*` headers), HMAC-SHA256 of `{id}.{timestamp}.{body}`; secrets are `whsec_...` | The signed `webhook-id` header; type from the body's `type` |
| `VerifyShopify(secrets)` | `X-Shopify-Hmac-Sha256`, base64 HMAC-SHA256 of the body | Unsigned headers (`X-Shopify-Webhook-Id`, `X-Shopify-Topic`) |
| `VerifyGitHub(secrets)` | `X-Hub-Signature-256: sha256=<hex>` | Unsigned headers (`X-GitHub-Delivery`, `X-GitHub-Event`) |
| `VerifyHmac(secrets, options)` | Any single header holding an HMAC of the body | The headers you name, or a hash of the body |
| `Verify(IWebhookVerifier)` | Your own | Your own |

Stripe and Standard Webhooks sign a timestamp: requests more than 5 minutes off (the `tolerance` default) are refused
as replays. Shopify, GitHub and generic HMAC schemes sign only the body, so their event type and id headers aren't
covered by the signature; don't trust them beyond routing and deduplication.

### Generic HMAC

```csharp
app.MapWebhookInbox("/webhooks/acme", w => w
    .VerifyHmac(WebhookSecrets.FromConfiguration("Acme:Secret"), o =>
    {
        o.SignatureHeader = "X-Acme-Signature";
        o.Prefix = "sha256=";
        o.Algorithm = WebhookHmacAlgorithm.Sha256;   // or Sha512
        o.Encoding = SignatureEncoding.Hex;          // or Base64
        o.EventIdHeader = "X-Acme-Delivery";
        o.EventTypeHeader = "X-Acme-Event";
    })
    .WithProvider("acme"));
```

Without `EventIdHeader`, a hash of the body is the event id, so only byte-identical retries are deduplicated.

### Your own verifier

```csharp
public sealed class PartnerVerifier(string secret) : IWebhookVerifier
{
    public string DefaultProvider => "partner";

    public WebhookVerification Verify(WebhookRequest request)
    {
        if (!request.Headers.TryGetValue("X-Partner-Signature", out var header)
            || !request.Headers.TryGetValue("X-Partner-Delivery", out var delivery))
        {
            return WebhookVerification.Unauthorized("missing signature or delivery header");
        }

        var expected = HMACSHA256.HashData(Encoding.UTF8.GetBytes(secret), request.Body.Span);
        var actual = Convert.FromHexString(header.ToString());   // validate the format in real code
        return CryptographicOperations.FixedTimeEquals(expected, actual)
            ? WebhookVerification.Verified(delivery.ToString())
            : WebhookVerification.Unauthorized("signature mismatch");
    }
}

app.MapWebhookInbox("/webhooks/partner", w => w.Verify(new PartnerVerifier(partnerSecret)));
```

`WebhookVerification.Malformed(reason)` answers 400. A verifier that throws gets 500.

## Secrets and rotation

`WebhookSecrets.Of("s1", "s2")` or `WebhookSecrets.FromConfiguration("Stripe:WebhookSecret")`. Every secret is tried,
so during a rotation list both the old and the new one. A configuration key can hold one secret or an array
(`Stripe:WebhookSecret:0`, `Stripe:WebhookSecret:1`), and it is read on every request, so a configuration reload picks
up a rotation without a restart. A key with no secret refuses webhooks (500) rather than accepting them unsigned.

## Endpoint options

| Method | Effect |
|---|---|
| `WithProvider(name)` | Names the sender. It scopes event-id deduplication and becomes the destination `webhooks/{provider}`. Defaults to the verifier's provider (`stripe`, `github`, ...). |
| `ForwardHeaders(names...)` | Copies these request headers into `WebhookReceived.Headers`. Shopify and GitHub verifiers forward their event headers already. |
| `WithMaxBodySize(bytes)` | Body limit; default 1 MB. |
| `WithStore(name)` | The outbox store to write to, by its name (`"SqlServer"`, `"MongoDB"`, an EF Core context's class name, ...). Only needed when several stores are registered; without it, mapping the endpoint throws. |
| `WithTenant(context => ...)` | Picks the request's tenant, e.g. from a route value. Required when `UseTenants` is configured; a request it returns null or empty for is answered **404**. |

With [multi-tenancy](concepts/multi-tenancy.md), route the tenant into the URL and resolve it there:

```csharp
app.MapWebhookInbox("/webhooks/{tenant}/stripe", w => w
    .VerifyStripe(WebhookSecrets.FromConfiguration("Stripe:WebhookSecret"))
    .WithTenant(context => context.Request.RouteValues["tenant"] as string));
```

The webhook is stored in that tenant's database and handled with the tenant entered, like any other tenant's message.
Validate the tenant against your own list if unknown values must not reach your registrations.

`MapWebhookInbox` returns an `IEndpointConventionBuilder`, so rate limiting, host filtering and other endpoint
conventions apply as usual. It only maps `POST`.

## Limitations

- **Local delivery only.** Stored webhooks are dispatched to handlers in the same app; to forward them elsewhere,
  send a message from the handler.
- The [destination prefix](concepts/routing.md#destination-prefix) applies to `webhooks/{provider}` like to any
  logical destination, so handlers see `MessageContext.Source` with the prefix.
- The body is decoded as UTF-8 text into `WebhookReceived.Body`; binary payloads aren't supported.
