# HTTP

`Twinbox.Http` · transport name `"http"`

Sends each message as one HTTP request. A route destination names an **endpoint** you configure (URL, method,
headers). Calls to vendor APIs and outgoing webhooks get the outbox's durability, retries with backoff, `Retry-After`
handling and a circuit breaker, instead of a hand-written dispatcher. The HTTP transport only sends; to receive
webhooks, see [Webhooks](../webhooks.md).

## Setup

```csharp
builder.Services.AddTwinbox(twinbox => twinbox
    .UseEntityFrameworkCore<ShopContext>()
    .UseHttp(http => http.AddEndpoint("valuelink", e =>
    {
        e.Url = new Uri("https://api.vendor.com/tenants/{tenant}/orders");
        e.Headers["X-Api-Key"] = apiKey;
        e.TreatAsSuccess(409);   // the vendor answers 409 to a repeat it already applied
    }))
    .Route<OrderPlaced>().To("valuelink", transport: "http"));
```

With a broker registered too, name the transport on each route (`transport: "http"`).

## Configuration

Endpoints can also come from configuration, under `Twinbox:Http:Endpoints:{name}`. Configuration values override
code.

```json
{
  "Twinbox": {
    "Http": {
      "Endpoints": {
        "partner-webhook": {
          "Url": "https://partner.example/hooks/orders",
          "Method": "POST",
          "Timeout": "00:00:10",
          "WebhookSecret": "whsec_...",
          "Headers": { "X-Source": "orders" },
          "TransientStatusCodes": [ 423 ],
          "SuccessStatusCodes": [ 409 ]
        }
      }
    }
  }
}
```

```csharp
twinbox.UseHttp().Route<OrderShipped>().To("partner-webhook", transport: "http");
```

Endpoints are validated at startup: the URL must be absolute http or https, the timeout positive, the webhook secret
well formed, and header names and values sendable.

## Endpoint options

| Option | Default | Description |
|---|---|---|
| `Url` | required | Absolute URL. Path and query may contain placeholders (below). |
| `Method` | `POST` | `GET` and `HEAD` requests carry no body. |
| `Headers` | empty | Sent on every request, e.g. an API key. They win over forwarded message headers. |
| `Timeout` | none | Caps one request. Keep it below `Twinbox:Dispatcher:SendTimeout` (30 s), which applies regardless. |
| `ContentType` | message's | Overrides the content type, for vendors that insist on one. |
| `ForwardHeaders` | `false` | Also send the message id, name, partition key and all Twinbox headers. Off so internal details stay private. |
| `IdempotencyKeyHeader` | `"Idempotency-Key"` | Header carrying the message id; `null` or empty omits it. |
| `WebhookSecret` | `null` | A `whsec_` base64 secret. Adds Standard Webhooks signature headers. |
| `ConfigureHttpClient` | `null` | Customizes the named `HttpClient`, e.g. with an auth handler. Code only. |
| `TreatAsSuccess(codes)` | | Status codes that count as delivered. |
| `TreatAsTransient(codes)` | | Status codes to retry, on top of 408, 429 and 5xx. |

## The request

- **Body:** the message payload (JSON by default) with its content type.
- **`Idempotency-Key`:** the message id, so a retried request can be deduplicated by the receiver. Every retry of a
  message uses the same id.
- **`traceparent`:** always sent, so the call joins the producer's trace.
- **Signatures:** with `WebhookSecret`, the request carries `webhook-id`, `webhook-timestamp` and `webhook-signature`
  per [Standard Webhooks](https://www.standardwebhooks.com/) (HMAC-SHA256 over `{id}.{timestamp}.{body}`), so
  receivers can verify it with any Standard Webhooks library. Twinbox's own [webhook ingress](../webhooks.md) verifies
  it with `VerifyStandardWebhooks`.

### URL placeholders

`{messageId}`, `{messageName}`, `{partitionKey}`, `{tenant}`, or any message header by name, e.g.
`https://api.example.com/customers/{partitionKey}/events`. Values are URL-escaped. A message with no value for a
placeholder is dead-lettered, since retrying can't produce one.

## Responses

| Response | Result |
|---|---|
| 2xx, or a `TreatAsSuccess` code | Delivered. |
| 408, 429, 5xx, or a `TreatAsTransient` code | Retried with backoff; a `Retry-After` header is honoured up to `Twinbox:Retry:MaxRetryAfter`. |
| Timeout or network error | Retried. The request may have landed; the idempotency key covers that. |
| Any other status | Dead-lettered, with the status and the first 500 characters of the body in `LastError`. |
| No endpoint with that name | Dead-lettered. |

Retry limits and the circuit breaker are the outbox's own, and can be set per endpoint name under
`Twinbox:Destinations:{name}`. See [Retries](../concepts/retries-and-dead-letters.md).

## Authentication and HttpClient

Each endpoint uses the `IHttpClientFactory` client named `twinbox-http:{name}` (`HttpTransport.HttpClientName(name)`).
Add handlers in code:

```csharp
twinbox.UseHttp(http => http.AddEndpoint("vendor", e =>
{
    e.Url = new Uri("https://api.vendor.com/orders");
    e.ConfigureHttpClient = client => client.AddHttpMessageHandler<VendorAuthHandler>();
}));
```

For endpoints defined only in configuration, configure the named client directly:

```csharp
builder.Services.AddHttpClient(HttpTransport.HttpClientName("partner-webhook"))
    .AddHttpMessageHandler<PartnerAuthHandler>();
```

## Destination prefix

With `Twinbox:DestinationPrefix`, routed destinations are prefixed, but endpoints keep their plain names: the
transport strips the prefix when it looks an endpoint up.

## Limitations

- One message, one request: there is no batching.
- The transport doesn't read response bodies beyond the error snippet, so request/response style calls that need the
  answer belong in your own code.
