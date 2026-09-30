namespace Twinbox.Http;

/// <summary>An endpoint ready to send to, with its webhook key decoded once.</summary>
internal sealed record HttpEndpoint(string Name, string ClientName, HttpEndpointOptions Options, WebhookSigner? Signer)
{
    public static HttpEndpoint Create(string name, HttpEndpointOptions options) =>
        new(name, HttpTransport.HttpClientName(name), options, WebhookSigner.Create(options.WebhookSecret));
}
