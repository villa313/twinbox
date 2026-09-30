using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace Twinbox.Http.Tests;

public sealed class WebhookSignatureTests
{
    private const string Secret = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw";

    [Fact]
    public void Sign_MatchesTheStandardWebhooksReferenceVector()
    {
        var signer = WebhookSigner.Create(Secret)!;

        var signature = signer.Sign("msg_p5jXN8AQM9LWM0D4loKWxJek", 1614265330, """{"test": 2432232314}"""u8);

        Assert.Equal("v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE=", signature);
    }

    [Fact]
    public async Task Send_AddsHeadersAReceiverCanVerify()
    {
        await using var host = HttpTestHost.Create(o => o.AddEndpoint("hooks", e =>
        {
            e.Url = new Uri("https://hooks.test/in");
            e.WebhookSecret = Secret;
        }));
        var message = HttpTestHost.Message("hooks");

        await host.SendAsync(message);

        var request = Assert.Single(host.Handler.Requests);
        var id = request.Headers["webhook-id"];
        var timestamp = request.Headers["webhook-timestamp"];
        Assert.Equal(message.MessageId, id);
        Assert.Equal(HttpTestHost.Start.ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture), timestamp);
        Assert.Equal(Reference(Secret, id, timestamp, request.Body!), request.Headers["webhook-signature"]);
    }

    [Fact]
    public async Task Send_WithoutSecret_AddsNoSignature()
    {
        await using var host = HttpTestHost.Create(o => o.AddEndpoint("hooks", e => e.Url = new Uri("https://hooks.test/in")));

        await host.SendAsync(HttpTestHost.Message("hooks"));

        Assert.False(Assert.Single(host.Handler.Requests).Headers.ContainsKey("webhook-signature"));
    }

    [Theory]
    [InlineData("MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw")]
    [InlineData("whsec_")]
    [InlineData("whsec_not base64!")]
    public void TryParseSecret_RejectsMalformedSecrets(string secret) =>
        Assert.False(WebhookSigner.TryParseSecret(secret, out _));

    /// <summary>What a receiver does, written independently of the signer.</summary>
    private static string Reference(string secret, string id, string timestamp, byte[] body)
    {
        var key = Convert.FromBase64String(secret["whsec_".Length..]);
        var signed = Encoding.UTF8.GetBytes($"{id}.{timestamp}.{Encoding.UTF8.GetString(body)}");
        return "v1," + Convert.ToBase64String(HMACSHA256.HashData(key, signed));
    }
}
