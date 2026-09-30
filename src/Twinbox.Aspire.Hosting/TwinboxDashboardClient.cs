using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Twinbox.Aspire.Hosting;

internal sealed record ReplayResult(bool Success, int Replayed, string? Error)
{
    public static ReplayResult Failed(string error) => new(false, 0, error);
}

/// <summary>Talks to a Twinbox dashboard's JSON API the way its own page does: fetch the CSRF token, then POST with it.</summary>
internal sealed class TwinboxDashboardClient(HttpClient http)
{
    public const string HttpClientName = "Twinbox.Aspire.Hosting";

    private const string CsrfHeader = "X-Twinbox-Csrf";

    private static readonly DashboardJsonContext Json = DashboardJsonContext.Default;

    /// <summary>Replays dead messages in every browsable store and tenant. <paramref name="dashboard"/> must end with '/'.</summary>
    public async Task<ReplayResult> ReplayDeadLettersAsync(Uri dashboard, CancellationToken cancellationToken)
    {
        try
        {
            return await ReplayCoreAsync(dashboard, cancellationToken).ConfigureAwait(false);
        }
        catch (HttpRequestException ex)
        {
            return ReplayResult.Failed($"Couldn't reach the Twinbox dashboard at {dashboard}: {ex.Message}");
        }
        catch (TaskCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return ReplayResult.Failed($"The Twinbox dashboard at {dashboard} didn't answer in time.");
        }
    }

    private async Task<ReplayResult> ReplayCoreAsync(Uri dashboard, CancellationToken cancellationToken)
    {
        using var configResponse = await http.GetAsync(new Uri(dashboard, "api/config"), cancellationToken).ConfigureAwait(false);
        if (!configResponse.IsSuccessStatusCode)
        {
            return ReplayResult.Failed(await DescribeFailureAsync(configResponse, dashboard, cancellationToken).ConfigureAwait(false));
        }

        var config = await ReadAsync(configResponse, Json.DashboardConfig, cancellationToken).ConfigureAwait(false);
        if (config?.CsrfToken is not { Length: > 0 } token)
        {
            // A sign-in redirect ends on an HTML page with status 200, so the body is the only reliable signal.
            return ReplayResult.Failed(
                $"{dashboard} didn't return the Twinbox dashboard's configuration. If it's behind interactive sign-in, map it with AllowAnonymous or a development policy.");
        }

        if (config.ReadOnly)
        {
            return ReplayResult.Failed("The Twinbox dashboard is read-only, so dead letters can't be replayed from it.");
        }

        var stores = config.Stores?.Where(s => s.Browsable).ToList() ?? [];
        if (stores.Count == 0)
        {
            return ReplayResult.Failed("None of the app's outbox stores supports browsing messages.");
        }

        IReadOnlyList<string?> tenants = config.Tenants is { Count: > 0 } named ? [.. named] : [null];
        var replayed = 0;
        foreach (var store in stores)
        {
            foreach (var tenant in tenants)
            {
                using var request = new HttpRequestMessage(HttpMethod.Post, new Uri(dashboard, "api/dead/replay-all"))
                {
                    Content = JsonContent.Create(new ReplayAllRequest(store.Id, tenant), Json.ReplayAllRequest),
                };
                request.Headers.Add(CsrfHeader, token);

                using var response = await http.SendAsync(request, cancellationToken).ConfigureAwait(false);
                if (!response.IsSuccessStatusCode)
                {
                    return new ReplayResult(false, replayed, await DescribeFailureAsync(response, dashboard, cancellationToken).ConfigureAwait(false));
                }

                var result = await ReadAsync(response, Json.MutationResponse, cancellationToken).ConfigureAwait(false);
                if (result is null)
                {
                    return new ReplayResult(false, replayed, $"{dashboard} answered the replay with something other than the Twinbox dashboard's JSON.");
                }

                replayed += result.Changed;
            }
        }

        return new ReplayResult(true, replayed, null);
    }

    private static async Task<string> DescribeFailureAsync(HttpResponseMessage response, Uri dashboard, CancellationToken cancellationToken)
    {
        var detail = (await ReadAsync(response, Json.ErrorResponse, cancellationToken).ConfigureAwait(false))?.Error;
        var status = (int)response.StatusCode;
        return response.StatusCode switch
        {
            HttpStatusCode.NotFound when detail is null =>
                $"No Twinbox dashboard at {dashboard} (404). Check the path passed to WithTwinboxDashboard and MapTwinboxDashboard.",
            HttpStatusCode.Unauthorized =>
                $"The Twinbox dashboard at {dashboard} needs sign-in (401). Map it with AllowAnonymous or a development policy.",
            _ when detail is not null => $"The Twinbox dashboard refused the request ({status}): {detail}",
            _ => $"The Twinbox dashboard at {dashboard} returned {status} {response.ReasonPhrase}.",
        };
    }

    private static async Task<T?> ReadAsync<T>(HttpResponseMessage response, JsonTypeInfo<T> typeInfo, CancellationToken cancellationToken)
        where T : class
    {
        if (response.Content.Headers.ContentType?.MediaType is not { } mediaType
            || !mediaType.EndsWith("json", StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        try
        {
            return await response.Content.ReadFromJsonAsync(typeInfo, cancellationToken).ConfigureAwait(false);
        }
        catch (JsonException)
        {
            return null;
        }
    }
}
