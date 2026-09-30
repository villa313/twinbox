using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Http;

namespace Twinbox.Dashboard;

/// <summary>Guards the dashboard's POSTs with a token sent in a custom header and bound to the signed-in user.</summary>
internal sealed class DashboardCsrf
{
    public const string HeaderName = "X-Twinbox-Csrf";

    private const string Purpose = "Twinbox.Dashboard.Csrf.v1";

    private readonly IDataProtector? _protector;
    private readonly byte[] _key = RandomNumberGenerator.GetBytes(32);

    // Data protection shares keys across instances when the app configures it; otherwise tokens are per process.
    public DashboardCsrf(IDataProtectionProvider? protection) => _protector = protection?.CreateProtector(Purpose);

    public string Issue(HttpContext context)
    {
        var user = Encoding.UTF8.GetBytes(UserOf(context));
        return _protector is null
            ? Convert.ToHexString(HMACSHA256.HashData(_key, user))
            : Convert.ToBase64String(_protector.Protect(user));
    }

    public bool IsValid(HttpContext context)
    {
        if (!context.Request.Headers.TryGetValue(HeaderName, out var values) || values.Count != 1 || string.IsNullOrEmpty(values[0]))
        {
            return false;
        }

        var token = values[0]!;
        var user = Encoding.UTF8.GetBytes(UserOf(context));
        if (_protector is null)
        {
            var expected = Encoding.ASCII.GetBytes(Convert.ToHexString(HMACSHA256.HashData(_key, user)));
            return CryptographicOperations.FixedTimeEquals(expected, Encoding.ASCII.GetBytes(token));
        }

        try
        {
            return CryptographicOperations.FixedTimeEquals(_protector.Unprotect(Convert.FromBase64String(token)), user);
        }
        catch (FormatException)
        {
            return false;
        }
        catch (CryptographicException)
        {
            return false;
        }
    }

    /// <summary>Browsers label cross-site requests; clients that send no label still need the token.</summary>
    public static bool IsCrossSite(HttpRequest request) =>
        request.Headers.TryGetValue("Sec-Fetch-Site", out var site)
            && site.ToString() is not ("same-origin" or "none");

    private static string UserOf(HttpContext context) => context.User.Identity?.Name ?? string.Empty;
}
