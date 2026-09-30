using System.Diagnostics;

namespace Twinbox.Aspire.Hosting;

/// <summary>Opens a URL on the machine running the AppHost; resolved from services first so tests can swap it.</summary>
internal sealed class BrowserLauncher(Action<Uri> open)
{
    public static readonly BrowserLauncher Default = new(url =>
    {
        using var _ = Process.Start(new ProcessStartInfo(url.ToString()) { UseShellExecute = true });
    });

    public void Open(Uri url) => open(url);
}
