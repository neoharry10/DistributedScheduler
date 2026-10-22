using System.Globalization;

namespace DistributedScheduler.Web.Services;

public static class Fmt
{
    public static string Time(DateTime? utc) =>
        utc is null ? "-" : utc.Value.ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

    public static string Short(Guid id) => id.ToString("N")[..8];

    public static string FirstLine(string? s, int max = 90)
    {
        if (string.IsNullOrEmpty(s)) return "";
        var line = s.Split('\n')[0].Trim();
        return line.Length > max ? line[..max] + "..." : line;
    }
}