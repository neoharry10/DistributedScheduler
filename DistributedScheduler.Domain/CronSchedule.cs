using Cronos;

namespace DistributedScheduler.Domain;

public static class CronSchedule
{
    public static CronExpression Parse(string cron)
    {
        var fields = cron.Split(' ', StringSplitOptions.RemoveEmptyEntries).Length;
        return CronExpression.Parse(cron, fields == 6 ? CronFormat.IncludeSeconds : CronFormat.Standard);
    }

    /// First occurrence strictly after fromUtc, or null if it never fires again.
    public static DateTime? NextAfter(string cron, string timeZoneId, DateTime fromUtc)
    {
        var zone = TimeZoneInfo.FindSystemTimeZoneById(timeZoneId);
        var next = Parse(cron).GetNextOccurrence(DateTime.SpecifyKind(fromUtc, DateTimeKind.Utc), zone);
        return next is null ? null : DateTime.SpecifyKind(next.Value, DateTimeKind.Utc);
    }
}