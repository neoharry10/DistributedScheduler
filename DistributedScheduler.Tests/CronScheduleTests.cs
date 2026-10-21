using DistributedScheduler.Domain;

namespace DistributedScheduler.Tests;

public class CronScheduleTests
{
    private static DateTime Utc(int y, int mo, int d, int h, int mi, int s) =>
        new(y, mo, d, h, mi, s, DateTimeKind.Utc);

    [Fact]
    public void Five_field_cron_is_minute_based() =>
        Assert.Equal(Utc(2026, 10, 22, 3, 0, 0),
            CronSchedule.NextAfter("0 3 * * *", "UTC", Utc(2026, 10, 21, 10, 0, 0)));

    [Fact]
    public void Six_field_cron_includes_seconds() =>
        Assert.Equal(Utc(2026, 10, 21, 12, 0, 2),
            CronSchedule.NextAfter("*/2 * * * * *", "UTC", Utc(2026, 10, 21, 12, 0, 1)));

    [Fact]
    public void Next_is_strictly_after_the_given_time() =>
        Assert.Equal(Utc(2026, 10, 21, 12, 0, 4),
            CronSchedule.NextAfter("*/2 * * * * *", "UTC", Utc(2026, 10, 21, 12, 0, 2)));

    [Fact]
    public void Time_zone_is_respected() =>
        // Athens is UTC+3 on Oct 21 (summer time ends Oct 25), so 03:00 local = 00:00 UTC
        Assert.Equal(Utc(2026, 10, 22, 0, 0, 0),
            CronSchedule.NextAfter("0 3 * * *", "Europe/Athens", Utc(2026, 10, 21, 10, 0, 0)));
}