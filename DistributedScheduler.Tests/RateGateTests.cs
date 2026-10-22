using System.Diagnostics;
using DistributedScheduler.Worker;

namespace DistributedScheduler.Tests;

public class RateGateTests
{
    [Fact]
    public async Task Sequential_callers_are_paced()
    {
        var gate = new RateGate(20);   // one start every 50ms
        var sw = Stopwatch.StartNew();
        for (var i = 0; i < 11; i++) await gate.WaitAsync(default);   // 10 intervals = ~500ms
        Assert.InRange(sw.ElapsedMilliseconds, 450, 1500);
    }

    [Fact]
    public async Task Concurrent_callers_each_get_their_own_slot()
    {
        var gate = new RateGate(20);
        var sw = Stopwatch.StartNew();
        await Task.WhenAll(Enumerable.Range(0, 11).Select(_ => gate.WaitAsync(default)));
        Assert.InRange(sw.ElapsedMilliseconds, 450, 1500);   // not "all at once"
    }
}