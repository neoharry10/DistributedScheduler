using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using DistributedScheduler.Domain;

namespace DistributedScheduler.Tests;

public class TelemetryTests : PgTestBase
{
    [Fact]
    public async Task Job_execution_continues_the_trace_of_the_enqueue()
    {
        var started = new ConcurrentBag<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == Telemetry.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStarted = started.Add
        };
        ActivitySource.AddActivityListener(listener);

        var enqueued = await Repo.EnqueueAsync("GenerateInvoiceJob", new InvoicePayload(Guid.NewGuid(), 10m));

        using var host = BuildHost(o => o.PollInterval = TimeSpan.FromMilliseconds(100));
        await host.StartAsync();
        await WaitForAsync(async () => await StatusAsync(enqueued.Id) == JobStatus.Succeeded,
            TimeSpan.FromSeconds(30), "the job to succeed");
        await host.StopAsync();

        var enqueue = started.Single(a => a.OperationName == "enqueue GenerateInvoiceJob");
        var run = started.Single(a => a.OperationName == "job GenerateInvoiceJob");

        Assert.Equal(enqueue.TraceId, run.TraceId);        // same trace...
        Assert.Equal(enqueue.SpanId, run.ParentSpanId);    // ...and the job span hangs under the enqueue span
    }

    [Fact]
    public async Task Finished_counter_reports_each_outcome()
    {
        var outcomes = new ConcurrentDictionary<string, long>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (inst, l) =>
        {
            if (inst.Meter.Name == Telemetry.Name && inst.Name == "scheduler.jobs.finished")
                l.EnableMeasurementEvents(inst);
        };
        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            foreach (var tag in tags)
                if (tag.Key == "outcome")
                    outcomes.AddOrUpdate((string)tag.Value!, value, (_, old) => old + value);
        });
        listener.Start();

        for (var i = 0; i < 5; i++) await EnqueueAsync("SendEmailJob");
        await EnqueueAsync("PoisonJob", maxAttempts: 2);   // attempt 1 -> retry, attempt 2 -> dead

        using var host = BuildHost(o =>
        {
            o.PollInterval = TimeSpan.FromMilliseconds(100);
            o.RetryBase = TimeSpan.FromMilliseconds(50);
            o.RetryCap = TimeSpan.FromMilliseconds(200);
        });
        await host.StartAsync();
        await WaitForAsync(
            async () => await CountAsync("SELECT count(*) FROM jobs WHERE status IN (2, 4)") == 6,
            TimeSpan.FromSeconds(30), "all jobs to finish");
        await host.StopAsync();   // the drain guarantees every counter increment has happened

        Assert.Equal(5, outcomes.GetValueOrDefault("succeeded"));
        Assert.Equal(1, outcomes.GetValueOrDefault("retry"));
        Assert.Equal(1, outcomes.GetValueOrDefault("dead"));
    }
}