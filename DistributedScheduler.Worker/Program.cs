using DistributedScheduler.Worker;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddJobWorker(
    builder.Configuration.GetConnectionString("Scheduler")!,
    o => builder.Configuration.GetSection("Worker").Bind(o));   // lets the tests tune it via env vars

// The host must wait longer than our own drain timeout, otherwise it kills us mid-drain.
builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(45));

builder.Build().Run();