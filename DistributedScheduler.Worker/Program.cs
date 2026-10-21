using DistributedScheduler.Worker;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddJobWorker(builder.Configuration.GetConnectionString("Scheduler")!);
builder.Build().Run();