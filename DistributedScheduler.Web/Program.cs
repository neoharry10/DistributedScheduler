using DistributedScheduler.Web.Components;
using DistributedScheduler.Data;
using DistributedScheduler.Domain;
using DistributedScheduler.Web.Services;
using Npgsql;

var builder = WebApplication.CreateBuilder(args);

var cs = builder.Configuration.GetConnectionString("Scheduler")
    ?? throw new InvalidOperationException("ConnectionStrings:Scheduler is missing");

builder.Services.AddSingleton(_ => NpgsqlDataSource.Create(cs));
builder.Services.AddSingleton<IJobRepository, JobRepository>();
builder.Services.AddSingleton<JobChangeFeed>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<JobChangeFeed>());

// Add services to the container.
builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
    // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
    app.UseHsts();
}
app.UseStatusCodePagesWithReExecute("/not-found", createScopeForStatusCodePages: true);
app.UseHttpsRedirection();

app.UseAntiforgery();

app.MapStaticAssets();
app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

app.Run();
