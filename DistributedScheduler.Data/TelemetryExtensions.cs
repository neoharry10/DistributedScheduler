using DistributedScheduler.Domain;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace DistributedScheduler.Data;

public static class TelemetryExtensions
{
    public static IServiceCollection AddSchedulerTelemetry(
        this IServiceCollection services, IConfiguration config, string serviceName,
        Action<TracerProviderBuilder>? configureTracing = null)
    {
        var endpoint = config["OTEL_EXPORTER_OTLP_ENDPOINT"];

        // No endpoint = no OpenTelemetry at all. Then StartActivity returns null and the overhead is zero.
        if (string.IsNullOrWhiteSpace(endpoint)) return services;

        services.AddOpenTelemetry()
            .ConfigureResource(r => r.AddService(serviceName))
            .WithTracing(t => { t.AddSource(Telemetry.Name); configureTracing?.Invoke(t); })
            .WithMetrics(m => m.AddMeter(Telemetry.Name))
            .UseOtlpExporter(OtlpExportProtocol.Grpc, new Uri(endpoint));

        return services;
    }
}