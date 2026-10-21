using System.Collections.Concurrent;
using System.Diagnostics;

namespace DistributedScheduler.Tests;

public sealed class WorkerProcess : IDisposable
{
    private readonly Process _p;
    private readonly ConcurrentQueue<string> _output = new();

    private WorkerProcess(Process p) => _p = p;

    public string Output => string.Join(Environment.NewLine, _output);
    public string WorkerId => $"{Environment.MachineName}-{_p.Id}";   // same format JobWorker uses

    public static WorkerProcess Start(string connectionString, params (string Key, string Value)[] settings)
    {
        var dll = Path.Combine(AppContext.BaseDirectory, "DistributedScheduler.Worker.dll");
        if (!File.Exists(dll)) throw new FileNotFoundException("Worker dll not found next to the test assembly", dll);

        var psi = new ProcessStartInfo("dotnet", $"\"{dll}\"")
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };
        psi.Environment["ConnectionStrings__Scheduler"] = connectionString;
        foreach (var (key, value) in settings) psi.Environment[key] = value;

        var p = new Process { StartInfo = psi };
        var wp = new WorkerProcess(p);
        p.OutputDataReceived += (_, e) => { if (e.Data is not null) wp._output.Enqueue(e.Data); };
        p.ErrorDataReceived += (_, e) => { if (e.Data is not null) wp._output.Enqueue(e.Data); };
        p.Start();
        p.BeginOutputReadLine();
        p.BeginErrorReadLine();
        return wp;
    }

    /// TerminateProcess on Windows: no shutdown hooks, no finally blocks. Same effect as kill -9.
    public void Kill()
    {
        _p.Kill(entireProcessTree: true);
        _p.WaitForExit();
    }

    public void Dispose()
    {
        try { if (!_p.HasExited) _p.Kill(entireProcessTree: true); } catch { /* already gone */ }
        _p.Dispose();
    }
}