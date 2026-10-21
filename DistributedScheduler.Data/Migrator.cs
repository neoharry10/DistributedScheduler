using DbUp;

namespace DistributedScheduler.Data;

public static class Migrator
{
    public static void Run(string connectionString)
    {
        EnsureDatabase.For.PostgresqlDatabase(connectionString);

        var result = DeployChanges.To
            .PostgresqlDatabase(connectionString)
            .WithScriptsEmbeddedInAssembly(typeof(Migrator).Assembly)
            .LogToConsole()
            .Build()
            .PerformUpgrade();

        if (!result.Successful) throw result.Error;
    }
}