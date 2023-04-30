using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Taskling.Blocks.Factories;
using Taskling.CleanUp;
using Taskling.Configuration;
using Taskling.Contexts;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.CleanUp;
using Taskling.InfrastructureContracts.CriticalSections;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.Events;
using Taskling.SqlServer.Tokens;
using Taskling.SqlServer.Tokens.Executions;

namespace Taskling.SqlServer.Tests.Helpers;

public class ClientHelper : IClientHelper
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<ClientHelper> _logger;

    public ClientHelper(IServiceProvider serviceProvider, ILogger<ClientHelper> logger)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
        _logger.LogDebug($"{nameof(ClientHelper)} constructor was called");
    }

    public ConfigurationOptions GetDefaultTaskConfigurationWithTimePeriodOverrideAndNoReprocessing(
        int maxBlocksToGenerate = 2000)
    {
        return GetConfigurationOptions(1, 2000, 2000, 1, false, 0, 0, 240, false, 0, 0, false, 0, 0,
            maxBlocksToGenerate);
    }

    public ITaskExecutionContext GetExecutionContext(string taskName, ConfigurationOptions configurationOptions)
    {
        var client = CreateClient(configurationOptions);
        return client.CreateTaskExecutionContext(TestConstants.ApplicationName, taskName);
    }


    public ConfigurationOptions GetDefaultTaskConfigurationWithKeepAliveAndReprocessing(int maxBlocksToGenerate = 2000)
    {
        return GetConfigurationOptions(1, 2000, 2000, 1, true, 1, 10, 0, true, 600, 3, true, 600, 3,
            maxBlocksToGenerate);
    }

    public ConfigurationOptions GetDefaultTaskConfigurationWithKeepAliveAndNoReprocessing(
        int maxBlocksToGenerate = 2000)
    {
        return GetConfigurationOptions(1, 2000, 2000, 1, true, 1, 2, 0, false, 0, 0, false, 0, 0,
            maxBlocksToGenerate);
    }

    public ConfigurationOptions GetDefaultTaskConfigurationWithTimePeriodOverrideAndReprocessing(
        int maxBlocksToGenerate = 2000)
    {
        return GetConfigurationOptions(1, 2000, 2000, 1, false, 0, 0, 240, true, 600, 3, true, 600, 3,
            maxBlocksToGenerate);
    }

    public ConfigurationOptions GetConfigurationOptions(int v0, int v1, int v2, int v3, bool v4, int v5, int v6, int v7,
        bool v8, int v9,
        int v10, bool v11, int v12, int v13, int v14)
    {
        return new ConfigurationOptions
        {
            DB = "Server=(local);Database=TasklingDb;Application Name=Entity;Trusted_Connection=True;",
            TO = 120,
            E = true,
            CON = v0,
            KPLT = v1,
            KPDT = v2,
            MCI = v3,
            KA = v4,
            KAINT = v5,
            KADT = v6,
            TPDT = v7,
            RPC_FAIL = v8,
            RPC_FAIL_MTS = v9,
            RPC_FAIL_RTYL = v10,
            RPC_DEAD = v11,
            RPC_DEAD_MTS = v12,
            RPC_DEAD_RTYL = v13,
            MXBL = v14
        };
    }

    private object mutex = new object();
    private TasklingClient CreateClient(ConfigurationOptions configurationOptions)
    {
        lock (mutex)
        {
            

            return new TasklingClient(_serviceProvider, new TaskConfigurationRepository(new TestConfigurationReader(configurationOptions)));
        }
    }
}