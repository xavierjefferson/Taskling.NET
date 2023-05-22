using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Taskling.Contexts;
using Taskling.InfrastructureContracts;

namespace Taskling.EntityFrameworkCore.Tests.Helpers;

public class ClientHelper : IClientHelper
{
    private readonly ILogger<ClientHelper> _logger;

    private readonly object _mutex = new();
    private readonly IServiceProvider _serviceProvider;

    public ClientHelper(IServiceProvider serviceProvider, ILogger<ClientHelper> logger)
    {
        _logger = logger;

        _serviceProvider = serviceProvider;

        _logger.LogDebug($"{nameof(ClientHelper)} constructor was called");
    }

    public ConfigurationOptions GetDefaultTaskConfigurationWithTimePeriodOverrideAndNoReprocessing(
        int maxBlocksToGenerate = 2000)
    {
        return GetConfigurationOptions(1, 2000, 2000, 1, false, 0, 0, 240, false, 0, 0, false, 0, 0,
            maxBlocksToGenerate);
    }

    public ITaskExecutionContext GetExecutionContext(TaskId taskId,
        ConfigurationOptions configurationOptions)
    {
        var client = CreateClient(configurationOptions);
        return client.CreateTaskExecutionContext(taskId);
    }


    public ConfigurationOptions GetDefaultTaskConfigurationWithKeepAliveAndReprocessing(int maxBlocksToGenerate = 2000)
    {
        return GetConfigurationOptions(1, 2000, 2000, 1, true, 1, 10, 0, true, 600, 3, true, 600, 3,
            maxBlocksToGenerate);
    }

    public ConfigurationOptions GetDefaultTaskConfigurationWithKeepAliveAndNoReprocessing(
        int maxBlocksToGenerate = 2000)
    {
        var a = GetConfigurationOptions(1, 2000, 2000, 1, true, 1, 2, 0, true, 0, 0, false, 0, 0,
            maxBlocksToGenerate);
        return a;
    }

    public ConfigurationOptions GetDefaultTaskConfigurationWithTimePeriodOverrideAndReprocessing(
        int maxBlocksToGenerate = 2000)
    {
        return GetConfigurationOptions(1, 2000, 2000, 1, false, 0, 0, 240, true, 600, 3, true, 600, 3,
            maxBlocksToGenerate);
    }

    public ConfigurationOptions GetConfigurationOptions(int con, int v1, int v2, int v3, bool v4, int v5, int v6,
        int v7,
        bool rpcFail, int v9,
        int v10, bool v11, int v12, int v13, int v14)
    {
        return new ConfigurationOptions
        {
            DB = TestConstants.GetTestConnectionString(),
            TO = 120,
            E = true,
            CON = con,
            KPLT = v1,
            KPDT = v2,
            MCI = v3,
            KA = v4,
            KAINT = v5,
            KADT = v6,
            TPDT = v7,
            RPC_FAIL = rpcFail,
            RPC_FAIL_MTS = v9,
            RPC_FAIL_RTYL = v10,
            RPC_DEAD = v11,
            RPC_DEAD_MTS = v12,
            RPC_DEAD_RTYL = v13,
            MXBL = v14
        };
    }

    private TasklingClient CreateClient(ConfigurationOptions configurationOptions)
    {
        lock (_mutex)
        {
            return new TasklingClient(_serviceProvider,
                new TestConfigurationReader(configurationOptions,
                    _serviceProvider.GetRequiredService<ILogger<TestConfigurationReader>>()));
        }
    }
}