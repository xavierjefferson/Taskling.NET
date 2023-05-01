﻿using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Taskling.SqlServer.AncilliaryServices;
using Taskling.SqlServer.Tests.Helpers;
using TransactionScopeRetryHelper;
using TransactionScopeRetryHelper.MicrosoftDataSqlClient;
using TransactionScopeRetryHelper.MySqlClient;
using TransactionScopeRetryHelper.SystemDataSqlClient;
using Xunit.DependencyInjection;
using Xunit.DependencyInjection.Logging;

namespace Taskling.SqlServer.Tests;

public class Startup
{
    public void Configure(ILoggerFactory loggerFactory, ITestOutputHelperAccessor accessor)
    {
        loggerFactory.AddProvider(new XunitTestOutputLoggerProvider(accessor,
            (source, ll) => ll >= LogLevel.Debug));
    }

    public void ConfigureServices(IServiceCollection services)
    {
        RetryHelper.Extensions.AddMicrosoftDataSqlClient().AddMySqlClient().AddSystemDataSqlClient();

        RetryHelper.TransientCheckFunctions.AddSystemDataSqlClient()
            .AddMicrosoftDataSqlClient().AddMySqlClient();
        services.AddSingleton<IDbContextFactoryEx, DbContextFactoryEx>();
        services.AddLogging(configure =>
        {
            configure.AddConsole().AddDebug();
            configure.SetMinimumLevel(LogLevel.Debug);
        });
        services.AddTransient<IExecutionsHelper, ExecutionsHelper>();
        services.AddTransient<IClientHelper, ClientHelper>();
        services.AddTransient<IBlocksHelper, BlocksHelper>();
        services.AddTaskling();
    }
}