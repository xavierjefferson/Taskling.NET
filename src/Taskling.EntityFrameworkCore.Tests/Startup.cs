using Microsoft.Extensions.DependencyInjection;
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
        var xunitTestOutputLoggerProvider = new XunitTestOutputLoggerProvider(accessor,
            (source, ll) => ll >= LogLevel.Debug);

        loggerFactory.AddProvider(xunitTestOutputLoggerProvider);
    }

    public void ConfigureServices(IServiceCollection services)
    {
        

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