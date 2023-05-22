using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Taskling.EntityFrameworkCore.AncilliaryServices;
using Taskling.EntityFrameworkCore.Tests.Helpers;
using Xunit.DependencyInjection;
using Xunit.DependencyInjection.Logging;

namespace Taskling.EntityFrameworkCore.Tests;

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