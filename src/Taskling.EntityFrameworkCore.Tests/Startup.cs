using System;
using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Taskling.EntityFrameworkCore.Extensions;
using Taskling.EntityFrameworkCore.Tests.Enums;
using Taskling.EntityFrameworkCore.Tests.Helpers;
using Xunit.DependencyInjection;
using Xunit.DependencyInjection.Logging;
using Npgsql.Util;
 
namespace Taskling.EntityFrameworkCore.Tests;

public class Startup
{
    internal static TimeSpan QueryTimeout = new(0, 1, 0);
    public static readonly ConnectionTypeEnum ConnectionType = ConnectionTypeEnum.PostgreSQL;

    public void Configure(ILoggerFactory loggerFactory, ITestOutputHelperAccessor accessor)
    {
        var xunitTestOutputLoggerProvider = new XunitTestOutputLoggerProvider(accessor,
            (source, ll) => ll >= LogLevel.Debug);

        loggerFactory.AddProvider(xunitTestOutputLoggerProvider);
    }

    public void ConfigureServices(IServiceCollection services)
    {
        services.AddLogging(configure =>
        {
            configure.AddConsole().AddDebug();
            configure.SetMinimumLevel(LogLevel.Debug);
        });
        services.AddTransient<IExecutionsHelper, ExecutionsHelper>();
        services.AddTransient<IClientHelper, ClientHelper>();
        services.AddTransient<IBlocksHelper, BlocksHelper>();
        services.AddTaskling(cfg =>
        {
            cfg.WithDbContextOptions(eventArgs =>
            {
                switch (ConnectionType)
                {
                    case ConnectionTypeEnum.PostgreSQL:
                        eventArgs.Builder.UseNpgsql(eventArgs.ConnectionString);
                        break;
                    case ConnectionTypeEnum.Sqlite:
                        eventArgs.Builder.UseSqlite("DataSource=\"file::memory:?cache=shared\"").EnableDetailedErrors()
                            .ConfigureWarnings(i => i.Ignore(RelationalEventId.AmbientTransactionWarning,
                                InMemoryEventId.TransactionIgnoredWarning)).LogTo(i => Debug.Print(i), LogLevel.Error);
                        break;
                    case ConnectionTypeEnum.InMemory:
                        eventArgs.Builder.UseInMemoryDatabase("abcdef");
                        break;
                    case ConnectionTypeEnum.SqlServer:
                        eventArgs.Builder.UseSqlServer(eventArgs.ConnectionString);
                        break;
                    case ConnectionTypeEnum.MySql:
                        eventArgs.Builder.UseMySQL(eventArgs.ConnectionString);
                        break;
                    default: throw new NotImplementedException();
                }
            }).WithReader<TestTaskConfigurationReader>();
        });
    }

    public static string GetConnectionString()
    {
        switch (ConnectionType)
        {
            case ConnectionTypeEnum.PostgreSQL:
                return "Server=127.0.0.1;Port=5432;Database=TasklingDb;User Id=postgres;Password=password;";
            case ConnectionTypeEnum.SqlServer:
                return
                    "Server=(local);Database=TasklingDb;Encrypt=false; Application Name=Entity Tester;Trusted_Connection=True;";
            default:
                return "Server=localhost;Database=taskling;uid=root;";
        }
    }
}