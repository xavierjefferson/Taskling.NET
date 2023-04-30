using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Taskling.Blocks.Factories;
using Taskling.CleanUp;
using Taskling.Contexts;
using Taskling.ExecutionContext;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.CleanUp;
using Taskling.InfrastructureContracts.CriticalSections;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.Blocks;
using Taskling.SqlServer.Events;
using Taskling.SqlServer.TaskExecution;
using Taskling.SqlServer.Tasks;
using Taskling.SqlServer.Tests.Helpers;
using Taskling.SqlServer.Tokens;
using Taskling.SqlServer.Tokens.CriticalSections;
using Taskling.SqlServer.Tokens.Executions;
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

public static class TasklingServiceCollectionExtensions
{
    public static void AddTaskling(this IServiceCollection services)
    {
        services.AddSingleton<TasklingOptions>(new TasklingOptions());
        services.AddSingleton<ITaskRepository, TaskRepository>();
        services.AddScoped<ITaskExecutionRepository, TaskExecutionRepository>();
        services.AddScoped<IExecutionTokenRepository, ExecutionTokenRepository>();
        services.AddScoped<IListBlockRepository, ListBlockRepository>();
        services.AddScoped<ICommonTokenRepository, CommonTokenRepository>();
        services.AddScoped<IEventsRepository, EventsRepository>();
        services.AddScoped<ICriticalSectionRepository, CriticalSectionRepository>();
        services.AddScoped<IBlockRepository, BlockRepository>();
        services.AddScoped<IRangeBlockRepository, RangeBlockRepository>();
        services.AddScoped<IListBlockRepository, ListBlockRepository>(); ;
        services.AddScoped<IObjectBlockRepository, ObjectBlockRepository>();
        services.AddScoped<ICleanUpRepository, CleanUpRepository>();
        services.AddTransient<ITaskExecutionContext, 
            TaskExecutionContext>();


        services.AddScoped<IBlockFactory, BlockFactory>();
        services.AddScoped<ICleanUpService, CleanUpService>();
       
        services.AddSingleton<IConnectionStore, ConnectionStore>();
    }
}