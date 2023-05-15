using Microsoft.Extensions.DependencyInjection;
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
using Taskling.SqlServer.Tokens;
using Taskling.SqlServer.Tokens.CriticalSections;
using Taskling.SqlServer.Tokens.Executions;

namespace Taskling.SqlServer;

public static class TasklingServiceCollectionExtensions
{
    public static IServiceCollection AddTaskling(this IServiceCollection services)
    {
        services.AddSingleton(new TasklingOptions());
        services.AddSingleton<ITaskRepository, TaskRepository>();
        services.AddScoped<ITaskExecutionRepository, TaskExecutionRepository>();
        services.AddScoped<IExecutionTokenRepository, ExecutionTokenRepository>();
        services.AddScoped<IListBlockRepository, ListBlockRepository>();
        services.AddScoped<ICommonTokenRepository, CommonTokenRepository>();
        services.AddScoped<IEventsRepository, EventsRepository>();
        services.AddScoped<ICriticalSectionRepository, CriticalSectionRepository>();
        services.AddScoped<IBlockRepository, BlockRepository>();
        services.AddScoped<IRangeBlockRepository, RangeBlockRepository>();
        services.AddScoped<IListBlockRepository, ListBlockRepository>();
        ;
        services.AddScoped<IObjectBlockRepository, ObjectBlockRepository>();
        services.AddScoped<ICleanUpRepository, CleanUpRepository>();
        services.AddTransient<ITaskExecutionContext,
            TaskExecutionContext>();


        services.AddScoped<IBlockFactory, BlockFactory>();
        services.AddScoped<ICleanUpService, CleanUpService>();
        services.AddMemoryCache();
        services.AddSingleton<IConnectionStore, ConnectionStore>();
        return services;
    }
}