﻿using Microsoft.Extensions.DependencyInjection;
using Taskling.Blocks.Factories;
using Taskling.CleanUp;
using Taskling.Contexts;
using Taskling.EntityFrameworkCore.Blocks;
using Taskling.EntityFrameworkCore.Events;
using Taskling.EntityFrameworkCore.TaskExecution;
using Taskling.EntityFrameworkCore.Tasks;
using Taskling.EntityFrameworkCore.Tokens;
using Taskling.EntityFrameworkCore.Tokens.CriticalSections;
using Taskling.EntityFrameworkCore.Tokens.Executions;
using Taskling.ExecutionContext;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.CleanUp;
using Taskling.InfrastructureContracts.CriticalSections;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.Retries;

namespace Taskling.EntityFrameworkCore;

public static class TasklingServiceCollectionExtensions
{
    public static IServiceCollection AddTaskling(this IServiceCollection services)
    {
        services.AddSingleton(new StartupOptions());
        services.AddSingleton<IRetryService, RetryService>();
        services.AddSingleton<ITaskRepository, TaskRepository>();
        services.AddScoped<ITaskExecutionRepository, TaskExecutionRepository>();
        services.AddScoped<IExecutionTokenRepository, ExecutionTokenRepository>();
        services.AddScoped<IExecutionTokenHelper, ExecutionTokenHelper>();
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