using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Taskling.Configuration;
using Taskling.EntityFrameworkCore.AncilliaryServices;
using Taskling.EntityFrameworkCore.Models;
using Taskling.InfrastructureContracts;

namespace Taskling.EntityFrameworkCore;

public class DbContextFactoryEx : IDbContextFactoryEx
{
    private readonly IDbContextConfigurator _dbContextConfigurator;

    private readonly ILogger<DbContextFactoryEx> _logger;
    private readonly IMemoryCache _memoryCache;
    private readonly object _mutex = new();
    private readonly ITaskConfigurationRepository _taskConfigurationRepository;

    public DbContextFactoryEx(IMemoryCache memoryCache,
        ILogger<DbContextFactoryEx> logger, IDbContextConfigurator dbContextConfigurator,
        ITaskConfigurationRepository taskConfigurationRepository)
    {
        _logger = logger;
        _dbContextConfigurator = dbContextConfigurator;
        _taskConfigurationRepository = taskConfigurationRepository;
        _memoryCache = memoryCache;
    }

    public TasklingDbContext GetDbContext(TaskId taskId)
    {
        lock (_mutex)
        {
            var key = $"options-{taskId.GetUniqueKey()}";
            var dbContextInfo = _memoryCache.GetOrCreate(key, cacheEntry =>
            {
                var taskConfiguration = _taskConfigurationRepository.GetTaskConfiguration(taskId);
                var commandTimeout = new TimeSpan(taskConfiguration.CommandTimeoutSeconds);
                var eventArgs = new TasklingDbContextEventArgs(new DbContextOptionsBuilder<TasklingDbContext>(), taskId,
                    taskConfiguration.ConnectionString);
                _dbContextConfigurator.Configure(eventArgs);
                var tmp = new DbContextInfo
                {
                    Options = eventArgs.Builder.Options,
                    CommandTimeout = commandTimeout,
                    First = true
                };
                cacheEntry.Value = tmp;
                cacheEntry.AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(10);
                return tmp;
            });

            var tasklingDbContext = new TasklingDbContext(dbContextInfo.Options);
            tasklingDbContext.Database.SetCommandTimeout(dbContextInfo.CommandTimeout);
            if (dbContextInfo.First)
            {
                tasklingDbContext.Database.EnsureCreated();
                dbContextInfo.First = false;
            }

            return tasklingDbContext;
        }
    }

    private class DbContextInfo
    {
        public DbContextOptions<TasklingDbContext> Options { get; set; }
        public TimeSpan CommandTimeout { get; set; }
        public bool First { get; set; }
    }
}