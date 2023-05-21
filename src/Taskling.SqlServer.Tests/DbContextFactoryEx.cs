using System;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Taskling.InfrastructureContracts;
using Taskling.SqlServer.AncilliaryServices;
using Taskling.SqlServer.Models;

namespace Taskling.SqlServer.Tests;

public class DbContextFactoryEx : IDbContextFactoryEx
{
    private static bool first = true;
    private readonly IConnectionStore _connectionStore;
    private readonly ILogger<DbContextFactoryEx> _logger;
    private readonly IMemoryCache _memoryCache;
    private readonly object mutex = new();

    public DbContextFactoryEx(IConnectionStore connectionStore, IMemoryCache memoryCache,
        ILogger<DbContextFactoryEx> logger)
    {
        _logger = logger;
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _connectionStore = connectionStore;
        _memoryCache = memoryCache;
        //
    }

    public TasklingDbContext GetDbContext(TaskId taskId)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        lock (mutex)
        {
            var key = $"options-{taskId.GetUniqueKey()}";
            var dbContextInfo = _memoryCache.GetOrCreate(key, cacheEntry =>
            {
                var clientConnectionSettings = _connectionStore.GetConnection(taskId);
                var builder = DbContextOptionsHelper.GetDbContextOptionsBuilder(
                    clientConnectionSettings.ConnectionString,
                    clientConnectionSettings.QueryTimeoutSeconds);
                var dbContextInfo2 = new DbContextInfo
                    { Options = builder.Options, Timespan = clientConnectionSettings.QueryTimeout };
                cacheEntry.Value = dbContextInfo2;
                cacheEntry.AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(1);
                return dbContextInfo2;
            });

            var tasklingDbContext = new TasklingDbContext(dbContextInfo.Options);
            tasklingDbContext.Database.SetCommandTimeout(dbContextInfo.Timespan);
            if (first)
            {
                tasklingDbContext.Database.EnsureCreated();
                first = false;
            }

            return tasklingDbContext;
        }
    }

    private class DbContextInfo
    {
        public DbContextOptions<TasklingDbContext> Options { get; set; }
        public TimeSpan Timespan { get; set; }
    }
}