using System;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Caching.Memory;
using Taskling.InfrastructureContracts;
using Taskling.SqlServer.AncilliaryServices;
using Taskling.SqlServer.Models;

namespace Taskling.SqlServer.Tests;

public class DbContextFactoryEx : IDbContextFactoryEx
{
    private readonly IConnectionStore _connectionStore;
    private readonly IMemoryCache _memoryCache;

    public DbContextFactoryEx(IConnectionStore connectionStore, IMemoryCache memoryCache)
    {
        _connectionStore = connectionStore;
        _memoryCache = memoryCache;
    }

    public TasklingDbContext GetDbContext(TaskId taskId)
    {
        var key = $"options-{taskId.GetUniqueKey()}";
        var dbContextInfo = _memoryCache.GetOrCreate(key, (cacheEntry) =>
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
        //        var dbContextInfo = _memoryCache.ge.GetOrCreate<DbContextInfo>(key, entry => { },
        //        return (a, b) =>
        //                {
        //                    var clientConnectionSettings = _connectionStore.GetConnection(taskId);
        //                    var builder = DbContextOptionsHelper.GetDbContextOptionsBuilder(
        //                        clientConnectionSettings.ConnectionString,
        //                        clientConnectionSettings.QueryTimeoutSeconds);
        //                    var dbContextInfo = new DbContextInfo
        //                    { Options = builder.Options, Timespan = clientConnectionSettings.QueryTimeout };
        //                    a.Value = dbContextInfo;
        //                    a.AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(1);
        //                    return dbContextInfo;
        //                };
        //    });
        //            .Get<DbContextInfo>(key);
        //        if (dbContextInfo == null)
        //        {
        //            var clientConnectionSettings = _connectionStore.GetConnection(taskId);
        //    var builder = DbContextOptionsHelper.GetDbContextOptionsBuilder(clientConnectionSettings.ConnectionString,
        //        clientConnectionSettings.QueryTimeoutSeconds);
        //    dbContextInfo = new DbContextInfo
        //                { Options = builder.Options, Timespan = clientConnectionSettings.QueryTimeout
        //};
        //_memoryCache.Set(key, dbContextInfo, TimeSpan.FromMinutes(1));
        //        }

        var tasklingDbContext = new TasklingDbContext(dbContextInfo.Options);
        tasklingDbContext.Database.SetCommandTimeout(dbContextInfo.Timespan);
        return tasklingDbContext;
    }

    private class DbContextInfo
    {
        public DbContextOptions<TasklingDbContext> Options { get; set; }
        public TimeSpan Timespan { get; set; }
    }
}