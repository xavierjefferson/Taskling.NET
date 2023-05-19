using System;
using Microsoft.EntityFrameworkCore;
using Taskling.SqlServer.Models;
using Taskling.SqlServer.Tests.Enums;
using Taskling.SqlServer.Tests.Helpers;

internal static class DbContextOptionsHelper
{
    private static readonly object _mutex = new();
    private static bool created;

    public static TasklingDbContext GetDbContext()
    {
        //_logger.LogDebug($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {Constants.CheckpointName}");
        lock (_mutex)
        {
            var builder = GetDbContextOptionsBuilder(TestConstants.GetTestConnectionString(), null);

            var tasklingDbContext = new TasklingDbContext(builder.Options);

            if (!created)
            {
                tasklingDbContext.Database.EnsureCreated();
                created = true;
            }

            return tasklingDbContext;
        }
    }

    public static DbContextOptionsBuilder<TasklingDbContext> GetDbContextOptionsBuilder(string connectionString,
        int? queryTimeoutSeconds)
    {
        //_logger.LogDebug($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {Constants.CheckpointName}");
        var builder = new DbContextOptionsBuilder<TasklingDbContext>();
        switch (TestConstants.ConnectionType)
        {
            case ConnectionTypeEnum.InMemory:

                builder.UseInMemoryDatabase("abcdef");
                //connectionString, options =>
                //{
                //    if (queryTimeoutSeconds != null)
                //        options.CommandTimeout(queryTimeoutSeconds.Value);

                //    //       options.EnableRetryOnFailure();
                //});
                break;
            case ConnectionTypeEnum.SqlServer:
                builder.UseSqlServer(connectionString, options =>
                {
                    if (queryTimeoutSeconds != null)
                        options.CommandTimeout(queryTimeoutSeconds.Value);

                    //       options.EnableRetryOnFailure();
                });
                break;
            case ConnectionTypeEnum.MySql:
                builder.UseMySQL(connectionString,
                    options =>
                    {
                        if (queryTimeoutSeconds != null)
                            options.CommandTimeout(queryTimeoutSeconds.Value);
                    });
                break;
            default: throw new NotImplementedException();
        }

        return builder;
    }
}