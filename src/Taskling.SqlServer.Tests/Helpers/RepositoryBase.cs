using System.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Taskling.SqlServer.Models;

namespace Taskling.SqlServer.Tests.Helpers;

public abstract class RepositoryBase
{
    private static bool created;
    private static readonly object _mutex = new();

    public TasklingDbContext GetDbContext()
    {
        lock (_mutex)
        {
            var builder = new DbContextOptionsBuilder<TasklingDbContext>();

            builder.UseSqlServer(TestConstants.TestConnectionString);


            var tasklingDbContext = new TasklingDbContext(builder.Options);

            if (!created)
            {
                tasklingDbContext.Database.EnsureCreated();
                created = true;
            }

            return tasklingDbContext;
        }
    }
}