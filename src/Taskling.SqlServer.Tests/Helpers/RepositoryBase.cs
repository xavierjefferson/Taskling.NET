using Taskling.SqlServer.Models;

namespace Taskling.SqlServer.Tests.Helpers;

public abstract class RepositoryBase
{
    public TasklingDbContext GetDbContext()
    {
        return DbContextOptionsHelper.GetDbContext();
    }
}