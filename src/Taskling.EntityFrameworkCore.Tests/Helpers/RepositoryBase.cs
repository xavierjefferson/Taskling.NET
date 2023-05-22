using Taskling.EntityFrameworkCore.Models;

namespace Taskling.EntityFrameworkCore.Tests.Helpers;

public abstract class RepositoryBase
{
    public TasklingDbContext GetDbContext()
    {
        return DbContextOptionsHelper.GetDbContext();
    }
}