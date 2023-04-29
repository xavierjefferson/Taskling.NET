using Microsoft.EntityFrameworkCore;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Taskling.InfrastructureContracts;
using Taskling.SqlServer.Models;

namespace Taskling.SqlServer.Tests.Helpers;

public abstract class RepositoryBase
{
    public TasklingDbContext GetDbContext()
    {
        var builder = new DbContextOptionsBuilder<TasklingDbContext>();
        
        builder.UseSqlServer(TestConstants.TestConnectionString);
        

        var tasklingDbContext = new TasklingDbContext(builder.Options);
        
        return tasklingDbContext;
    }
    public SqlConnection GetConnection()
    {
        var connection = new SqlConnection(TestConstants.TestConnectionString);
        connection.Open();
        return connection;
    }
}