using Microsoft.EntityFrameworkCore;
using Taskling.InfrastructureContracts;
using Taskling.SqlServer.Models;
using Taskling;
using Taskling.SqlServer.Blocks;

namespace ConsoleApp1
{
    internal class Program
    {
        static void Main(string[] args)
        {
            //const string conn = "Server=.;Database=TasklingDb;Trusted_Connection=True;";
            //var builder = new DbContextOptionsBuilder<TasklingDbContext>();
            //// var clientConnectionSettings = ConnectionStore.Instance.GetConnection(conn);
            //builder.UseSqlServer(conn, options =>
            //{


            //    //       options.EnableRetryOnFailure();
            //});


            //var tasklingDbContext = new TasklingDbContext(builder.Options);
            //var a = new BlockQueryRequestBase(); 
            //var t = DeadBlocksQueryBuilder.GetBlocksInner(tasklingDbContext, a, blockType).Result;
        }
    }
}