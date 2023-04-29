namespace Taskling.SqlServer.Blocks.Models;

public class BlocksOfTaskQueryParams
{
    public List<int> StatusesToMatch { get; set; } = new List<int>();
    public int NotStarted { get; set; }
    public int Started { get; set; }
    public int Failed { get; set; }
}