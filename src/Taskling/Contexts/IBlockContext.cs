using System.Threading.Tasks;

namespace Taskling.Contexts;

public interface IBlockContext
{
    long ForcedBlockQueueId { get; }
    Task StartAsync();
    Task CompleteAsync();
    Task FailedAsync();
    Task FailedAsync(string message);

    void Complete();
    void Failed(string message);
    void Start();
}