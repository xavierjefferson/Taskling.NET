using Taskling.InfrastructureContracts;
using Taskling.Tasks;

namespace Taskling;

public interface IConnectionStore
{
    void SetConnection(TaskId taskId, ClientConnectionSettings connectionSettings);
    ClientConnectionSettings GetConnection(TaskId taskId);
}