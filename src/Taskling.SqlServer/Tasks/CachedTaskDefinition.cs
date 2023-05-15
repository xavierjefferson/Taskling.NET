using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.SqlServer.Tasks;

internal class CachedTaskDefinition
{
    public TaskDefinition? TaskDefinition { get; set; }
    public DateTime CachedAt { get; set; }
}