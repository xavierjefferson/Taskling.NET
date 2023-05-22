using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.EntityFrameworkCore.Tasks;

internal class CachedTaskDefinition
{
    public TaskDefinition? TaskDefinition { get; set; }
    public DateTime CachedAt { get; set; }
}