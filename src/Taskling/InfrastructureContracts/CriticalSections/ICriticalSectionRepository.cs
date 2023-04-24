using System.Threading.Tasks;

namespace Taskling.InfrastructureContracts.CriticalSections;

public interface ICriticalSectionRepository
{
    Task<StartCriticalSectionResponse> StartAsync(StartCriticalSectionRequest startRequest);
    Task<CompleteCriticalSectionResponse> CompleteAsync(CompleteCriticalSectionRequest completeCriticalSectionRequest);
}