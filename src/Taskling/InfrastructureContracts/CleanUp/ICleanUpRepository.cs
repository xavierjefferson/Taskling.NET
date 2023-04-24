using System.Threading.Tasks;

namespace Taskling.InfrastructureContracts.CleanUp;

public interface ICleanUpRepository
{
    Task<bool> CleanOldDataAsync(CleanUpRequest cleanUpRequest);
}