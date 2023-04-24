using System;
using System.Threading.Tasks;

namespace Taskling.Contexts;

public interface ICriticalSectionContext : IDisposable
{
    bool IsActive();
    Task<bool> TryStartAsync();
    Task<bool> TryStartAsync(TimeSpan retryInterval, int numberOfAttempts);
    Task CompleteAsync();
}