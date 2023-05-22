using Microsoft.Extensions.Logging;
using Taskling.Extensions;

namespace Taskling.EntityFrameworkCore.Tokens.CriticalSections;

internal class CriticalSectionState
{
    private readonly ILogger<CriticalSectionState> _logger;
    private long? _grantedToExecution;

    private bool _hasBeenModified;
    private bool _isGranted;
    private Queue<CriticalSectionQueueItem> _queue = new();

    //private List<CriticalSectionQueueItem>? _queue;

    public CriticalSectionState(ILogger<CriticalSectionState> logger)
    {
        _logger = logger;
    }

    public bool IsGranted
    {
        get => _isGranted;
        set
        {
            _logger.LogDebug($"{nameof(IsGranted)} set to {value}");

            if (_isGranted != value)
                HasBeenModified = true;

            _isGranted = value;
        }
    }

    public long? GrantedToExecution
    {
        get => _grantedToExecution;
        set
        {
            _logger.LogDebug($"{nameof(GrantedToExecution)} set to {value}");

            if (_grantedToExecution != value)
                HasBeenModified = true;

            _grantedToExecution = value;
        }
    }

    public bool HasBeenModified
    {
        get => _hasBeenModified;
        private set
        {
            _hasBeenModified = value;
            _logger.LogDebug($"{nameof(HasBeenModified)} set to {value}");
        }
    }

    public void StartTrackingModifications()
    {
        HasBeenModified = false;
    }

    public bool HasQueuedExecutions()
    {
        return _queue != null && _queue.Any();
    }

    public void SetQueue(string? queueStr)
    {
        _logger.LogDebug($"QueueString = {queueStr}");
        _queue = new Queue<CriticalSectionQueueItem>(CriticalSectionQueueSerializer.Deserialize(queueStr));
    }

    public string GetQueueString()
    {
        return CriticalSectionQueueSerializer.Serialize(_queue.ToArray());
    }

    public List<CriticalSectionQueueItem> GetQueue()
    {
        return _queue.ToList();
    }

    public void UpdateQueue(List<CriticalSectionQueueItem> queueDetails)
    {
        _queue = new Queue<CriticalSectionQueueItem>(queueDetails);
        HasBeenModified = true;
    }

    public long? GetFirstExecutionIdInQueue()
    {
        return _queue?.Peek()?.TaskExecutionId;
    }

    public void RemoveFirstInQueue()
    {
        _queue.Dequeue();
        HasBeenModified = true;
    }

    public bool ExistsInQueue(long taskExecutionId)
    {
        return _queue != null && _queue.Any(i => i.TaskExecutionId == taskExecutionId);
    }

    public void AddToQueue(long taskExecutionId)
    {
        _queue.Enqueue(new CriticalSectionQueueItem(taskExecutionId));
        HasBeenModified = true;
    }
}