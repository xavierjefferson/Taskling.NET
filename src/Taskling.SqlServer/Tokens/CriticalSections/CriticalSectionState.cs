using System.Reflection;
using System.Text;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Taskling.Extensions;

namespace Taskling.SqlServer.Tokens.CriticalSections;

internal class CriticalSectionState
{
    private Queue<CriticalSectionQueueItem> _queue = new Queue<CriticalSectionQueueItem>();
    private readonly ILogger<CriticalSectionState> _logger;
    private long? _grantedToExecution;

    private bool _hasBeenModified;
    private bool _isGranted;

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
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));

        HasBeenModified = false;
    }

    public bool HasQueuedExecutions()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return this._queue != null && this._queue.Any();
    }

    public void SetQueue(string? queueStr)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _logger.Debug("a0175e5a-a4ea-4777-8ece-d0e1114a0805");
        _logger.Debug($"QueueString = {queueStr}");
        _queue = new Queue<CriticalSectionQueueItem>(CsQueueSerializer.Deserialize(queueStr));
    }

    public string GetQueueString()
    {
        _logger.Debug("be8efa4e-4573-4151-a0b8-0bf517f10f98");
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return CsQueueSerializer.Serialize(_queue.ToArray());
    }

    public List<CriticalSectionQueueItem> GetQueue()
    {
        _logger.Debug("dcacc755-5b81-4fb1-8e08-a9ebdbe9f31a");
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));

        return _queue.ToList();
    }

    public void UpdateQueue(List<CriticalSectionQueueItem> queueDetails)
    {
        _logger.Debug("7c76666f-b4d4-4919-8c1f-b1b014344c0d");
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _queue = new Queue<CriticalSectionQueueItem>(queueDetails);
        HasBeenModified = true;
    }

    public long? GetFirstExecutionIdInQueue()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));

        return _queue?.Peek()?.TaskExecutionId;

    }

    public void RemoveFirstInQueue()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _queue.Dequeue();
        HasBeenModified = true;
    }

    public bool ExistsInQueue(long taskExecutionId)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return _queue != null && _queue.Any(i => i.TaskExecutionId == taskExecutionId);
    }

    public void AddToQueue(long taskExecutionId)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _queue.Enqueue(new CriticalSectionQueueItem(taskExecutionId));
        HasBeenModified = true;
    }
}

