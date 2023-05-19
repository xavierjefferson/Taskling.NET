using System.Reflection;
using System.Text;
using Microsoft.Extensions.Logging;
using Taskling.Extensions;

namespace Taskling.SqlServer.Tokens.CriticalSections;

internal class CriticalSectionState
{
    private readonly ILogger<CriticalSectionState> _logger;
    private int? _grantedToExecution;

    private bool _hasBeenModified;
    private bool _isGranted;

    private List<CriticalSectionQueueItem>? _queue;

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

    public int? GrantedToExecution
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

        return _queue != null && _queue.Any();
    }

    public void SetQueue(string queueStr)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _logger.Debug("a0175e5a-a4ea-4777-8ece-d0e1114a0805");
        _logger.Debug($"QueueString = {queueStr}");
        _queue = new List<CriticalSectionQueueItem>();
        if (!string.IsNullOrEmpty(queueStr))
        {
            var queueItems = queueStr.Split(new[] { '|' }, StringSplitOptions.RemoveEmptyEntries);
            foreach (var queueItem in queueItems)
            {
                var parts = queueItem.Split(',');
                var index = int.Parse(parts[0]);
                _queue.Add(new CriticalSectionQueueItem(index, int.Parse(parts[1])));
            }
        }
    }

    public string GetQueueString()
    {
        _logger.Debug("be8efa4e-4573-4151-a0b8-0bf517f10f98");
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));

        var sb = new StringBuilder();
        foreach (var queueItem in _queue)
        {
            if (queueItem.Index > 1)
                sb.Append("|");

            sb.Append(queueItem.Index + "," + queueItem.TaskExecutionId);
        }

        return sb.ToString();
    }

    public List<CriticalSectionQueueItem> GetQueue()
    {
        _logger.Debug("dcacc755-5b81-4fb1-8e08-a9ebdbe9f31a");
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));

        return _queue;
    }

    public void UpdateQueue(List<CriticalSectionQueueItem> queueDetails)
    {
        _logger.Debug("7c76666f-b4d4-4919-8c1f-b1b014344c0d");
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));

        _queue = queueDetails;
        HasBeenModified = true;
    }

    public int? GetFirstExecutionIdInQueue()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));

        if (_queue == null || !_queue.Any())
        {
            _logger.Debug("3f406e23-2a8d-4509-9b1d-be3232ea7a77");
            return null;
        }
        _logger.Debug("9a341c0b-e913-4bef-af9c-7df5563d5f32");
        return _queue.OrderBy(x => x.Index).First().TaskExecutionId;
    }

    public void RemoveFirstInQueue()
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));

        // remove the first element
        if (_queue != null && _queue.Any())
            _queue.RemoveAt(0);

        // reset the index values
        var index = 1;
        foreach (var item in _queue.OrderBy(x => x.Index))
        {
            item.Index = index;
            index++;
        }

        HasBeenModified = true;
    }

    public bool ExistsInQueue(int taskExecutionId)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));

        return _queue.Any(x => x.TaskExecutionId == taskExecutionId);
    }

    public void AddToQueue(int taskExecutionId)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));

        var index = 1;
        if (_queue.Any())
            index = _queue.Max(x => x.Index) + 1;

        _queue.Add(new CriticalSectionQueueItem(index, taskExecutionId));
        HasBeenModified = true;
    }
}