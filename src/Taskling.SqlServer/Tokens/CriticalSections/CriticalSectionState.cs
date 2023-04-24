using System.Text;

namespace Taskling.SqlServer.Tokens.CriticalSections;

internal class CriticalSectionState
{
    private int? _grantedToExecution;
    private bool _isGranted;

    private List<CriticalSectionQueueItem> _queue;

    public bool IsGranted
    {
        get => _isGranted;
        set
        {
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
            if (_grantedToExecution != value)
                HasBeenModified = true;

            _grantedToExecution = value;
        }
    }

    public bool HasBeenModified { get; private set; }

    public void StartTrackingModifications()
    {
        HasBeenModified = false;
    }

    public bool HasQueuedExecutions()
    {
        return _queue != null && _queue.Any();
    }

    public void SetQueue(string queueStr)
    {
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
        return _queue;
    }

    public void UpdateQueue(List<CriticalSectionQueueItem> queueDetails)
    {
        _queue = queueDetails;
        HasBeenModified = true;
    }

    public int? GetFirstExecutionIdInQueue()
    {
        if (_queue == null || !_queue.Any())
            return null;

        return _queue.OrderBy(x => x.Index).First().TaskExecutionId;
    }

    public void RemoveFirstInQueue()
    {
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
        return _queue.Any(x => x.TaskExecutionId == taskExecutionId);
    }

    public void AddToQueue(int taskExecutionId)
    {
        var index = 1;
        if (_queue.Any())
            index = _queue.Max(x => x.Index) + 1;

        _queue.Add(new CriticalSectionQueueItem(index, taskExecutionId));
        HasBeenModified = true;
    }
}