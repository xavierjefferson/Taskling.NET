using Taskling.Enums;

namespace Taskling.EntityFrameworkCore.Tokens;

public class TaskExecutionState
{
    public long TaskExecutionId { get; set; }
    public DateTime StartedAt { get; set; }
    public DateTime? CompletedAt { get; set; }
    public DateTime? LastKeepAlive { get; set; }
    public TaskDeathModeEnum TaskDeathMode { get; set; }
    public TimeSpan? OverrideThreshold { get; set; }
    public TimeSpan? KeepAliveInterval { get; set; }
    public TimeSpan? KeepAliveDeathThreshold { get; set; }
    public DateTime CurrentDateTime { get; set; }
    public int QueueIndex { get; set; }

    public bool HasExpired()
    {
        var taskExecutionState = this;
        if (taskExecutionState.CompletedAt.HasValue)
            return true;

        if (taskExecutionState.TaskDeathMode == TaskDeathModeEnum.KeepAlive)
        {
            if (!taskExecutionState.LastKeepAlive.HasValue)
                return true;

            var lastKeepAliveDiff = taskExecutionState.CurrentDateTime - taskExecutionState.LastKeepAlive.Value;
            if (lastKeepAliveDiff > taskExecutionState.KeepAliveDeathThreshold)
                return true;

            return false;
        }

        var activePeriod = taskExecutionState.CurrentDateTime - taskExecutionState.StartedAt;
        if (activePeriod > taskExecutionState.OverrideThreshold)
            return true;

        return false;
    }
}