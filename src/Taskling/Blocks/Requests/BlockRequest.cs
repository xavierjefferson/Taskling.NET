using System;
using Taskling.Blocks.Common;
using Taskling.InfrastructureContracts;
using Taskling.Tasks;

namespace Taskling.Blocks.Requests;

public abstract class BlockRequest
{
    public BlockRequest(TaskId taskId)
    {
        TaskId = taskId;
    }

    public TaskId TaskId { get; }

    public BlockType BlockType { get; protected set; }

    public bool ReprocessDeadTasks { get; set; }

    public bool ReprocessFailedTasks { get; set; }

    public int MaxBlocks { get; set; }

    public long TaskExecutionId { get; set; }

    public ReprocessOption ReprocessOption { get; set; }

    public int DeadTaskRetryLimit { get; set; }

    public int FailedTaskRetryLimit { get; set; }

    //    //public string ApplicationName { get; set; }
    public Guid ReprocessReferenceValue { get; set; }

    //    //public string TaskName { get; set; }
    public TaskDeathMode TaskDeathMode { get; set; }

    public TimeSpan DeadTaskDetectionRange { get; set; }

    public TimeSpan FailedTaskDetectionRange { get; set; }

    public TimeSpan KeepAliveDeathThreshold { get; set; }

    public TimeSpan OverrideDeathThreshold { get; set; }
}