using System;
using Taskling.Enums;
using Taskling.InfrastructureContracts;

namespace Taskling.Blocks.Requests;

public abstract class BlockRequest
{
    public BlockRequest(TaskId taskId)
    {
        TaskId = taskId;
    }

    public TaskId TaskId { get; }

    public BlockTypeEnum BlockType { get; protected set; }

    public bool ReprocessDeadTasks { get; set; }

    public bool ReprocessFailedTasks { get; set; }

    public int MaxBlocks { get; set; }

    public long TaskExecutionId { get; set; }

    public ReprocessOptionEnum ReprocessOption { get; set; }

    public int DeadTaskRetryLimit { get; set; }

    public int FailedTaskRetryLimit { get; set; }

    //    //public string ApplicationName { get; set; }
    public Guid ReprocessReferenceValue { get; set; }

    //    //public string TaskName { get; set; }
    public TaskDeathModeEnum TaskDeathMode { get; set; }

    public TimeSpan DeadTaskDetectionRange { get; set; }

    public TimeSpan FailedTaskDetectionRange { get; set; }

    public TimeSpan KeepAliveDeathThreshold { get; set; }

    public TimeSpan OverrideDeathThreshold { get; set; }
}