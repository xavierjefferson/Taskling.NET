﻿using System;
using Taskling.Tasks;

namespace Taskling.InfrastructureContracts.TaskExecution;

public class TaskExecutionStartRequest : RequestBase
{
    public TaskExecutionStartRequest(TaskId taskId,
        TaskDeathMode taskDeathMode,
        int concurrencyLimit,
        int failedTaskRetryLimit,
        int deadTaskRetryLimit
    )
        : base(taskId)
    {
        TaskDeathMode = taskDeathMode;
        ConcurrencyLimit = concurrencyLimit;
        FailedTaskRetryLimit = failedTaskRetryLimit;
        DeadTaskRetryLimit = deadTaskRetryLimit;
    }

    public string TasklingVersion { get; set; }
    public TaskDeathMode TaskDeathMode { get; set; }
    public TimeSpan? OverrideThreshold { get; set; }
    public TimeSpan? KeepAliveInterval { get; set; }
    public TimeSpan? KeepAliveDeathThreshold { get; set; }
    public Guid ReferenceValue { get; set; }
    public int ConcurrencyLimit { get; set; }
    public int FailedTaskRetryLimit { get; set; }
    public int DeadTaskRetryLimit { get; set; }
    public string TaskExecutionHeader { get; set; }
}