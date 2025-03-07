﻿using System;
using Taskling.Enums;

namespace Taskling.Tasks;

public abstract class TaskExecutionMetaBase
{
    public TaskExecutionMetaBase(DateTime startedAt,
        DateTime? completedAt,
        TaskExecutionStatusEnum status,
        Guid referenceValue)
    {
        StartedAt = startedAt;
        CompletedAt = completedAt;
        Status = status;
        ReferenceValue = referenceValue;
    }

    public DateTime StartedAt { get; set; }
    public DateTime? CompletedAt { get; set; }
    public TaskExecutionStatusEnum Status { get; set; }
    public Guid ReferenceValue { get; set; }
}

public class TaskExecutionMeta : TaskExecutionMetaBase
{
    public TaskExecutionMeta(DateTime startedAt,
        DateTime? completedAt,
        TaskExecutionStatusEnum status,
        Guid referenceValue) : base(startedAt, completedAt, status, referenceValue)
    {
    }
}

public class TaskExecutionMeta<TaskExecutionHeader> : TaskExecutionMetaBase
{
    public TaskExecutionMeta(DateTime startedAt,
        DateTime? completedAt,
        TaskExecutionStatusEnum status,
        TaskExecutionHeader header,
        Guid referenceValue) : base(startedAt, completedAt, status, referenceValue)
    {
        StartedAt = startedAt;
        CompletedAt = completedAt;
        Status = status;
        Header = header;
    }

    public TaskExecutionHeader Header { get; set; }
}