using System;
using Taskling.Enums;

namespace Taskling.InfrastructureContracts.Blocks.CommonRequests;

public abstract class StatusSpecificBlockRequestBase : BlockRequestBase
{
    protected StatusSpecificBlockRequestBase(TaskId taskId, long taskExecutionId, BlockTypeEnum blockType) : base(
        taskId,
        taskExecutionId, blockType)
    {
    }

    protected StatusSpecificBlockRequestBase(TaskId taskId, long taskExecutionId, BlockTypeEnum blockType,
        long blockExecutionId) : base(taskId, taskExecutionId, blockType, blockExecutionId)
    {
    }

    public int AttemptLimit => RetryLimit + 1;

    public DateTime SearchPeriodBegin { get; set; }
    public DateTime SearchPeriodEnd { get; set; }
    public int BlockCountLimit { get; set; }
    public int RetryLimit { get; set; }

    public abstract int[] GetMatchingStatuses();
}