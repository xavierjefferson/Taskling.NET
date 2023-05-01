using System;
using Taskling.Blocks.Common;

namespace Taskling.InfrastructureContracts.Blocks.CommonRequests;

public abstract class StatusSpecificBlockRequestBase : BlockRequestBase
{
    protected StatusSpecificBlockRequestBase(TaskId taskId, int taskExecutionId, BlockType blockType) : base(taskId,
        taskExecutionId, blockType)
    {
    }

    protected StatusSpecificBlockRequestBase(TaskId taskId, int taskExecutionId, BlockType blockType,
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