using System;
using Taskling.Enums;

namespace Taskling.InfrastructureContracts.Blocks.CommonRequests;

public class FindDeadBlocksRequest : StatusSpecificBlockRequestBase, ISearchableBlockRequest
{
    private static readonly int[] Statuses =
        { (int)BlockExecutionStatusEnum.NotStarted, (int)BlockExecutionStatusEnum.Started };

    public FindDeadBlocksRequest(TaskId taskId,
        long taskExecutionId,
        BlockTypeEnum blockType,
        DateTime searchPeriodBegin,
        DateTime searchPeriodEnd,
        int blockCountLimit,
        TaskDeathModeEnum taskDeathMode,
        int retryLimit)
        : base(taskId, taskExecutionId, blockType)
    {
        SearchPeriodBegin = searchPeriodBegin;
        SearchPeriodEnd = searchPeriodEnd;
        BlockCountLimit = blockCountLimit;
        TaskDeathMode = taskDeathMode;
        RetryLimit = retryLimit;
    }

    public TaskDeathModeEnum TaskDeathMode { get; set; }

    public override int[] GetMatchingStatuses()
    {
        return Statuses;
    }
}