using System;
using Taskling.Blocks.Common;
using Taskling.Tasks;

namespace Taskling.InfrastructureContracts.Blocks.CommonRequests;

public class FindDeadBlocksRequest : StatusSpecificBlockRequestBase, ISearchableBlockRequest
{
    private static readonly int[] Statuses =
        { (int)BlockExecutionStatus.NotStarted, (int)BlockExecutionStatus.Started };

    public FindDeadBlocksRequest(TaskId taskId,
        int taskExecutionId,
        BlockType blockType,
        DateTime searchPeriodBegin,
        DateTime searchPeriodEnd,
        int blockCountLimit,
        TaskDeathMode taskDeathMode,
        int retryLimit)
        : base(taskId, taskExecutionId, blockType)
    {
        SearchPeriodBegin = searchPeriodBegin;
        SearchPeriodEnd = searchPeriodEnd;
        BlockCountLimit = blockCountLimit;
        TaskDeathMode = taskDeathMode;
        RetryLimit = retryLimit;
    }


    public TaskDeathMode TaskDeathMode { get; set; }

    public override int[] GetMatchingStatuses()
    {
        return Statuses;
    }
}