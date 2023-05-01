using System;
using Taskling.Blocks.Common;

namespace Taskling.InfrastructureContracts.Blocks.CommonRequests;

public class FindFailedBlocksRequest : StatusSpecificBlockRequestBase, ISearchableBlockRequest
{
    private static readonly int[] Statuses = { (int)BlockExecutionStatus.Failed };

    public FindFailedBlocksRequest(TaskId taskId,
        int taskExecutionId,
        BlockType blockType,
        DateTime searchPeriodBegin,
        DateTime searchPeriodEnd,
        int blockCountLimit,
        int retryLimit)
        : base(taskId, taskExecutionId, blockType)
    {
        SearchPeriodBegin = searchPeriodBegin;
        SearchPeriodEnd = searchPeriodEnd;
        BlockCountLimit = blockCountLimit;
        RetryLimit = retryLimit;
    }

    public override int[] GetMatchingStatuses()
    {
        return Statuses;
    }
}