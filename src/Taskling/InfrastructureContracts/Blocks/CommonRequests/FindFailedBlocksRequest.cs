using System;
using Taskling.Enums;

namespace Taskling.InfrastructureContracts.Blocks.CommonRequests;

public class FindFailedBlocksRequest : StatusSpecificBlockRequestBase, ISearchableBlockRequest
{
    private static readonly int[] Statuses = { (int)BlockExecutionStatusEnum.Failed };

    public FindFailedBlocksRequest(TaskId taskId,
        long taskExecutionId,
        BlockTypeEnum blockType,
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