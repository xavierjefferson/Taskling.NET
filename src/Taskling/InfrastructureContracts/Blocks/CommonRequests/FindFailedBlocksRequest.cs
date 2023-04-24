using System;
using Taskling.Blocks.Common;

namespace Taskling.InfrastructureContracts.Blocks.CommonRequests;

public class FindFailedBlocksRequest : BlockRequestBase
{
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

    public DateTime SearchPeriodBegin { get; set; }
    public DateTime SearchPeriodEnd { get; set; }
    public int BlockCountLimit { get; set; }
    public int RetryLimit { get; set; }
}