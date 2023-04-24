﻿using System;
using Taskling.Blocks.Common;
using Taskling.Tasks;

namespace Taskling.InfrastructureContracts.Blocks.CommonRequests;

public class FindDeadBlocksRequest : BlockRequestBase
{
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

    public DateTime SearchPeriodBegin { get; set; }
    public DateTime SearchPeriodEnd { get; set; }
    public int BlockCountLimit { get; set; }
    public TaskDeathMode TaskDeathMode { get; set; }
    public int RetryLimit { get; set; }
}