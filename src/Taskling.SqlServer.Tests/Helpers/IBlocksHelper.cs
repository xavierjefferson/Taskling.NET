﻿using System;
using System.Collections.Generic;
using Taskling.Blocks.Common;
using Taskling.Blocks.ListBlocks;
using Taskling.InfrastructureContracts;

namespace Taskling.SqlServer.Tests.Helpers;

public interface IBlocksHelper
{
    int GetListBlockItemCountByStatus(long blockId, ItemStatus status);
    long GetLastBlockId(TaskId taskId);
    List<ListBlockItem<T>> GetListBlockItems<T>(long blockId, ItemStatus status);
    void EnqueueForcedBlock(long blockId);
    void InsertPhantomDateRangeBlock(TaskId taskId, DateTime fromDate, DateTime toDate);
    void InsertPhantomNumericBlock(TaskId taskId, long fromId, long toId);
    void InsertPhantomListBlock(TaskId taskId);
    void InsertPhantomObjectBlock(TaskId taskId);
    int GetBlockCount(TaskId taskId);
    long InsertDateRangeBlock(int taskDefinitionId, DateTime fromDate, DateTime toDate);
    long InsertDateRangeBlock(int taskDefinitionId, DateTime fromDate, DateTime toDate, DateTime createdAt);
    long InsertNumericRangeBlock(int taskDefinitionId, long fromNumber, long toNumber, DateTime createdDate);
    long InsertListBlock(int taskDefinitionId, DateTime createdDate, string objectData = null);
    long InsertObjectBlock(int taskDefinitionId, DateTime createdDate, string objectData);

    long InsertBlockExecution(int taskExecutionId, long blockId, DateTime createdAt, DateTime? startedAt,
        DateTime? completedAt, BlockExecutionStatus executionStatus, int attempt = 1);

    void DeleteBlocks(string applicationName);

    int GetBlockExecutionCountByStatus(TaskId taskId,
        BlockExecutionStatus blockExecutionStatus);

    int GetBlockExecutionItemCount(long blockExecutionId);
}