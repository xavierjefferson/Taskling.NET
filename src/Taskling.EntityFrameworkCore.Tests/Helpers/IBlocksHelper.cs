using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Taskling.Blocks.ListBlocks;
using Taskling.Enums;
using Taskling.InfrastructureContracts;

namespace Taskling.EntityFrameworkCore.Tests.Helpers;

public interface IBlocksHelper
{
    int GetListBlockItemCountByStatus(long blockId, ItemStatusEnum status);
    long GetLastBlockId(TaskId taskId);
    List<ListBlockItem<T>> GetListBlockItems<T>(long blockId, ItemStatusEnum status, ILoggerFactory loggerFactory);
    void EnqueueForcedBlock(long blockId);
    void InsertPhantomDateRangeBlock(TaskId taskId, DateTime fromDate, DateTime toDate);
    void InsertPhantomNumericBlock(TaskId taskId, long fromId, long toId);
    void InsertPhantomListBlock(TaskId taskId);
    void InsertPhantomObjectBlock(TaskId taskId);
    int GetBlockCount(TaskId taskId);
    long InsertDateRangeBlock(long taskDefinitionId, DateTime fromDate, DateTime toDate);
    long InsertDateRangeBlock(long taskDefinitionId, DateTime fromDate, DateTime toDate, DateTime createdAt);
    long InsertNumericRangeBlock(long taskDefinitionId, long fromNumber, long toNumber, DateTime createdDate);
    long InsertListBlock(long taskDefinitionId, DateTime createdDate, string objectData = null);
    long InsertObjectBlock(long taskDefinitionId, DateTime createdDate, string objectData);

    long InsertBlockExecution(long taskExecutionId, long blockId, DateTime createdAt, DateTime? startedAt,
        DateTime? completedAt, BlockExecutionStatusEnum executionStatus, int attempt = 1);

    void DeleteBlocks(string applicationName);

    int GetBlockExecutionCountByStatus(TaskId taskId,
        BlockExecutionStatusEnum blockExecutionStatus);

    int GetBlockExecutionItemCount(long blockExecutionId);
}