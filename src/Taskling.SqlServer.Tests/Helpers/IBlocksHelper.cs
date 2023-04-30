using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using Taskling.Blocks.Common;
using Taskling.Blocks.ListBlocks;
using Taskling.SqlServer.Models;

namespace Taskling.SqlServer.Tests.Helpers;

public interface IBlocksHelper
{
    int GetListBlockItemCountByStatus(long blockId, ItemStatus status);
    long GetLastBlockId(string applicationName, string taskName);
    List<ListBlockItem<T>> GetListBlockItems<T>(long blockId, ItemStatus status);
    void EnqueueForcedBlock(long blockId);
    void InsertPhantomDateRangeBlock(string applicationName, string taskName, DateTime fromDate, DateTime toDate);
    void InsertPhantomNumericBlock(string applicationName, string taskName, long fromId, long toId);
    void InsertPhantomListBlock(string applicationName, string taskName);
    void InsertPhantomObjectBlock(string applicationName, string taskName);
    int GetBlockCount(string applicationName, string taskName);
    long InsertDateRangeBlock(int taskDefinitionId, DateTime fromDate, DateTime toDate);
    long InsertDateRangeBlock(int taskDefinitionId, DateTime fromDate, DateTime toDate, DateTime createdAt);
    long InsertNumericRangeBlock(int taskDefinitionId, long fromNumber, long toNumber, DateTime createdDate);
    long InsertListBlock(int taskDefinitionId, DateTime createdDate, string objectData = null);
    long InsertObjectBlock(int taskDefinitionId, DateTime createdDate, string objectData);

    long InsertBlockExecution(int taskExecutionId, long blockId, DateTime createdAt, DateTime? startedAt,
        DateTime? completedAt, BlockExecutionStatus executionStatus, int attempt = 1);

    void DeleteBlocks(string applicationName);

    int GetBlockExecutionCountByStatus(string applicationName, string taskName,
        BlockExecutionStatus blockExecutionStatus);

    int GetBlockExecutionItemCount(long blockExecutionId);
  
}