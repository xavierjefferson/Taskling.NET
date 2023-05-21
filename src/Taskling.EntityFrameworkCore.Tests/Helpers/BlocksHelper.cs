using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Taskling.Blocks.Common;
using Taskling.Blocks.ListBlocks;
using Taskling.InfrastructureContracts;
using Taskling.Serialization;
using Taskling.SqlServer.Models;
using TransactionScopeRetryHelper;

namespace Taskling.SqlServer.Tests.Helpers;

public class BlocksHelper : RepositoryBase, IBlocksHelper
{
    private readonly ILogger<BlocksHelper> _logger;

    public BlocksHelper(ILogger<BlocksHelper> logger)
    {
        _logger = logger;
    }

    public int GetListBlockItemCountByStatus(long blockId, ItemStatus status)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                return dbContext.ListBlockItems.Count(i => i.BlockId == blockId && i.Status == (int)status);
            }
        });
    }

    public long GetLastBlockId(TaskId taskId)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                return dbContext.Blocks.Include(i => i.TaskDefinition).Where(i =>
                        i.TaskDefinition.TaskName == taskId.TaskName &&
                        i.TaskDefinition.ApplicationName == taskId.ApplicationName)
                    .Max(i => i.BlockId);
            }
        });
    }

    public List<ListBlockItem<T>> GetListBlockItems<T>(long blockId, ItemStatus status, ILoggerFactory loggerFactory)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return RetryHelper.WithRetry(() =>
        {
            var items = new List<ListBlockItem<T>>();
            using (var dbContext = GetDbContext())
            {
                var tmp = dbContext.ListBlockItems.Where(i => i.BlockId == blockId && i.Status == (int)status)
                    .Select(i => new { i.ListBlockItemId, i.Value, i.Status, i.StatusReason, i.Step }).ToList();
                foreach (var reader in tmp)
                {
                    var item = new ListBlockItem<T>(loggerFactory.CreateLogger<ListBlockItem<T>>());
                    item.ListBlockItemId = reader.ListBlockItemId;
                    item.Value = JsonGenericSerializer.Deserialize<T>(reader.Value);
                    item.Status = (ItemStatus)reader.Status;
                    item.StatusReason = reader.StatusReason;
                    item.Step = reader.Step;

                    items.Add(item);
                }
            }

            return items;
        });
    }

    public void EnqueueForcedBlock(long blockId)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                var forceBlockQueue = new ForceBlockQueue
                {
                    BlockId = blockId,
                    ForcedBy = "Test",
                    ForcedDate = DateTime.UtcNow,
                    ProcessingStatus = "Pending"
                };
                dbContext.ForceBlockQueues.Add(forceBlockQueue);
                dbContext.SaveChanges();
            }
        });
    }

    public void InsertPhantomDateRangeBlock(TaskId taskId, DateTime fromDate, DateTime toDate)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                OnTaskDefinitionFound(dbContext, taskId,
                    (taskDefinitionId, context) =>
                    {
                        AddDateRange(context, taskDefinitionId, fromDate, toDate, DateTime.UtcNow, true);
                    });
            }
        });
    }

    public void InsertPhantomNumericBlock(TaskId taskId, long fromId, long toId)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                OnTaskDefinitionFound(dbContext, taskId,
                    (taskDefinitionId, context) =>
                    {
                        AddNumericBlock(context, taskDefinitionId, fromId, toId, DateTime.UtcNow, true);
                    });
            }
        });
    }

    public void InsertPhantomListBlock(TaskId taskId)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                OnTaskDefinitionFound(dbContext, taskId,
                    (taskDefinitionId, context) =>
                    {
                        var block = new Block
                        {
                            TaskDefinitionId = taskDefinitionId,
                            BlockType = (int)BlockType.List,
                            IsPhantom = true,
                            CreatedDate = DateTime.UtcNow
                        };
                        context.Blocks.Add(block);
                        //have to save changes to get a block id
                        context.SaveChanges();
                        if (block.BlockId <= 0)
                            throw new InvalidOperationException($"{nameof(block.BlockId)} is invalid");
                        var listBlockItem = new ListBlockItem
                        {
                            BlockId = block.BlockId,
                            Value = "test",
                            Status = 1
                        };
                        context.ListBlockItems.Add(listBlockItem);
                        context.SaveChanges();
                    });
            }
        });
    }

    public void InsertPhantomObjectBlock(TaskId taskId)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                OnTaskDefinitionFound(dbContext, taskId,
                    (taskDefinitionId, context) =>
                    {
                        AddObjectBlock(context, taskDefinitionId, DateTime.UtcNow,
                            JsonGenericSerializer.Serialize("My phantom block"),
                            true);
                    });
            }
        });
    }

    #region .: Get Block Counts :.

    public int GetBlockCount(TaskId taskId)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                return dbContext.Blocks.Include(i =>
                        i.TaskDefinition)
                    .Count(i => i.TaskDefinition.ApplicationName == taskId.ApplicationName &&
                                i.TaskDefinition.TaskName == taskId.TaskName);
            }
        });
    }

    #endregion .: Get Block Counts :.

    private void OnTaskDefinitionFound(TasklingDbContext dbContext, TaskId taskId,
        TaskDefinitionDelegate action)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var taskDefinitionId = dbContext.TaskDefinitions
            .Where(i => i.TaskName == taskId.TaskName && i.ApplicationName == taskId.ApplicationName)
            .Select(i => i.TaskDefinitionId).FirstOrDefault();
        if (taskDefinitionId != default) action(taskDefinitionId, dbContext);
    }

    private delegate void TaskDefinitionDelegate(long taskDefinitionId, TasklingDbContext dbContext);

    #region .: Queries :.

    private const string InsertDateRangeBlockQuery = @"INSERT INTO [Taskling].[Block]
           ([TaskDefinitionId]
           ,[FromDate]
           ,[ToDate]
           ,[CreatedDate]
           ,[BlockType])
     VALUES
           (@TaskDefinitionId
           ,@FromDate
           ,@ToDate
           ,@CreatedDate
           ,@BlockType);

SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

    private const string InsertNumericRangeBlockQuery = @"INSERT INTO [Taskling].[Block]
           ([TaskDefinitionId]
           ,[FromNumber]
           ,[ToNumber]
           ,[CreatedDate]
           ,[BlockType])
     VALUES
           (@TaskDefinitionId
           ,@FromNumber
           ,@ToNumber
           ,@CreatedDate
           ,@BlockType);

SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

    private const string InsertListBlockQuery = @"INSERT INTO [Taskling].[Block]
           ([TaskDefinitionId]
           ,[CreatedDate]
           ,[BlockType]
           ,[ObjectData])
     VALUES
           (@TaskDefinitionId
           ,@CreatedDate
           ,@BlockType
           ,@ObjectData);

SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

    private const string InsertObjectBlockQuery = @"INSERT INTO [Taskling].[Block]
           ([TaskDefinitionId]
           ,[CreatedDate]
           ,[BlockType]
           ,[ObjectData])
     VALUES
           (@TaskDefinitionId
           ,@CreatedDate
           ,@BlockType
           ,@ObjectData);

SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

    private const string InsertBlockExecutionQuery = @"INSERT INTO [Taskling].[BlockExecution]
           ([TaskExecutionId]
           ,[BlockId]
           ,[CreatedAt]
           ,[StartedAt]
           ,[CompletedAt]
           ,[BlockExecutionStatus]
           ,[Attempt])
     VALUES
           (@TaskExecutionId
           ,@BlockId
           ,@CreatedAt
           ,@StartedAt
           ,@CompletedAt
           ,@BlockExecutionStatus
           ,@Attempt);

SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

    private const string GetBlockCountQuery = @"SELECT COUNT(*)
FROM [Taskling].[Block] B
JOIN [Taskling].[TaskDefinition]  T ON B.TaskDefinitionId = T.TaskDefinitionId
WHERE T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName;";

    private const string GetBlockExecutionsCountByStatusQuery = @"SELECT COUNT(*)
FROM [Taskling].[BlockExecution] BE
JOIN [Taskling].[TaskExecution] TE ON BE.TaskExecutionId = TE.TaskExecutionId
JOIN [Taskling].[TaskDefinition]  T ON TE.TaskDefinitionId = T.TaskDefinitionId
WHERE T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName
AND BE.BlockExecutionStatus = @BlockExecutionStatus;";

    private const string GetListBlockItemCountByStatusQuery = @"SELECT COUNT(*)
FROM [Taskling].[ListBlockItem] LBI
WHERE LBI.BlockId = @BlockId
AND LBI.Status = @Status;";

    private const string GetItemsCountQuery = @"SELECT [ItemsCount]
FROM [Taskling].[BlockExecution]
WHERE [BlockExecutionId] = @BlockExecutionId";

    private const string GetLastBlockIdQuery = @"SELECT MAX(BlockId)
FROM [Taskling].[Block] B
JOIN [Taskling].[TaskDefinition] TD ON B.TaskDefinitionId = TD.TaskDefinitionId
WHERE ApplicationName = @ApplicationName
AND TaskName = @TaskName";

    private const string GetListBlockItemsQuery = @"SELECT [ListBlockItemId]
      ,[Value]
      ,[Status]
      ,[LastUpdated]
      ,[StatusReason]
      ,[Step]
FROM [Taskling].[ListBlockItem]
WHERE [BlockId] = @BlockId
AND [Status] = @Status";

    private const string InsertForcedBlockQueueQuery = @"INSERT INTO [Taskling].[ForceBlockQueue]
           ([BlockId]
           ,[ForcedBy]
           ,[ForcedDate]
           ,[ProcessingStatus])
     VALUES
           (@BlockId
           ,'Test'
           ,GETUTCDATE()
           ,'Pending')";

    private const string InsertPhantomNumericBlockQuery = @"DECLARE @TaskDefinitionId INT = (
	SELECT TaskDefinitionId 
	FROM [Taskling].[TaskDefinition]
	WHERE ApplicationName = @ApplicationName
	AND TaskName = @TaskName)

INSERT INTO [Taskling].[Block]
           ([TaskDefinitionId]
           ,[FromNumber]
           ,[ToNumber]
           ,[BlockType]
           ,[IsPhantom]
           ,[CreatedDate])
     VALUES
           (@TaskDefinitionId
           ,@FromNumber
           ,@ToNumber
           ,@BlockType
           ,1
           ,GETUTCDATE())";

    private const string InsertPhantomDateBlockQuery = @"DECLARE @TaskDefinitionId INT = (
	SELECT TaskDefinitionId 
	FROM [Taskling].[TaskDefinition]
	WHERE ApplicationName = @ApplicationName
	AND TaskName = @TaskName)

INSERT INTO [Taskling].[Block]
           ([TaskDefinitionId]
           ,[FromDate]
           ,[ToDate]
           ,[BlockType]
           ,[IsPhantom]
           ,[CreatedDate])
     VALUES
           (@TaskDefinitionId
           ,@FromDate
           ,@ToDate
           ,@BlockType
           ,1
           ,GETUTCDATE())";

    private const string InsertPhantomObjectBlockQuery = @"DECLARE @TaskDefinitionId INT = (
	SELECT TaskDefinitionId 
	FROM [Taskling].[TaskDefinition]
	WHERE ApplicationName = @ApplicationName
	AND TaskName = @TaskName)

INSERT INTO [Taskling].[Block]
           ([TaskDefinitionId]
           ,[ObjectData]
           ,[BlockType]
           ,[IsPhantom]
           ,[CreatedDate])
     VALUES
           (@TaskDefinitionId
           ,@ObjectData
           ,@BlockType
           ,1
           ,GETUTCDATE())";

    #endregion .: Queries :.

    #region .: Insert and Delete Blocks :.

    public long InsertDateRangeBlock(long taskDefinitionId, DateTime fromDate, DateTime toDate)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return InsertDateRangeBlock(taskDefinitionId, fromDate, toDate, fromDate);
    }

    public long InsertDateRangeBlock(long taskDefinitionId, DateTime fromDate, DateTime toDate, DateTime createdAt)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                return AddDateRange(dbContext, taskDefinitionId, fromDate, toDate, createdAt, false);
            }
        });
    }

    private long AddDateRange(TasklingDbContext dbContext, long taskDefinitionId, DateTime fromDate,
        DateTime toDate, DateTime createdAt,
        bool isPhantom)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var objectBlock = new Block
        {
            TaskDefinitionId = taskDefinitionId,
            CreatedDate = createdAt,
            ToDate = toDate,
            IsPhantom = isPhantom,
            FromDate = fromDate, //ObjectData = JsonGenericSerializer.Serialize(objectData),
            BlockType = (int)BlockType.DateRange
        };
        dbContext.Blocks.Add(objectBlock);
        dbContext.SaveChanges();
        return objectBlock.BlockId;
    }

    public long InsertNumericRangeBlock(long taskDefinitionId, long fromNumber, long toNumber, DateTime createdDate)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                return AddNumericBlock(dbContext, taskDefinitionId, fromNumber, toNumber, createdDate, false);
            }
        });
    }

    private long AddNumericBlock(TasklingDbContext dbContext, long taskDefinitionId, long fromNumber,
        long toNumber, DateTime createdDate,
        bool isPhantom)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var objectBlock = new Block
        {
            TaskDefinitionId = taskDefinitionId,
            CreatedDate = createdDate,
            ToNumber = toNumber,
            FromNumber = fromNumber,
            IsPhantom = isPhantom,
            //ObjectData = JsonGenericSerializer.Serialize(objectData),
            BlockType = (int)BlockType.NumericRange
        };
        dbContext.Blocks.Add(objectBlock);
        dbContext.SaveChanges();
        return objectBlock.BlockId;
    }

    public long InsertListBlock(long taskDefinitionId, DateTime createdDate, string objectData = null)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                var isPhantom = false;
                var objectBlock = new Block
                {
                    TaskDefinitionId = taskDefinitionId,
                    CreatedDate = createdDate,
                    ObjectData = objectData,
                    BlockType = (int)BlockType.List,
                    IsPhantom = isPhantom
                };
                dbContext.Blocks.Add(objectBlock);
                dbContext.SaveChanges();
                return objectBlock.BlockId;
            }
        });
    }

    public long InsertObjectBlock(long taskDefinitionId, DateTime createdDate, string objectData)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                return AddObjectBlock(dbContext, taskDefinitionId, createdDate, objectData, false);
            }
        });
    }

    private long AddObjectBlock(TasklingDbContext dbContext, long taskDefinitionId, DateTime createdDate,
        string objectData,
        bool isPhantom)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var objectBlock = new Block
        {
            IsPhantom = isPhantom,
            TaskDefinitionId = taskDefinitionId,
            CreatedDate = createdDate,
            ObjectData = JsonGenericSerializer.Serialize(objectData),
            BlockType = (int)BlockType.Object
        };
        dbContext.Blocks.Add(objectBlock);
        dbContext.SaveChanges();
        return objectBlock.BlockId;
    }

    public long InsertBlockExecution(long taskExecutionId, long blockId, DateTime createdAt, DateTime? startedAt,
        DateTime? completedAt, BlockExecutionStatus executionStatus, int attempt = 1)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                var blockExecution = new BlockExecution
                {
                    TaskExecutionId = taskExecutionId,
                    BlockId = blockId,
                    CreatedAt = createdAt,
                    StartedAt = startedAt,
                    CompletedAt = completedAt,
                    BlockExecutionStatus = (int)executionStatus,
                    Attempt = attempt
                };
                dbContext.BlockExecutions.Add(blockExecution);
                dbContext.SaveChanges();
                return blockExecution.BlockExecutionId;
            }
        });
    }

    public void DeleteBlocks(string applicationName)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        RetryHelper.WithRetry(() =>
            {
                using (var dbContext = GetDbContext())
                {
                    dbContext.BlockExecutions.RemoveRange(dbContext.BlockExecutions.Include(i => i.Block)
                        .ThenInclude(i => i.TaskDefinition).Include(i => i.TaskExecution)
                        .ThenInclude(i => i.TaskDefinition)
                        .Where(i => i.TaskExecution.TaskDefinition.ApplicationName == applicationName ||
                                    i.Block.TaskDefinition.ApplicationName == applicationName));
                    dbContext.ListBlockItems.RemoveRange(dbContext.ListBlockItems.Include(i => i.Block)
                        .ThenInclude(i => i.TaskDefinition)
                        .Where(i => i.Block.TaskDefinition.ApplicationName == applicationName));
                    dbContext.ForceBlockQueues.RemoveRange(dbContext.ForceBlockQueues.Include(i => i.Block)
                        .ThenInclude(i => i.TaskDefinition)
                        .Where(i => i.Block.TaskDefinition.ApplicationName == applicationName));
                    dbContext.Blocks.RemoveRange(dbContext.Blocks.Include(i => i.TaskDefinition)
                        .Where(i => i.TaskDefinition.ApplicationName == applicationName));
                    dbContext.SaveChanges();
                }
            }
        );
    }

    #endregion .: Insert and Delete Blocks :.

    #region .: Get Block Execution Counts :.

    public int GetBlockExecutionCountByStatus(TaskId taskId,
        BlockExecutionStatus blockExecutionStatus)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                return dbContext.BlockExecutions.Include(i => i.TaskExecution).ThenInclude(i => i.TaskDefinition)
                    .Count(i => i.TaskExecution.TaskDefinition.ApplicationName == taskId.ApplicationName &&
                                i.TaskExecution.TaskDefinition.TaskName == taskId.TaskName &&
                                i.BlockExecutionStatus == (int)blockExecutionStatus);
            }
        });
    }

    public int GetBlockExecutionItemCount(long blockExecutionId)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                return dbContext.BlockExecutions.Where(i => i.BlockExecutionId == blockExecutionId)
                    .Select(i => i.ItemsCount ?? 0).FirstOrDefault();
            }
        });
    }

    #endregion .: Get Block Execution Counts :.
}