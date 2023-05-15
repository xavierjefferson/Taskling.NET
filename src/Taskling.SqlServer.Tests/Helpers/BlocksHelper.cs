using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using Taskling.Blocks.Common;
using Taskling.Blocks.ListBlocks;
using Taskling.InfrastructureContracts;
using Taskling.Serialization;
using Taskling.SqlServer.Models;
using TransactionScopeRetryHelper;

namespace Taskling.SqlServer.Tests.Helpers;

public class BlocksHelper : RepositoryBase, IBlocksHelper
{
    public int GetListBlockItemCountByStatus(long blockId, ItemStatus status)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                return dbContext.ListBlockItems.Count(i => i.BlockId == blockId && i.Status == (int)status);
            }
            //using (var connection = GetConnection())
            //{
            //    var command = connection.CreateCommand();
            //    command.CommandText = GetListBlockItemCountByStatusQuery;
            //    command.Parameters.Add("@BlockId", SqlDbType.BigInt).Value = blockId;
            //    command.Parameters.Add("@Status", SqlDbType.Int).Value = (int)status;
            //    return (int)command.ExecuteScalar();
            //}
        });
    }

    public long GetLastBlockId(TaskId taskId)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                return dbContext.Blocks.Include(i => i.TaskDefinition).Where(i =>
                        i.TaskDefinition.TaskName == taskId.TaskName && i.TaskDefinition.ApplicationName == taskId.ApplicationName)
                    .Max(i => i.BlockId);
            }
            //using (var connection = GetConnection())
            //{
            //    var command = connection.CreateCommand();
            //    command.CommandText = GetLastBlockIdQuery;
            //    command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = taskId.ApplicationName;
            //    command.Parameters.Add("@TaskName", SqlDbType.VarChar, 200).Value = taskId.TaskName;
            //    return (long)command.ExecuteScalar();
            //}
        });
    }

    public List<ListBlockItem<T>> GetListBlockItems<T>(long blockId, ItemStatus status)
    {
        return RetryHelper.WithRetry(() =>
        {
            var items = new List<ListBlockItem<T>>();
            using (var dbContext = GetDbContext())
            {
                var tmp = dbContext.ListBlockItems.Where(i => i.BlockId == blockId && i.Status == (int)status)
                    .Select(i => new { i.ListBlockItemId, i.Value, i.Status, i.StatusReason, i.Step }).ToList();
                foreach (var reader in tmp)
                {
                    var item = new ListBlockItem<T>();
                    item.ListBlockItemId = reader.ListBlockItemId;
                    item.Value = JsonGenericSerializer.Deserialize<T>(reader.Value);
                    item.Status = (ItemStatus)reader.Status;
                    item.StatusReason = reader.StatusReason;
                    item.Step = reader.Step;

                    items.Add(item);
                }
            }
            //using (var connection = GetConnection())
            //{
            //    var command = connection.CreateCommand();
            //    command.CommandText = GetListBlockItemsQuery;
            //    command.Parameters.Add("@BlockId", SqlDbType.BigInt).Value = blockId;
            //    command.Parameters.Add("@Status", SqlDbType.Int).Value = (int)status;

            //    var reader = command.ExecuteReader();
            //    while (reader.Read())
            //    {
            //        var item = new ListBlockItem<T>();
            //        item.ListBlockItemId = reader.GetInt64(0);
            //        item.Value = JsonGenericSerializer.Deserialize<T>(reader.GetString(1));
            //        item.Status = (ItemStatus)reader.GetInt32(2);

            //        if (reader[4] != DBNull.Value)
            //            item.StatusReason = reader.GetString(4);

            //        if (reader[5] != DBNull.Value)
            //            item.Step = reader.GetInt32(5);

            //        items.Add(item);
            //    }
            //}

            return items;
        });
    }

    public void EnqueueForcedBlock(long blockId)
    {
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
            //using (var connection = GetConnection())
            //{
            //    var command = connection.CreateCommand();
            //    command.CommandText = InsertForcedBlockQueueQuery;
            //    command.Parameters.Add("@BlockId", SqlDbType.BigInt).Value = blockId;
            //    command.ExecuteNonQuery();
            //}
        });
    }

    public void InsertPhantomDateRangeBlock(TaskId taskId, DateTime fromDate, DateTime toDate)
    {
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

            //using (var connection = GetConnection())
            //{
            //    var command = connection.CreateCommand();
            //    command.CommandText = InsertPhantomDateBlockQuery;
            //    command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = taskId.ApplicationName;
            //    command.Parameters.Add("@TaskName", SqlDbType.VarChar, 200).Value = taskId.TaskName;
            //    command.Parameters.Add("@FromDate", SqlDbType.DateTime).Value = fromDate;
            //    command.Parameters.Add("@ToDate", SqlDbType.DateTime).Value = toDate;
            //    command.Parameters.Add("@BlockType", SqlDbType.Int).Value = (int)BlockType.DateRange;
            //    command.ExecuteNonQuery();
            //}
        });
    }

    public void InsertPhantomNumericBlock(TaskId taskId, long fromId, long toId)
    {
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
            //using (var connection = GetConnection())
            //{
            //    var command = connection.CreateCommand();
            //    command.CommandText = InsertPhantomNumericBlockQuery;
            //    command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = taskId.ApplicationName;
            //    command.Parameters.Add("@TaskName", SqlDbType.VarChar, 200).Value = taskId.TaskName;
            //    command.Parameters.Add("@FromNumber", SqlDbType.BigInt).Value = fromId;
            //    command.Parameters.Add("@ToNumber", SqlDbType.BigInt).Value = toId;
            //    command.Parameters.Add("@BlockType", SqlDbType.Int).Value = (int)BlockType.NumericRange;
            //    command.ExecuteNonQuery();
            //}
        });
    }

    public void InsertPhantomListBlock(TaskId taskId)
    {
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
            //            using (var connection = GetConnection())
            //            {
            //                var command = connection.CreateCommand();
            //                command.CommandText = @"DECLARE @TaskDefinitionId INT = (
            //	SELECT TaskDefinitionId 
            //	FROM [Taskling].[TaskDefinition]
            //	WHERE ApplicationName = @ApplicationName
            //	AND TaskName = @TaskName)

            //INSERT INTO [Taskling].[Block]
            //           ([TaskDefinitionId]
            //           ,[BlockType]
            //           ,[IsPhantom]
            //           ,[CreatedDate])
            //     VALUES
            //           (@TaskDefinitionId
            //           ,@BlockType
            //           ,1
            //           ,GETUTCDATE())

            //DECLARE @BlockId BIGINT = (SELECT CAST(SCOPE_IDENTITY() AS BIGINT))

            //INSERT INTO [Taskling].[ListBlockItem]
            //           ([BlockId]
            //           ,[Value]
            //           ,[Status])
            //     VALUES
            //           (@BlockId
            //           ,'test'
            //           ,1)";
            //                command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = taskId.ApplicationName;
            //                command.Parameters.Add("@TaskName", SqlDbType.VarChar, 200).Value = taskId.TaskName;
            //                command.Parameters.Add("@BlockType", SqlDbType.Int).Value = (int)BlockType.List;
            //                command.ExecuteNonQuery();
            //            }
        });
    }

    public void InsertPhantomObjectBlock(TaskId taskId)
    {
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
            //using (var connection = GetConnection())
            //{
            //    var command = connection.CreateCommand();
            //    command.CommandText = InsertPhantomObjectBlockQuery;
            //    command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = taskId.ApplicationName;
            //    command.Parameters.Add("@TaskName", SqlDbType.VarChar, 200).Value = taskId.TaskName;
            //    command.Parameters.Add("@BlockType", SqlDbType.Int).Value = (int)BlockType.Object;
            //    command.Parameters.Add("@ObjectData", SqlDbType.NVarChar, -1).Value =
            //        JsonGenericSerializer.Serialize("My phantom block");
            //    command.ExecuteNonQuery();
            //}
        });
    }

    #region .: Get Block Counts :.

    public int GetBlockCount(TaskId taskId)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                return dbContext.Blocks.Include(i =>
                        i.TaskDefinition)
                    .Count(i => i.TaskDefinition.ApplicationName == taskId.ApplicationName &&
                                i.TaskDefinition.TaskName == taskId.TaskName);
            }
            //using (var connection = GetConnection())
            //{
            //    var command = connection.CreateCommand();
            //    command.CommandText = query;
            //    command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = taskId.ApplicationName;
            //    command.Parameters.Add("@TaskName", SqlDbType.VarChar, 200).Value = taskId.TaskName;
            //    return (int)command.ExecuteScalar();
            //}
        });
    }

    #endregion .: Get Block Counts :.

    private void OnTaskDefinitionFound(TasklingDbContext dbContext, TaskId taskId,
        TaskDefinitionDelegate action)
    {
        var taskDefinitionId = dbContext.TaskDefinitions
            .Where(i => i.TaskName == taskId.TaskName && i.ApplicationName == taskId.ApplicationName)
            .Select(i => i.TaskDefinitionId).FirstOrDefault();
        if (taskDefinitionId != default) action(taskDefinitionId, dbContext);
    }

    private delegate void TaskDefinitionDelegate(int taskDefinitionId, TasklingDbContext dbContext);

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

    public long InsertDateRangeBlock(int taskDefinitionId, DateTime fromDate, DateTime toDate)
    {
        return InsertDateRangeBlock(taskDefinitionId, fromDate, toDate, fromDate);
    }

    public long InsertDateRangeBlock(int taskDefinitionId, DateTime fromDate, DateTime toDate, DateTime createdAt)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                return AddDateRange(dbContext, taskDefinitionId, fromDate, toDate, createdAt, false);
            }
            //using (var connection = GetConnection())
            //{
            //    var command = connection.CreateCommand();
            //    command.CommandText = InsertDateRangeBlockQuery;
            //    command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
            //    command.Parameters.Add("@FromDate", SqlDbType.DateTime).Value = fromDate;
            //    command.Parameters.Add("@ToDate", SqlDbType.DateTime).Value = toDate;
            //    command.Parameters.Add("@CreatedDate", SqlDbType.DateTime).Value = createdAt;
            //    command.Parameters.Add("@BlockType", SqlDbType.Int).Value = (int)BlockType.DateRange;
            //    return (long)command.ExecuteScalar();
            //}
        });
    }

    private static long AddDateRange(TasklingDbContext dbContext, int taskDefinitionId, DateTime fromDate,
        DateTime toDate, DateTime createdAt,
        bool isPhantom)
    {
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

    public long InsertNumericRangeBlock(int taskDefinitionId, long fromNumber, long toNumber, DateTime createdDate)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                return AddNumericBlock(dbContext, taskDefinitionId, fromNumber, toNumber, createdDate, false);
            }
            //using (var connection = GetConnection())
            //{


            //    var command = connection.CreateCommand();
            //    command.CommandText = InsertNumericRangeBlockQuery;
            //    command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
            //    command.Parameters.Add("@FromNumber", SqlDbType.BigInt).Value = fromNumber;
            //    command.Parameters.Add("@ToNumber", SqlDbType.BigInt).Value = toNumber;
            //    command.Parameters.Add("@CreatedDate", SqlDbType.DateTime).Value = createdDate;
            //    command.Parameters.Add("@BlockType", SqlDbType.Int).Value = (int)BlockType.NumericRange;
            //    return (long)command.ExecuteScalar();
            //}
        });
    }

    private static long AddNumericBlock(TasklingDbContext dbContext, int taskDefinitionId, long fromNumber,
        long toNumber, DateTime createdDate,
        bool isPhantom)
    {
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

    public long InsertListBlock(int taskDefinitionId, DateTime createdDate, string objectData = null)
    {
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
            //using (var connection = GetConnection())
            //{
            //    var command = connection.CreateCommand();
            //    command.CommandText = InsertListBlockQuery;
            //    command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
            //    command.Parameters.Add("@CreatedDate", SqlDbType.DateTime).Value = createdDate;
            //    command.Parameters.Add("@BlockType", SqlDbType.Int).Value = (int)BlockType.List;
            //    if (objectData == null)
            //        command.Parameters.Add("@ObjectData", SqlDbType.NVarChar, 1000).Value = DBNull.Value;
            //    else
            //        command.Parameters.Add("@ObjectData", SqlDbType.NVarChar, 1000).Value = objectData;

            //    return (long)command.ExecuteScalar();
            //}
        });
    }

    public long InsertObjectBlock(int taskDefinitionId, DateTime createdDate, string objectData)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                return AddObjectBlock(dbContext, taskDefinitionId, createdDate, objectData, false);
            }
            //using (var connection = GetConnection())
            //{
            //    var command = connection.CreateCommand();
            //    command.CommandText = InsertObjectBlockQuery;
            //    command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
            //    command.Parameters.Add("@CreatedDate", SqlDbType.DateTime).Value = createdDate;
            //    command.Parameters.Add("@ObjectData", SqlDbType.NVarChar, -1).Value =
            //        JsonGenericSerializer.Serialize(objectData);
            //    command.Parameters.Add("@BlockType", SqlDbType.Int).Value = (int)BlockType.Object;
            //    return (long)command.ExecuteScalar();
            //}
        });
    }

    private static long AddObjectBlock(TasklingDbContext dbContext, int taskDefinitionId, DateTime createdDate,
        string objectData,
        bool isPhantom)
    {
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

    public long InsertBlockExecution(int taskExecutionId, long blockId, DateTime createdAt, DateTime? startedAt,
        DateTime? completedAt, BlockExecutionStatus executionStatus, int attempt = 1)
    {
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
            //using (var connection = GetConnection())
            //{
            //    var command = connection.CreateCommand();
            //    command.CommandText = InsertBlockExecutionQuery;
            //    command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = taskExecutionId;
            //    command.Parameters.Add("@BlockId", SqlDbType.BigInt).Value = blockId;
            //    command.Parameters.Add("@CreatedAt", SqlDbType.DateTime).Value = createdAt;
            //    command.Parameters.Add("@Attempt", SqlDbType.BigInt).Value = attempt;

            //    if (startedAt.HasValue)
            //        command.Parameters.Add("@StartedAt", SqlDbType.DateTime).Value = startedAt.Value;
            //    else
            //        command.Parameters.Add("@StartedAt", SqlDbType.DateTime).Value = DBNull.Value;

            //    if (completedAt.HasValue)
            //        command.Parameters.Add("@CompletedAt", SqlDbType.DateTime).Value = completedAt.Value;
            //    else
            //        command.Parameters.Add("@CompletedAt", SqlDbType.DateTime).Value = DBNull.Value;

            //    command.Parameters.Add("@BlockExecutionStatus", SqlDbType.Int).Value = (int)executionStatus;
            //    return (long)command.ExecuteScalar();
            //}
        });
    }

    public void DeleteBlocks(string applicationName)
    {
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
            //            using (var connection = GetConnection())
            //            {
            //                var command = connection.CreateCommand();
            //                command.CommandText = @"
            //DELETE BE FROM [Taskling].[BlockExecution] BE
            //inner JOIN [Taskling].[TaskExecution] TE ON BE.TaskExecutionId = TE.TaskExecutionId
            //inner JOIN [Taskling].[TaskDefinition] T ON TE.TaskDefinitionId = T.TaskDefinitionId
            //WHERE (T.ApplicationName = @ApplicationName);

            //DELETE Q FROM [Taskling].[ListBlockItem] Q inner join [Taskling].[Block] B on q.BLockid = b.blockid
            //inner JOIN [Taskling].[TaskDefinition] T ON B.TaskDefinitionId = T.TaskDefinitionId
            //WHERE (T.ApplicationName = @ApplicationName);

            //DELETE Q FROM [Taskling].[ForceBlockQueue] Q inner join [Taskling].[Block] B on q.BLockid = b.blockid
            //inner JOIN [Taskling].[TaskDefinition] T ON B.TaskDefinitionId = T.TaskDefinitionId
            //WHERE (T.ApplicationName = @ApplicationName);

            //DELETE B FROM [Taskling].[Block] B
            //inner JOIN [Taskling].[TaskDefinition] T ON B.TaskDefinitionId = T.TaskDefinitionId
            //WHERE (T.ApplicationName = @ApplicationName);

            //DELETE LBI FROM [Taskling].[ListBlockItem] LBI
            //inner JOIN [Taskling].[Block] B ON LBI.BlockId = B.BlockId 
            //inner JOIN [Taskling].[TaskDefinition] T ON B.TaskDefinitionId = T.TaskDefinitionId
            //WHERE (T.ApplicationName = @ApplicationName);
            //";
            //                command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = taskId.ApplicationName;
            //                command.ExecuteNonQuery();
            //            }
            //        }
        );
    }

    #endregion .: Insert and Delete Blocks :.

    #region .: Get Block Execution Counts :.

    public int GetBlockExecutionCountByStatus(TaskId taskId,
        BlockExecutionStatus blockExecutionStatus)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                return dbContext.BlockExecutions.Include(i => i.TaskExecution).ThenInclude(i => i.TaskDefinition)
                    .Count(i => i.TaskExecution.TaskDefinition.ApplicationName == taskId.ApplicationName &&
                                i.TaskExecution.TaskDefinition.TaskName == taskId.TaskName &&
                                i.BlockExecutionStatus == (int)blockExecutionStatus);
            }
            //using (var connection = GetConnection())
            //{
            //    var command = connection.CreateCommand();
            //    command.CommandText = GetBlockExecutionsCountByStatusQuery;
            //    command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = taskId.ApplicationName;
            //    command.Parameters.Add("@TaskName", SqlDbType.VarChar, 200).Value = taskId.TaskName;
            //    command.Parameters.Add("@BlockExecutionStatus", SqlDbType.Int).Value = (int)blockExecutionStatus;
            //    return (int)command.ExecuteScalar();
            //}
        });
    }

    public int GetBlockExecutionItemCount(long blockExecutionId)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = GetDbContext())
            {
                return dbContext.BlockExecutions.Where(i => i.BlockExecutionId == blockExecutionId)
                    .Select(i => i.ItemsCount ?? 0).FirstOrDefault();
            }
            //using (var connection = GetConnection())
            //{
            //    var command = connection.CreateCommand();
            //    command.CommandText = GetItemsCountQuery;
            //    command.Parameters.Add("@BlockExecutionId", SqlDbType.BigInt).Value = blockExecutionId;
            //    var result = command.ExecuteScalar();
            //    return (int)result;
            //}
        });
    }

    #endregion .: Get Block Execution Counts :.
}