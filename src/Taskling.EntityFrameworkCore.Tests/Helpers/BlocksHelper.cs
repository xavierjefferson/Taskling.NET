using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Taskling.Blocks.ListBlocks;
using Taskling.EntityFrameworkCore.AncilliaryServices;
using Taskling.EntityFrameworkCore.Models;
using Taskling.Enums;
using Taskling.InfrastructureContracts;
using Taskling.Serialization;

namespace Taskling.EntityFrameworkCore.Tests.Helpers;

public class BlocksHelper : RepositoryBase, IBlocksHelper
{
    private readonly ILogger<BlocksHelper> _logger;
    private readonly IDbContextFactoryEx _dbContextFactory;

    public BlocksHelper(ILogger<BlocksHelper> logger, IDbContextFactoryEx dbContextFactory)
    {
        _logger = logger;
        _dbContextFactory = dbContextFactory;
    }

    public int GetListBlockItemCountByStatus(long blockId, ItemStatusEnum status, TaskId taskId)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = _dbContextFactory.GetDbContext(taskId))
            {
                return dbContext.ListBlockItems.Count(i => i.BlockId == blockId && i.Status == (int)status);
            }
        });
    }

    public long GetLastBlockId(TaskId taskId)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = _dbContextFactory.GetDbContext(taskId))
            {
                return dbContext.Blocks.Include(i => i.TaskDefinition).Where(i =>
                        i.TaskDefinition.TaskName == taskId.TaskName &&
                        i.TaskDefinition.ApplicationName == taskId.ApplicationName)
                    .Max(i => i.BlockId);
            }
        });
    }

    public List<ListBlockItem<T>> GetListBlockItems<T>(long blockId, ItemStatusEnum status,
        ILoggerFactory loggerFactory, TaskId taskId)
    {
        return RetryHelper.WithRetry(() =>
        {
            var items = new List<ListBlockItem<T>>();
            using (var dbContext = _dbContextFactory.GetDbContext(taskId))
            {
                var tmp = dbContext.ListBlockItems.Where(i => i.BlockId == blockId && i.Status == (int)status)
                    .Select(i => new { i.ListBlockItemId, i.Value, i.Status, i.StatusReason, i.Step }).ToList();
                foreach (var reader in tmp)
                {
                    var item = new ListBlockItem<T>(loggerFactory.CreateLogger<ListBlockItem<T>>());
                    item.ListBlockItemId = reader.ListBlockItemId;
                    item.Value = JsonGenericSerializer.Deserialize<T>(reader.Value);
                    item.Status = (ItemStatusEnum)reader.Status;
                    item.StatusReason = reader.StatusReason;
                    item.Step = reader.Step;

                    items.Add(item);
                }
            }

            return items;
        });
    }

    public void EnqueueForcedBlock(long blockId, TaskId taskId)
    {
        RetryHelper.WithRetry(() =>
        {
            using (var dbContext = _dbContextFactory.GetDbContext(taskId))
            {
                var forcedBlockQueue = new ForcedBlockQueue
                {
                    BlockId = blockId,
                    ForcedBy = "Test",
                    ForcedDate = DateTime.UtcNow,
                    ProcessingStatus = "Pending"
                };
                dbContext.ForcedBlockQueues.Add(forcedBlockQueue);
                dbContext.SaveChanges();
            }
        });
    }

    public void InsertPhantomDateRangeBlock(TaskId taskId, DateTime fromDate, DateTime toDate)
    {
        RetryHelper.WithRetry(() =>
        {
            using (var dbContext = _dbContextFactory.GetDbContext(taskId))
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
        RetryHelper.WithRetry(() =>
        {
            using (var dbContext = _dbContextFactory.GetDbContext(taskId))
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
        RetryHelper.WithRetry(() =>
        {
            using (var dbContext = _dbContextFactory.GetDbContext(taskId))
            {
                OnTaskDefinitionFound(dbContext, taskId,
                    (taskDefinitionId, context) =>
                    {
                        var block = new Block
                        {
                            TaskDefinitionId = taskDefinitionId,
                            BlockType = (int)BlockTypeEnum.List,
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
        RetryHelper.WithRetry(() =>
        {
            using (var dbContext = _dbContextFactory.GetDbContext(taskId))
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

    public int GetBlockCount(TaskId taskId)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = _dbContextFactory.GetDbContext(taskId))
            {
                return dbContext.Blocks.Include(i =>
                        i.TaskDefinition)
                    .Count(i => i.TaskDefinition.ApplicationName == taskId.ApplicationName &&
                                i.TaskDefinition.TaskName == taskId.TaskName);
            }
        });
    }

    public long InsertDateRangeBlock(long taskDefinitionId, DateTime fromDate, DateTime toDate, TaskId taskId)
    {
        return InsertDateRangeBlock(taskDefinitionId, fromDate, toDate, fromDate, taskId);
    }

    public long InsertDateRangeBlock(long taskDefinitionId, DateTime fromDate, DateTime toDate, DateTime createdAt,
        TaskId taskId)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = _dbContextFactory.GetDbContext(taskId))
            {
                return AddDateRange(dbContext, taskDefinitionId, fromDate, toDate, createdAt, false);
            }
        });
    }

    public long InsertNumericRangeBlock(long taskDefinitionId, long fromNumber, long toNumber, DateTime createdDate,
        TaskId taskId)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = _dbContextFactory.GetDbContext(taskId))
            {
                return AddNumericBlock(dbContext, taskDefinitionId, fromNumber, toNumber, createdDate, false);
            }
        });
    }

    public long InsertListBlock(long taskDefinitionId, DateTime createdDate, TaskId taskId, string objectData = null)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = _dbContextFactory.GetDbContext(taskId))
            {
                var isPhantom = false;
                var block = new Block
                {
                    TaskDefinitionId = taskDefinitionId,
                    CreatedDate = createdDate,
                    ObjectData = objectData,
                    BlockType = (int)BlockTypeEnum.List,
                    IsPhantom = isPhantom
                };
                dbContext.Blocks.Add(block);
                dbContext.SaveChanges();
                return block.BlockId;
            }
        });
    }

    public long InsertObjectBlock(long taskDefinitionId, DateTime createdDate, string objectData, TaskId taskId)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = _dbContextFactory.GetDbContext(taskId))
            {
                return AddObjectBlock(dbContext, taskDefinitionId, createdDate, objectData, false);
            }
        });
    }

    public long InsertBlockExecution(long taskExecutionId, long blockId, DateTime createdAt, DateTime? startedAt,
        DateTime? completedAt, BlockExecutionStatusEnum executionStatus, TaskId taskId, int attempt = 1)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = _dbContextFactory.GetDbContext(taskId))
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

    public void DeleteBlocks(TaskId taskId)
    {
        var applicationName = taskId.ApplicationName;
        RetryHelper.WithRetry(() =>
            {
                using (var dbContext = _dbContextFactory.GetDbContext(taskId))
                {
                    dbContext.BlockExecutions.RemoveRange(dbContext.BlockExecutions.Include(i => i.Block)
                        .ThenInclude(i => i.TaskDefinition).Include(i => i.TaskExecution)
                        .ThenInclude(i => i.TaskDefinition)
                        .Where(i => i.TaskExecution.TaskDefinition.ApplicationName == applicationName ||
                                    i.Block.TaskDefinition.ApplicationName == applicationName));
                    dbContext.ListBlockItems.RemoveRange(dbContext.ListBlockItems.Include(i => i.Block)
                        .ThenInclude(i => i.TaskDefinition)
                        .Where(i => i.Block.TaskDefinition.ApplicationName == applicationName));
                    dbContext.ForcedBlockQueues.RemoveRange(dbContext.ForcedBlockQueues.Include(i => i.Block)
                        .ThenInclude(i => i.TaskDefinition)
                        .Where(i => i.Block.TaskDefinition.ApplicationName == applicationName));
                    dbContext.Blocks.RemoveRange(dbContext.Blocks.Include(i => i.TaskDefinition)
                        .Where(i => i.TaskDefinition.ApplicationName == applicationName));
                    dbContext.SaveChanges();
                }
            }
        );
    }

    public int GetBlockExecutionCountByStatus(TaskId taskId,
        BlockExecutionStatusEnum blockExecutionStatus)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = _dbContextFactory.GetDbContext(taskId))
            {
                return dbContext.BlockExecutions.Include(i => i.TaskExecution).ThenInclude(i => i.TaskDefinition)
                    .Count(i => i.TaskExecution.TaskDefinition.ApplicationName == taskId.ApplicationName &&
                                i.TaskExecution.TaskDefinition.TaskName == taskId.TaskName &&
                                i.BlockExecutionStatus == (int)blockExecutionStatus);
            }
        });
    }

    public int GetBlockExecutionItemCount(long blockExecutionId, TaskId taskId)
    {
        return RetryHelper.WithRetry(() =>
        {
            using (var dbContext = _dbContextFactory.GetDbContext(taskId))
            {
                return dbContext.BlockExecutions.Where(i => i.BlockExecutionId == blockExecutionId)
                    .Select(i => i.ItemsCount ?? 0).FirstOrDefault();
            }
        });
    }

    private void OnTaskDefinitionFound(TasklingDbContext dbContext, TaskId taskId,
        TaskDefinitionDelegate action)
    {
        var taskDefinitionId = dbContext.TaskDefinitions
            .Where(i => i.TaskName == taskId.TaskName && i.ApplicationName == taskId.ApplicationName)
            .Select(i => i.TaskDefinitionId).FirstOrDefault();
        if (taskDefinitionId != default) action(taskDefinitionId, dbContext);
    }

    private long AddDateRange(TasklingDbContext dbContext, long taskDefinitionId, DateTime fromDate,
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
            BlockType = (int)BlockTypeEnum.DateRange
        };
        dbContext.Blocks.Add(objectBlock);
        dbContext.SaveChanges();
        return objectBlock.BlockId;
    }

    private long AddNumericBlock(TasklingDbContext dbContext, long taskDefinitionId, long fromNumber,
        long toNumber, DateTime createdDate,
        bool isPhantom)
    {
        var block = new Block
        {
            TaskDefinitionId = taskDefinitionId,
            CreatedDate = createdDate,
            ToNumber = toNumber,
            FromNumber = fromNumber,
            IsPhantom = isPhantom,
            BlockType = (int)BlockTypeEnum.NumericRange
        };
        dbContext.Blocks.Add(block);
        dbContext.SaveChanges();
        return block.BlockId;
    }

    private long AddObjectBlock(TasklingDbContext dbContext, long taskDefinitionId, DateTime createdDate,
        string objectData,
        bool isPhantom)
    {
        var block = new Block
        {
            IsPhantom = isPhantom,
            TaskDefinitionId = taskDefinitionId,
            CreatedDate = createdDate,
            ObjectData = JsonGenericSerializer.Serialize(objectData),
            BlockType = (int)BlockTypeEnum.Object
        };
        dbContext.Blocks.Add(block);
        dbContext.SaveChanges();
        return block.BlockId;
    }

    private delegate void TaskDefinitionDelegate(long taskDefinitionId, TasklingDbContext dbContext);
}