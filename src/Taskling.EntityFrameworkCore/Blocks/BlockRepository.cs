using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Taskling.Blocks.Common;
using Taskling.Blocks.ListBlocks;
using Taskling.Blocks.ObjectBlocks;
using Taskling.Blocks.RangeBlocks;
using Taskling.EntityFrameworkCore.AncilliaryServices;
using Taskling.EntityFrameworkCore.Blocks.Models;
using Taskling.EntityFrameworkCore.Blocks.QueryBuilders;
using Taskling.EntityFrameworkCore.Blocks.Serialization;
using Taskling.EntityFrameworkCore.Models;
using Taskling.Exceptions;
using Taskling.Extensions;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.Blocks.CommonRequests.ForcedBlocks;
using Taskling.InfrastructureContracts.Blocks.ListBlocks;
using Taskling.InfrastructureContracts.Blocks.ObjectBlocks;
using Taskling.InfrastructureContracts.Blocks.RangeBlocks;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.Serialization;
using Taskling.Tasks;
using TaskDefinition = Taskling.InfrastructureContracts.TaskExecution.TaskDefinition;

namespace Taskling.EntityFrameworkCore.Blocks;

public partial class BlockRepository : DbOperationsService, IBlockRepository
{
    private const string UnexpectedBlockTypeMessage =
        "This block type was not expected. This can occur when changing the block type of an existing process or combining different block types in a single process - which is not supported";

    public static readonly int[] PendingOrFailedStatuses =
    {
        (int)BlockExecutionStatus.NotStarted,
        (int)BlockExecutionStatus.NotDefined, (int)BlockExecutionStatus.Started,
        (int)BlockExecutionStatus.Failed
    };

    private readonly ILogger<BlockRepository> _logger;
    private readonly ILoggerFactory _loggerFactory;


    private readonly ITaskRepository _taskRepository;


    public BlockRepository(ITaskRepository taskRepository, IConnectionStore connectionStore,
        ILogger<BlockRepository> logger, IDbContextFactoryEx dbContextFactoryEx, ILoggerFactory loggerFactory) : base(
        connectionStore,
        dbContextFactoryEx, loggerFactory.CreateLogger<DbOperationsService>())
    {
        _taskRepository = taskRepository;
        _logger = logger;
        _loggerFactory = loggerFactory;
    }


    public async Task<IList<ForcedRangeBlockQueueItem>> GetQueuedForcedRangeBlocksAsync(
        QueuedForcedBlocksRequest queuedForcedBlocksRequest)
    {
        switch (queuedForcedBlocksRequest.BlockType)
        {
            case BlockType.DateRange:
                var tmp = await GetForcedDateRangeBlocksAsync(queuedForcedBlocksRequest).ConfigureAwait(false);
                _logger.LogDebug($"Returning {tmp.Count}");
                return tmp;
            case BlockType.NumericRange:
                var tmp2 = await GetForcedNumericRangeBlocksAsync(queuedForcedBlocksRequest).ConfigureAwait(false);
                _logger.LogDebug($"Returning {tmp2.Count}");
                return tmp2;
            default:
                throw new NotSupportedException("This range type is not supported");
        }
    }

    public async Task<IList<ForcedListBlockQueueItem>> GetQueuedForcedListBlocksAsync(
        QueuedForcedBlocksRequest queuedForcedBlocksRequest)
    {
        return await GetForcedListBlocksAsync(queuedForcedBlocksRequest).ConfigureAwait(false);
    }

    public async Task<IList<ForcedObjectBlockQueueItem<T>>> GetQueuedForcedObjectBlocksAsync<T>(
        QueuedForcedBlocksRequest queuedForcedBlocksRequest)
    {
        return await GetForcedObjectBlocksAsync<T>(queuedForcedBlocksRequest).ConfigureAwait(false);
    }

    public async Task DequeueForcedBlocksAsync(DequeueForcedBlocksRequest dequeueForcedBlocksRequest)
    {
        await UpdateForcedBlocksAsync(dequeueForcedBlocksRequest).ConfigureAwait(false);
    }


    public async Task<IList<RangeBlock>> FindRangeBlocksOfTaskAsync(FindBlocksOfTaskRequest blocksOfTaskRequest)
    {
        return await FindBlocksOfTaskAsync(blocksOfTaskRequest, new[] { BlockType.DateRange, BlockType.NumericRange },
            (i, j) => GetRangeBlocks(j, i));
    }

    public async Task<RangeBlockCreateResponse> AddRangeBlockAsync(RangeBlockCreateRequest rangeBlockCreateRequest)
    {
        _logger.LogDebug("Adding range block");
        var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(rangeBlockCreateRequest.TaskId)
            .ConfigureAwait(false);

        var response = new RangeBlockCreateResponse();

        response.Block = await RetryHelper.WithRetryAsync(async () =>
        {
            using (var dbContext = await GetDbContextAsync(rangeBlockCreateRequest.TaskId))

            {
                Block block;
                switch (rangeBlockCreateRequest.BlockType)
                {
                    case BlockType.DateRange:
                        block = new Block
                        {
                            TaskDefinitionId = taskDefinition.TaskDefinitionId,
                            FromDate = new DateTime(rangeBlockCreateRequest.From),
                            ToDate = new DateTime(rangeBlockCreateRequest.To),
                            BlockType = (int)BlockType.DateRange,
                            CreatedDate = DateTime.UtcNow
                        };

                        break;
                    case BlockType.NumericRange:
                        block = new Block
                        {
                            TaskDefinitionId = taskDefinition.TaskDefinitionId,
                            FromNumber = rangeBlockCreateRequest.From,
                            ToNumber = rangeBlockCreateRequest.To,
                            BlockType = (int)BlockType.NumericRange,
                            CreatedDate = DateTime.UtcNow
                        };

                        break;
                    default:
                        throw new NotSupportedException(UnexpectedBlockTypeMessage);
                }

                await dbContext.Blocks.AddAsync(block).ConfigureAwait(false);
                await dbContext.SaveChangesAsync().ConfigureAwait(false);

                return new RangeBlock(block.BlockId,
                    0,
                    rangeBlockCreateRequest.From,
                    rangeBlockCreateRequest.To,
                    rangeBlockCreateRequest.BlockType, _loggerFactory.CreateLogger<RangeBlock>());
            }
        }).ConfigureAwait(false);

        return response;
    }

    public async Task<long> AddRangeBlockExecutionAsync(BlockExecutionCreateRequest executionCreateRequest)
    {
        return await AddBlockExecutionAsync(executionCreateRequest).ConfigureAwait(false);
    }

    public async Task<IList<ProtoListBlock>> FindListBlocksOfTaskAsync(FindBlocksOfTaskRequest blocksOfTaskRequest)
    {
        return await FindBlocksOfTaskAsync(blocksOfTaskRequest, new[] { BlockType.List },
            (i, j) => GetListBlocks(j, i));
    }

    public async Task<ListBlockCreateResponse> AddListBlockAsync(ListBlockCreateRequest createRequest)
    {
        _logger.LogDebug("Adding list block");
        var taskDefinition =
            await _taskRepository.EnsureTaskDefinitionAsync(createRequest.TaskId).ConfigureAwait(false);

        var response = new ListBlockCreateResponse();
        if (createRequest.BlockType == BlockType.List)
        {
            var blockId = await AddNewListBlockAsync(createRequest.TaskId, taskDefinition.TaskDefinitionId,
                createRequest.SerializedHeader, createRequest.CompressionThreshold).ConfigureAwait(false);
            await AddListBlockItemsAsync(blockId, createRequest).ConfigureAwait(false);

            // we do not populate the items here, they are lazy loaded
            response.Block = new ProtoListBlock
            {
                ListBlockId = blockId,
                Header = createRequest.SerializedHeader
            };

            return response;
        }

        throw new NotSupportedException(UnexpectedBlockTypeMessage);
    }

    public async Task<long> AddListBlockExecutionAsync(BlockExecutionCreateRequest executionCreateRequest)
    {
        return await AddBlockExecutionAsync(executionCreateRequest).ConfigureAwait(false);
    }


    public async Task<IList<ObjectBlock<T>>> FindObjectBlocksOfTaskAsync<T>(FindBlocksOfTaskRequest blocksOfTaskRequest)
    {
        if (blocksOfTaskRequest.BlockType == BlockType.Object)
            return await FindObjectBlocksOfTaskAsync<T>(blocksOfTaskRequest, blocksOfTaskRequest.ReprocessOption)
                .ConfigureAwait(false);

        throw new NotSupportedException(UnexpectedBlockTypeMessage);
    }


    public async Task<long> AddObjectBlockExecutionAsync(BlockExecutionCreateRequest executionCreateRequest)
    {
        return await AddBlockExecutionAsync(executionCreateRequest).ConfigureAwait(false);
    }

    public async Task<ObjectBlockCreateResponse<T>> AddObjectBlockAsync<T>(ObjectBlockCreateRequest<T> createRequest)
    {
        _logger.LogDebug("Adding object block");
        var taskDefinition =
            await _taskRepository.EnsureTaskDefinitionAsync(createRequest.TaskId).ConfigureAwait(false);

        var response = new ObjectBlockCreateResponse<T>();
        if (createRequest.BlockType == BlockType.Object)
        {
            var blockId = await AddNewObjectBlockAsync(createRequest.TaskId, taskDefinition.TaskDefinitionId,
                createRequest.Object, createRequest.CompressionThreshold).ConfigureAwait(false);
            response.Block = new ObjectBlock<T>
            {
                ObjectBlockId = blockId,
                Object = createRequest.Object
            };

            return response;
        }

        throw new NotSupportedException(UnexpectedBlockTypeMessage);
    }

    private async Task<IList<T>> FindBlocksOfTaskAsync<T>(FindBlocksOfTaskRequest blocksOfTaskRequest,
        BlockType[] blockTypes, Func<List<BlockQueryItem>, FindBlocksOfTaskRequest, IList<T>> converter)
    {
        if (blockTypes.Contains(blocksOfTaskRequest.BlockType))
            return await RetryHelper.WithRetryAsync(async () =>
            {
                var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(blocksOfTaskRequest.TaskId)
                    .ConfigureAwait(false);

                using (var dbContext = await GetDbContextAsync(blocksOfTaskRequest.TaskId))
                {
                    var items = await GetBlocksOfTaskQueryItems(dbContext,
                        taskDefinition.TaskDefinitionId,
                        blocksOfTaskRequest.ReferenceValueOfTask, blocksOfTaskRequest.ReprocessOption);
                    var results = converter(items, blocksOfTaskRequest);
                    return results;
                }
            });

        throw new NotSupportedException(UnexpectedBlockTypeMessage);
    }


    private async Task<long> AddNewListBlockAsync(TaskId taskId, long taskDefinitionId, string? header,
        int compressionThreshold)
    {
        header ??= string.Empty;

        var isLargeTextValue = false;
        byte[]? compressedData = null;
        if (header.Length > compressionThreshold)
        {
            isLargeTextValue = true;
            compressedData = LargeValueCompressor.Zip(header);
        }

        return await RetryHelper.WithRetryAsync(async () =>
        {
            using (var dbContext = await GetDbContextAsync(taskId).ConfigureAwait(false))

            {
                var block = new Block
                {
                    TaskDefinitionId = taskDefinitionId,

                    BlockType = (int)BlockType.List,
                    CreatedDate = DateTime.UtcNow
                };
                if (isLargeTextValue)
                {
                    block.ObjectData = null;
                    block.CompressedObjectData = compressedData;
                }
                else
                {
                    block.ObjectData = header;
                    block.CompressedObjectData = null;
                }

                await dbContext.Blocks.AddAsync(block).ConfigureAwait(false);
                await dbContext.SaveChangesAsync().ConfigureAwait(false);

                return block.BlockId;
            }
        });
    }

    private async Task AddListBlockItemsAsync(long blockId, ListBlockCreateRequest createRequest)
    {
        await RetryHelper.WithRetryAsync(async () =>
        {
            using (var dbContext = await GetDbContextAsync(createRequest.TaskId).ConfigureAwait(false))

            {
                foreach (var value in (List<string?>)createRequest.SerializedValues)
                {
                    var item = new ListBlockItem
                    {
                        BlockId = blockId
                    };


                    if (value.Length > createRequest.CompressionThreshold)
                    {
                        item.Value = null;
                        item.CompressedValue = LargeValueCompressor.Zip(value);
                    }
                    else
                    {
                        item.Value = value;
                        item.CompressedValue = null;
                    }

                    item.Status = (int)ItemStatus.Pending;
                    item.LastUpdated = DateTime.UtcNow;
                    await dbContext.ListBlockItems.AddAsync(item);
                }

                await dbContext.SaveChangesAsync().ConfigureAwait(false);
            }
        });
    }


    private async Task<long> AddNewObjectBlockAsync<T>(TaskId taskId, long taskDefinitionId, T objectData,
        int compressionThreshold)
    {
        var isLargeTextValue = false;
        var jsonValue = JsonGenericSerializer.Serialize(objectData);
        byte[]? compressedData = null;
        if (jsonValue.Length > compressionThreshold)
        {
            isLargeTextValue = true;
            compressedData = LargeValueCompressor.Zip(jsonValue);
        }

        return await RetryHelper.WithRetryAsync(async () =>
        {
            using (var dbContext = await GetDbContextAsync(taskId).ConfigureAwait(false))

            {
                var block = new Block
                {
                    TaskDefinitionId = taskDefinitionId,
                    BlockType = (int)BlockType.Object,
                    CreatedDate = DateTime.UtcNow
                };
                block.ObjectData = isLargeTextValue ? null : jsonValue;
                block.CompressedObjectData = isLargeTextValue ? compressedData : null;

                await dbContext.Blocks.AddAsync(block).ConfigureAwait(false);
                await dbContext.SaveChangesAsync().ConfigureAwait(false);

                return block.BlockId;
            }
        });
    }


    private async Task<IList<ObjectBlock<T>>> FindSearchableObjectBlocksAsync<T>(
        ISearchableBlockRequest deadBlocksRequest,
        BlockItemDelegateRunner blockItemDelegateRunner)
    {
        return await RetryHelper.WithRetryAsync(async () =>
        {
            var results = new List<ObjectBlock<T>>();
            var taskDefinition =
                await _taskRepository.EnsureTaskDefinitionAsync(deadBlocksRequest.TaskId).ConfigureAwait(false);
            using (var dbContext = await GetDbContextAsync(deadBlocksRequest.TaskId).ConfigureAwait(false))
            {
                var items = await GetBlockQueryItems(deadBlocksRequest, blockItemDelegateRunner, taskDefinition,
                    dbContext);


                foreach (var item in items)
                {
                    var blockType = (BlockType)item.BlockType;
                    if (blockType == deadBlocksRequest.BlockType)
                    {
                        var objectBlock = new ObjectBlock<T>();
                        objectBlock.ObjectBlockId = item.BlockId;
                        objectBlock.Attempt = item.Attempt;
                        objectBlock.Object =
                            SerializedValueReader.ReadValue<T>(item.ObjectData, item.CompressedObjectData);

                        results.Add(objectBlock);
                    }
                    else
                    {
                        throw new NotSupportedException(UnexpectedBlockTypeMessage);
                    }
                }
            }

            _logger.LogDebug($"{nameof(FindSearchableObjectBlocksAsync)} is returning {results.Count} rows");
            return results;
        });
    }

    private static async Task<List<BlockQueryItem>> GetBlockQueryItems(ISearchableBlockRequest searchableBlockRequest,
        BlockItemDelegateRunner blockItemDelegateRunner,
        TaskDefinition taskDefinition, TasklingDbContext dbContext)
    {
        var items = await blockItemDelegateRunner
            .Execute(dbContext, searchableBlockRequest, taskDefinition.TaskDefinitionId).ConfigureAwait(false);
        return items;
    }

    private async Task<IList<ObjectBlock<T>>> FindObjectBlocksOfTaskAsync<T>(
        FindBlocksOfTaskRequest blocksOfTaskRequest,
        ReprocessOption reprocessOption)
    {
        return await FindBlocksOfTaskAsync(blocksOfTaskRequest, new[] { BlockType.Object },
            (i, j) => GetObjectBlocks<T, BlockQueryItem>(j, i));
    }


    private async Task<IList<ForcedRangeBlockQueueItem>> GetForcedDateRangeBlocksAsync(
        QueuedForcedBlocksRequest queuedForcedBlocksRequest)
    {
        return await GetForcedRangeBlocksAsync(queuedForcedBlocksRequest).ConfigureAwait(false);
    }

    private async Task<IList<ForcedRangeBlockQueueItem>> GetForcedNumericRangeBlocksAsync(
        QueuedForcedBlocksRequest queuedForcedBlocksRequest)
    {
        return await GetForcedRangeBlocksAsync(queuedForcedBlocksRequest).ConfigureAwait(false);
    }

    private async Task<IList<ForcedRangeBlockQueueItem>> GetForcedRangeBlocksAsync(
        QueuedForcedBlocksRequest queuedForcedBlocksRequest)
    {
        return await RetryHelper.WithRetryAsync(async () =>
        {
            var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(queuedForcedBlocksRequest.TaskId)
                .ConfigureAwait(false);

            using (var dbContext = await GetDbContextAsync(queuedForcedBlocksRequest.TaskId))

            {
                var items = await ForcedBlockQueueQueryBuilder.GetForcedBlockQueueQueryItems(dbContext,
                    taskDefinition.TaskDefinitionId,
                    queuedForcedBlocksRequest.BlockType).ConfigureAwait(false);
                var results = new List<ForcedRangeBlockQueueItem>();
                foreach (var item in items)
                {
                    var blockType = (BlockType)item.BlockType;
                    if (blockType == queuedForcedBlocksRequest.BlockType)
                    {
                        var blockId = item.BlockId;
                        var attempt = item.Attempt;
                        var forceBlockQueueId = 0;

                        long rangeBegin;
                        long rangeEnd;

                        RangeBlock? rangeBlock = null;
                        var logger = _loggerFactory.CreateLogger<RangeBlock>();
                        if (queuedForcedBlocksRequest.BlockType == BlockType.DateRange)
                        {
                            rangeBegin = item.FromDate.Value.Ticks;
                            rangeEnd = item.ToDate.Value.Ticks;
                            rangeBlock = new RangeBlock(blockId, attempt + 1, rangeBegin, rangeEnd,
                                queuedForcedBlocksRequest.BlockType, logger);
                            forceBlockQueueId = item.ForceBlockQueueId;
                        }
                        else if (queuedForcedBlocksRequest.BlockType == BlockType.NumericRange)
                        {
                            rangeBegin = item.FromNumber.Value;
                            rangeEnd = item.ToNumber.Value;
                            rangeBlock = new RangeBlock(blockId, attempt + 1, rangeBegin, rangeEnd,
                                queuedForcedBlocksRequest.BlockType, logger);
                            forceBlockQueueId = item.ForceBlockQueueId;
                        }

                        var queueItem = new ForcedRangeBlockQueueItem
                        {
                            BlockType = queuedForcedBlocksRequest.BlockType,
                            ForcedBlockQueueId = forceBlockQueueId,
                            RangeBlock = rangeBlock
                        };

                        results.Add(queueItem);
                    }
                    else
                    {
                        throw GetBlockTypeException(queuedForcedBlocksRequest, blockType);
                    }
                }

                _logger.LogDebug($"{nameof(GetForcedRangeBlocksAsync)} is returning {results.Count} rows");
                return results;
            }
        });
    }

    private async Task<IList<ForcedListBlockQueueItem>> GetForcedListBlocksAsync(
        QueuedForcedBlocksRequest queuedForcedBlocksRequest)
    {
        return await RetryHelper.WithRetryAsync(async () =>
        {
            var results = new List<ForcedListBlockQueueItem>();
            var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(queuedForcedBlocksRequest.TaskId)
                .ConfigureAwait(false);
            using (var dbContext = await GetDbContextAsync(queuedForcedBlocksRequest.TaskId).ConfigureAwait(false))


            {
                var items = await ForcedBlockQueueQueryBuilder.GetForcedBlockQueueQueryItems(dbContext,
                    taskDefinition.TaskDefinitionId,
                    queuedForcedBlocksRequest.BlockType).ConfigureAwait(false);

                foreach (var item in items)
                {
                    var blockType = (BlockType)item.BlockType;
                    if (blockType == queuedForcedBlocksRequest.BlockType)
                    {
                        var blockId = item.BlockId;
                        var attempt = item.Attempt;
                        var forceBlockQueueId = item.ForceBlockQueueId;

                        var listBlock = new ProtoListBlock
                        {
                            ListBlockId = blockId,
                            Attempt = attempt + 1,
                            Header = SerializedValueReader.ReadValueAsString(item.ObjectData, item.CompressedObjectData)
                        };

                        var queueItem = new ForcedListBlockQueueItem
                        {
                            BlockType = queuedForcedBlocksRequest.BlockType,
                            ForcedBlockQueueId = forceBlockQueueId,
                            ListBlock = listBlock
                        };

                        results.Add(queueItem);
                    }
                    else
                    {
                        throw GetBlockTypeException(queuedForcedBlocksRequest, blockType);
                    }
                }
            }

            _logger.LogDebug($"{nameof(GetForcedListBlocksAsync)} is returning {results.Count} rows");
            return results;
        });
    }

    private async Task<IList<ForcedObjectBlockQueueItem<T>>> GetForcedObjectBlocksAsync<T>(
        QueuedForcedBlocksRequest queuedForcedBlocksRequest)
    {
        return await RetryHelper.WithRetryAsync(async () =>
        {
            var results = new List<ForcedObjectBlockQueueItem<T>>();
            var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(queuedForcedBlocksRequest.TaskId)
                .ConfigureAwait(false);

            using (var dbContext = await GetDbContextAsync(queuedForcedBlocksRequest.TaskId).ConfigureAwait(false))


            {
                var items = await ForcedBlockQueueQueryBuilder.GetForcedBlockQueueQueryItems(dbContext,
                    taskDefinition.TaskDefinitionId,
                    queuedForcedBlocksRequest.BlockType).ConfigureAwait(false);


                foreach (var item in items)
                {
                    var blockType = (BlockType)item.BlockType;
                    if (blockType == queuedForcedBlocksRequest.BlockType)
                    {
                        var objectData =
                            SerializedValueReader.ReadValue<T>(item.ObjectData, item.CompressedObjectData);

                        var objectBlock = new ObjectBlock<T>
                        {
                            ObjectBlockId = item.BlockId,
                            Attempt = item.Attempt + 1,
                            Object = objectData
                        };

                        var queueItem = new ForcedObjectBlockQueueItem<T>
                        {
                            BlockType = queuedForcedBlocksRequest.BlockType,
                            ForcedBlockQueueId = item.ForceBlockQueueId,
                            ObjectBlock = objectBlock
                        };

                        results.Add(queueItem);
                    }
                    else
                    {
                        throw GetBlockTypeException(queuedForcedBlocksRequest, blockType);
                    }
                }
            }

            _logger.LogDebug($"{nameof(GetForcedObjectBlocksAsync)} is returning {results.Count} rows");
            return results;
        });
    }

    private static ExecutionException GetBlockTypeException(IBlockRequest blockTypedRequest,
        BlockType blockType)
    {
        return new ExecutionException(
            @"The block type of the process does not match the block type of the queued item. 
This could occur if the block type of the process has been changed during a new development. Expected: " +
            blockTypedRequest.BlockType + " but queued block is: " + blockType);
    }

    private async Task UpdateForcedBlocksAsync(DequeueForcedBlocksRequest dequeueForcedBlocksRequest)
    {
        await RetryHelper.WithRetryAsync(async () =>
        {
            using (var dbContext = await GetDbContextAsync(dequeueForcedBlocksRequest.TaskId).ConfigureAwait(false))
            {
                var forceBlockQueues = await dbContext.ForceBlockQueues
                    .Where(i => dequeueForcedBlocksRequest.ForcedBlockQueueIds.Contains(i.ForceBlockQueueId))
                    .ToListAsync().ConfigureAwait(false);
                foreach (var forceBlockQueueId in forceBlockQueues)
                {
                    forceBlockQueueId.ProcessingStatus = "Execution Created";
                    dbContext.ForceBlockQueues.Update(forceBlockQueueId);
                }

                await dbContext.SaveChangesAsync().ConfigureAwait(false);
            }
        });
    }


    private async Task<long> AddBlockExecutionAsync(BlockExecutionCreateRequest executionCreateRequest)
    {
        return await RetryHelper.WithRetryAsync(async () =>
        {
            using (var dbContext = await GetDbContextAsync(executionCreateRequest.TaskId).ConfigureAwait(false))

            {
                var blockExecution = new BlockExecution
                {
                    TaskExecutionId = executionCreateRequest.TaskExecutionId,
                    BlockId = executionCreateRequest.BlockId,
                    Attempt = executionCreateRequest.Attempt,
                    BlockExecutionStatus = (int)BlockExecutionStatus.NotStarted,
                    CreatedAt = DateTime.UtcNow
                };
                await dbContext.BlockExecutions.AddAsync(blockExecution).ConfigureAwait(false);
                await dbContext.SaveChangesAsync().ConfigureAwait(false);
                _logger.LogDebug($"Created {nameof(BlockExecution)} # {blockExecution.BlockExecutionId}");
                return blockExecution.BlockExecutionId;
            }
        });
    }
}