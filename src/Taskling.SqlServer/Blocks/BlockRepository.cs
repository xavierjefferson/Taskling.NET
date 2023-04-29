using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;
using Taskling.Blocks.Common;
using Taskling.Blocks.ListBlocks;
using Taskling.Blocks.ObjectBlocks;
using Taskling.Blocks.RangeBlocks;
using Taskling.Exceptions;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.Blocks.CommonRequests.ForcedBlocks;
using Taskling.InfrastructureContracts.Blocks.ListBlocks;
using Taskling.InfrastructureContracts.Blocks.ObjectBlocks;
using Taskling.InfrastructureContracts.Blocks.RangeBlocks;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.Serialization;
using Taskling.SqlServer.AncilliaryServices;
using Taskling.SqlServer.Blocks.Models;
using Taskling.SqlServer.Blocks.QueryBuilders;
using Taskling.SqlServer.Blocks.Serialization;
using Taskling.SqlServer.Models;
using Taskling.Tasks;
using TaskDefinition = Taskling.InfrastructureContracts.TaskExecution.TaskDefinition;

namespace Taskling.SqlServer.Blocks;

public partial class BlockRepository : DbOperationsService, IBlockRepository
{
    #region .: Constructor :.

    public BlockRepository(ITaskRepository taskRepository)
    {
        _taskRepository = taskRepository;
    }

    #endregion .: Constructor :.

    #region .: Fields and services :.

    private readonly ITaskRepository _taskRepository;

    private const string UnexpectedBlockTypeMessage =
        "This block type was not expected. This can occur when changing the block type of an existing process or combining different block types in a single process - which is not supported";

    #endregion .: Fields and services :.

    #region .: Public Methods :.

    #region .: Force Block Queue :.

    public async Task<IList<ForcedRangeBlockQueueItem>> GetQueuedForcedRangeBlocksAsync(
        QueuedForcedBlocksRequest queuedForcedBlocksRequest)
    {
        switch (queuedForcedBlocksRequest.BlockType)
        {
            case BlockType.DateRange:
                return await GetForcedDateRangeBlocksAsync(queuedForcedBlocksRequest).ConfigureAwait(false);
            case BlockType.NumericRange:
                return await GetForcedNumericRangeBlocksAsync(queuedForcedBlocksRequest).ConfigureAwait(false);
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

    #endregion .: Force Block Queue :.

    #region .: Range Blocks :.

    public async Task<IList<RangeBlock>> FindRangeBlocksOfTaskAsync(FindBlocksOfTaskRequest blocksOfTaskRequest)
    {
        Func<BlocksOfTaskQueryParams,
            Expression<Func<BlockQueryItem, bool>>> query;
        switch (blocksOfTaskRequest.BlockType)
        {
            case BlockType.DateRange:
                query = BlocksOfTaskQueryBuilder.GetFindDateRangeBlocksOfTaskQuery(blocksOfTaskRequest.ReprocessOption);
                break;
            case BlockType.NumericRange:
                query = BlocksOfTaskQueryBuilder.GetFindNumericRangeBlocksOfTaskQuery(blocksOfTaskRequest
                    .ReprocessOption);
                break;
            default:
                throw new NotSupportedException("This range type is not supported");
        }

        return await FindRangeBlocksOfTaskAsync(blocksOfTaskRequest, query).ConfigureAwait(false);
    }

    public async Task<RangeBlockCreateResponse> AddRangeBlockAsync(RangeBlockCreateRequest rangeBlockCreateRequest)
    {
        var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(rangeBlockCreateRequest.TaskId)
            .ConfigureAwait(false);

        var response = new RangeBlockCreateResponse();
        switch (rangeBlockCreateRequest.BlockType)
        {
            case BlockType.DateRange:
                response.Block =
                    await AddDateRangeRangeBlockAsync(rangeBlockCreateRequest, taskDefinition.TaskDefinitionId)
                        .ConfigureAwait(false);
                break;
            case BlockType.NumericRange:
                response.Block =
                    await AddNumericRangeRangeBlockAsync(rangeBlockCreateRequest, taskDefinition.TaskDefinitionId)
                        .ConfigureAwait(false);
                break;
            default:
                throw new NotSupportedException(UnexpectedBlockTypeMessage);
        }

        return response;
    }

    public async Task<long> AddRangeBlockExecutionAsync(BlockExecutionCreateRequest executionCreateRequest)
    {
        return await AddBlockExecutionAsync(executionCreateRequest).ConfigureAwait(false);
    }

    #endregion .: Range Blocks :.

    #region .: List Blocks :.

    public async Task<IList<ProtoListBlock>> FindListBlocksOfTaskAsync(FindBlocksOfTaskRequest blocksOfTaskRequest)
    {
        if (blocksOfTaskRequest.BlockType == BlockType.List)
        {
            var query = BlocksOfTaskQueryBuilder.GetFindListBlocksOfTaskQuery(blocksOfTaskRequest.ReprocessOption);
            return await FindListBlocksOfTaskAsync(blocksOfTaskRequest, query, blocksOfTaskRequest.ReprocessOption)
                .ConfigureAwait(false);
        }

        throw new NotSupportedException(UnexpectedBlockTypeMessage);
    }

    public async Task<ListBlockCreateResponse> AddListBlockAsync(ListBlockCreateRequest createRequest)
    {
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

    #endregion .: List Blocks :.

    #region .: Object Blocks :.

    public async Task<IList<ObjectBlock<T>>> FindObjectBlocksOfTaskAsync<T>(FindBlocksOfTaskRequest blocksOfTaskRequest)
    {
        if (blocksOfTaskRequest.BlockType == BlockType.Object)
        {
            var query = BlocksOfTaskQueryBuilder.GetFindObjectBlocksOfTaskQuery(blocksOfTaskRequest.ReprocessOption);
            return await FindObjectBlocksOfTaskAsync<T>(blocksOfTaskRequest, query, blocksOfTaskRequest.ReprocessOption)
                .ConfigureAwait(false);
        }

        throw new NotSupportedException(UnexpectedBlockTypeMessage);
    }


    public async Task<long> AddObjectBlockExecutionAsync(BlockExecutionCreateRequest executionCreateRequest)
    {
        return await AddBlockExecutionAsync(executionCreateRequest).ConfigureAwait(false);
    }

    public async Task<ObjectBlockCreateResponse<T>> AddObjectBlockAsync<T>(ObjectBlockCreateRequest<T> createRequest)
    {
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

    #endregion .: Object Blocks :.

    #endregion .: Public Methods :.

    #region .: Private Methods :.

    #region .: Range Blocks :.

    private async Task<IList<RangeBlock>> FindRangeBlocksOfTaskAsync(FindBlocksOfTaskRequest blocksOfTaskRequest,
        Func<BlocksOfTaskQueryParams,
            Expression<Func<BlockQueryItem, bool>>> query)
    {
        return await RetryHelper.WithRetry(async (transactionScope) =>
        {
            var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(blocksOfTaskRequest.TaskId)
                .ConfigureAwait(false);

            using (var dbContext = await GetDbContextAsync(blocksOfTaskRequest.TaskId))
            {
                var items = await BlocksOfTaskQueryBuilder.GetBlocksOfTaskQueryItems(dbContext,
                    taskDefinition.TaskDefinitionId,
                    blocksOfTaskRequest.ReferenceValueOfTask,
                    query(new BlocksOfTaskQueryParams()));
                var results = GetRangeBlocks(blocksOfTaskRequest, items);

                return results;
            }
        }).ConfigureAwait(false);
    }

    private async Task<RangeBlock> AddDateRangeRangeBlockAsync(RangeBlockCreateRequest dateRangeBlockCreateRequest,
        int taskDefinitionId)
    {
        return await RetryHelper.WithRetry(async (transactionScope) =>
        {
            using (var dbContext = await GetDbContextAsync(dateRangeBlockCreateRequest.TaskId))

            {
                var block = new Block
                {
                    TaskDefinitionId = taskDefinitionId,
                    FromDate = new DateTime(dateRangeBlockCreateRequest.From),
                    ToDate = new DateTime(dateRangeBlockCreateRequest.To),
                    BlockType = (int)BlockType.DateRange,
                    CreatedDate = DateTime.UtcNow
                };
                await dbContext.Blocks.AddAsync(block).ConfigureAwait(false);
                await dbContext.SaveChangesAsync().ConfigureAwait(false);

                return new RangeBlock(block.BlockId,
                    0,
                    dateRangeBlockCreateRequest.From,
                    dateRangeBlockCreateRequest.To,
                    dateRangeBlockCreateRequest.BlockType);
            }
        }).ConfigureAwait(false);
    }

    private async Task<RangeBlock> AddNumericRangeRangeBlockAsync(RangeBlockCreateRequest dateRangeBlockCreateRequest,
        int taskDefinitionId)
    {
        return await RetryHelper.WithRetry(async (transactionScope) =>
        {
            using (var dbContext = await GetDbContextAsync(dateRangeBlockCreateRequest.TaskId))
            {
                var block = new Block
                {
                    TaskDefinitionId = taskDefinitionId,
                    FromNumber = dateRangeBlockCreateRequest.From,
                    ToNumber = dateRangeBlockCreateRequest.To,
                    BlockType = (int)BlockType.NumericRange,
                    CreatedDate = DateTime.UtcNow
                };
                await dbContext.Blocks.AddAsync(block).ConfigureAwait(false);
                await dbContext.SaveChangesAsync().ConfigureAwait(false);

                return new RangeBlock(block.BlockId, 0, dateRangeBlockCreateRequest.From,
                    dateRangeBlockCreateRequest.To,
                    dateRangeBlockCreateRequest.BlockType);
            }
        }).ConfigureAwait(false);
    }

    #endregion .: Range Blocks :.

    #region .: List Blocks :.

    private async Task<IList<ProtoListBlock>> FindListBlocksOfTaskAsync(FindBlocksOfTaskRequest blocksOfTaskRequest,
        Func<BlocksOfTaskQueryParams,
            Expression<Func<BlockQueryItem, bool>>> query,
        ReprocessOption reprocessOption)
    {
        return await RetryHelper.WithRetry(async (transactionScope) =>
        {
            var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(blocksOfTaskRequest.TaskId)
                .ConfigureAwait(false);


            using (var dbContext = await GetDbContextAsync(blocksOfTaskRequest.TaskId))
            {
                BlocksOfTaskQueryParams blocksOfTaskQueryParams;
                if (reprocessOption == ReprocessOption.PendingOrFailed)

                    blocksOfTaskQueryParams = new BlocksOfTaskQueryParams
                    {
                        StatusesToMatch = new List<int>
                        {
                            (int)BlockExecutionStatus.Started, (int)BlockExecutionStatus.NotStarted,
                            (int)BlockExecutionStatus.Failed
                        }
                    };
                else blocksOfTaskQueryParams = new BlocksOfTaskQueryParams();
                var items = await BlocksOfTaskQueryBuilder.GetBlocksOfTaskQueryItems(dbContext,
                    taskDefinition.TaskDefinitionId,
                    blocksOfTaskRequest.ReferenceValueOfTask, query(blocksOfTaskQueryParams));
                var results = GetListBlocks(blocksOfTaskRequest, items);
                return results;
                ;
            }
        });
    }


    private async Task<long> AddNewListBlockAsync(TaskId taskId, int taskDefinitionId, string header,
        int compressionThreshold)
    {
        if (header == null)
            header = string.Empty;

        var isLargeTextValue = false;
        byte[] compressedData = null;
        if (header.Length > compressionThreshold)
        {
            isLargeTextValue = true;
            compressedData = LargeValueCompressor.Zip(header);
        }

        return await RetryHelper.WithRetry(async (transactionScope) =>
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
        await RetryHelper.WithRetry(async (transactionScope) =>
        {
            using (var dbContext = await GetDbContextAsync(createRequest.TaskId).ConfigureAwait(false))

            {
                 


                foreach (var value in (List<string>)createRequest.SerializedValues)
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

    #endregion .: List Blocks :.

    #region .: Object Blocks :.

    private async Task<long> AddNewObjectBlockAsync<T>(TaskId taskId, int taskDefinitionId, T objectData,
        int compressionThreshold)
    {
        var isLargeTextValue = false;
        var jsonValue = JsonGenericSerializer.Serialize(objectData);
        byte[] compressedData = null;
        if (jsonValue.Length > compressionThreshold)
        {
            isLargeTextValue = true;
            compressedData = LargeValueCompressor.Zip(jsonValue);
        }

        return await RetryHelper.WithRetry(async (transactionScope) =>
        {
            using (var dbContext = await GetDbContextAsync(taskId).ConfigureAwait(false))

            {
                var block = new Block
                {
                    TaskDefinitionId = taskDefinitionId,

                    BlockType = (int)BlockType.Object,
                    CreatedDate = DateTime.UtcNow
                };
                if (isLargeTextValue)
                {
                    block.ObjectData = null;
                    block.CompressedObjectData = compressedData;
                }
                else
                {
                    block.ObjectData = jsonValue;
                    block.CompressedObjectData = null;
                }

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
        return await RetryHelper.WithRetry(async (transactionScope) =>
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
        Func<BlocksOfTaskQueryParams,
            Expression<Func<BlockQueryItem, bool>>> query,
        ReprocessOption reprocessOption)
    {
        return await RetryHelper.WithRetry(async (transactionScope) =>
        {
            var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(blocksOfTaskRequest.TaskId)
                .ConfigureAwait(false);
            using (var dbContext = await GetDbContextAsync(blocksOfTaskRequest.TaskId).ConfigureAwait(false))
            {
                var blockOfTypeQueryParams = new BlocksOfTaskQueryParams();


                if (reprocessOption == ReprocessOption.PendingOrFailed)
                    blockOfTypeQueryParams.StatusesToMatch = new List<int>
                    {
                        (int)BlockExecutionStatus.Started, (int)BlockExecutionStatus.NotStarted,
                        (int)BlockExecutionStatus.Failed
                    };

                var items = await BlocksOfTaskQueryBuilder.GetBlocksOfTaskQueryItems(dbContext,
                    taskDefinition.TaskDefinitionId,
                    blocksOfTaskRequest.ReferenceValueOfTask, query(blockOfTypeQueryParams)).ConfigureAwait(false);
                return GetObjectBlocks<T, BlockQueryItem>(blocksOfTaskRequest, items);
            }
        });
    }

    #endregion .: Object Blocks :.

    #region .: Force Block Queue :.

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
        return await RetryHelper.WithRetry(async (transactionScope) =>
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
                        if (queuedForcedBlocksRequest.BlockType == BlockType.DateRange)
                        {
                            rangeBegin = item.FromDate.Value.Ticks;
                            rangeEnd = item.ToDate.Value.Ticks;
                            rangeBlock = new RangeBlock(blockId, attempt + 1, rangeBegin, rangeEnd,
                                queuedForcedBlocksRequest.BlockType);
                            forceBlockQueueId = item.ForceBlockQueueId;
                        }
                        else if (queuedForcedBlocksRequest.BlockType == BlockType.NumericRange)
                        {
                            rangeBegin = item.FromNumber.Value;
                            rangeEnd = item.ToNumber.Value;
                            rangeBlock = new RangeBlock(blockId, attempt + 1, rangeBegin, rangeEnd,
                                queuedForcedBlocksRequest.BlockType);
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

                return results;
            }
        });
    }

    private async Task<IList<ForcedListBlockQueueItem>> GetForcedListBlocksAsync(
        QueuedForcedBlocksRequest queuedForcedBlocksRequest)
    {
        return await RetryHelper.WithRetry(async (transactionScope) =>
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
                            Header = SerializedValueReader.ReadValueAsString(item, i => i.ObjectData,
                                i => i.CompressedObjectData)
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

            return results;
        });
    }

    private async Task<IList<ForcedObjectBlockQueueItem<T>>> GetForcedObjectBlocksAsync<T>(
        QueuedForcedBlocksRequest queuedForcedBlocksRequest)
    {
        return await RetryHelper.WithRetry(async (transactionScope) =>
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
        await RetryHelper.WithRetry(async (transactionScope) =>
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

    #endregion .: Force Block Queue :.


    private async Task<long> AddBlockExecutionAsync(BlockExecutionCreateRequest executionCreateRequest)
    {
        return await RetryHelper.WithRetry(async (transactionScope) =>
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
                return blockExecution.BlockExecutionId;
            }
        });
    }

    #endregion .: Private Methods :.
}