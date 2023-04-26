using System.Data;
using System.Data.SqlClient;
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
using Taskling.SqlServer.Blocks.QueryBuilders;
using Taskling.SqlServer.Blocks.Serialization;
using Taskling.SqlServer.Models;
using Taskling.Tasks;

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
        Func<BlocksOfTaskQueryBuilder.BlocksOfTaskQueryParams, Expression<Func<BlocksOfTaskQueryBuilder.BlocksOfTaskQueryItem, bool>>> query;
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
        Func<BlocksOfTaskQueryBuilder.BlocksOfTaskQueryParams, Expression<Func<BlocksOfTaskQueryBuilder.BlocksOfTaskQueryItem, bool>>> query)
    {
        var results = new List<RangeBlock>();
        var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(blocksOfTaskRequest.TaskId)
            .ConfigureAwait(false);

        try
        {
            using (var dbContext = await GetDbContextAsync(blocksOfTaskRequest.TaskId))
            {
                var items = await BlocksOfTaskQueryBuilder.GetBlocksOfTaskQueryItems(dbContext, taskDefinition.TaskDefinitionId,
                    blocksOfTaskRequest.ReferenceValueOfTask, query(new BlocksOfTaskQueryBuilder.BlocksOfTaskQueryParams()));

                foreach (var item in items)
                {
                    var blockType = (BlockType)item.BlockType;
                    if (blockType != blocksOfTaskRequest.BlockType)
                        throw new ExecutionException(
                            "The block with this reference value is of a different BlockType. BlockType resuested: " +
                            blocksOfTaskRequest.BlockType + " BlockType found: " + blockType);

                    var rangeBlockId = item.BlockId;
                    var attempt = item.Attempt;
                    long rangeBegin;
                    long rangeEnd;
                    if (blocksOfTaskRequest.BlockType == BlockType.DateRange)
                    {
                        rangeBegin = item.FromDate.Value.Ticks; //reader.GetDateTime("FromDate").Ticks;
                        rangeEnd = item.ToDate.Value.Ticks; //reader.GetDateTime("ToDate").Ticks;
                    }
                    else
                    {
                        rangeBegin = item.FromNumber.Value;
                        rangeEnd = item.ToNumber.Value;
                    }

                    results.Add(new RangeBlock(rangeBlockId, attempt, rangeBegin, rangeEnd,
                        blocksOfTaskRequest.BlockType));
                }
            }
        }
        catch (SqlException sqlEx)
        {
            if (TransientErrorDetector.IsTransient(sqlEx))
                throw new TransientException("A transient exception has occurred", sqlEx);

            throw;
        }

        return results;
    }

    private async Task<RangeBlock> AddDateRangeRangeBlockAsync(RangeBlockCreateRequest dateRangeBlockCreateRequest,
        int taskDefinitionId)
    {
        try
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
        }
        catch (SqlException sqlEx)
        {
            if (TransientErrorDetector.IsTransient(sqlEx))
                throw new TransientException("A transient exception has occurred", sqlEx);

            throw;
        }
    }

    private async Task<RangeBlock> AddNumericRangeRangeBlockAsync(RangeBlockCreateRequest dateRangeBlockCreateRequest,
        int taskDefinitionId)
    {
        try
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
        }
        catch (SqlException sqlEx)
        {
            if (TransientErrorDetector.IsTransient(sqlEx))
                throw new TransientException("A transient exception has occurred", sqlEx);

            throw;
        }
    }

    #endregion .: Range Blocks :.

    #region .: List Blocks :.

  
    private async Task<IList<ProtoListBlock>> FindListBlocksOfTaskAsync(FindBlocksOfTaskRequest blocksOfTaskRequest,
        Func<BlocksOfTaskQueryBuilder.BlocksOfTaskQueryParams, Expression<Func<BlocksOfTaskQueryBuilder.BlocksOfTaskQueryItem, bool>>> query,
        ReprocessOption reprocessOption)
    {
        var results = new List<ProtoListBlock>();
        var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(blocksOfTaskRequest.TaskId)
            .ConfigureAwait(false);

        try
        {
            using (var dbContext = await GetDbContextAsync(blocksOfTaskRequest.TaskId))
            {
                BlocksOfTaskQueryBuilder.BlocksOfTaskQueryParams blocksOfTaskQueryParams;
                if (reprocessOption == ReprocessOption.PendingOrFailed)

                    blocksOfTaskQueryParams = new BlocksOfTaskQueryBuilder.BlocksOfTaskQueryParams
                    {
                        Started = (int)BlockExecutionStatus.Started,
                        NotStarted = (int)BlockExecutionStatus.NotStarted,
                        Failed = (int)BlockExecutionStatus.Failed
                    };
                else blocksOfTaskQueryParams = new BlocksOfTaskQueryBuilder.BlocksOfTaskQueryParams();
                var items = await BlocksOfTaskQueryBuilder.GetBlocksOfTaskQueryItems(dbContext, taskDefinition.TaskDefinitionId,
                    blocksOfTaskRequest.ReferenceValueOfTask, query(blocksOfTaskQueryParams));

                foreach (var reader in items)
                {
                    var blockType = (BlockType)reader.BlockType;
                    if (blockType != blocksOfTaskRequest.BlockType)
                        throw GetBlockTypeException(blocksOfTaskRequest, blockType);


                    var listBlock = new ProtoListBlock();
                    listBlock.ListBlockId = reader.BlockId;
                    listBlock.Attempt = reader.Attempt;
                    listBlock.Header =
                        SerializedValueReader.ReadValueAsString(reader, i => i.ObjectData,
                            i => i.CompressedObjectData);

                    results.Add(listBlock);
                }
            }
        }
        catch (SqlException sqlEx)
        {
            if (TransientErrorDetector.IsTransient(sqlEx))
                throw new TransientException("A transient exception has occurred", sqlEx);

            throw;
        }

        return results;
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

        try
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
        }
        catch (SqlException sqlEx)
        {
            if (TransientErrorDetector.IsTransient(sqlEx))
                throw new TransientException("A transient exception has occurred", sqlEx);

            throw;
        }
    }

    private async Task AddListBlockItemsAsync(long blockId, ListBlockCreateRequest createRequest)
    {
        using (var dbContext = await GetDbContextAsync(createRequest.TaskId).ConfigureAwait(false))

        {
            var transaction = await dbContext.Database.BeginTransactionAsync();


            try
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


                await transaction.CommitAsync();
            }
            catch (SqlException sqlEx)
            {
                TryRollBack(transaction, sqlEx);
            }
            catch (Exception ex)
            {
                TryRollback(transaction, ex);
            }
        }
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

        try
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
        }
        catch (SqlException sqlEx)
        {
            if (TransientErrorDetector.IsTransient(sqlEx))
                throw new TransientException("A transient exception has occurred", sqlEx);

            throw;
        }
    }

   
    private async Task<IList<ObjectBlock<T>>> FindDeadObjectBlocksAsync<T>(FindDeadBlocksRequest deadBlocksRequest,
        string query)
    {
        var results = new List<ObjectBlock<T>>();
        var taskDefinition =
            await _taskRepository.EnsureTaskDefinitionAsync(deadBlocksRequest.TaskId).ConfigureAwait(false);

        try
        {
            using (var connection = await CreateNewConnectionAsync(deadBlocksRequest.TaskId).ConfigureAwait(false))
            {
                var command = connection.CreateCommand();
                command.CommandText = query;
                command.CommandTimeout =
                    ConnectionStore.Instance.GetConnection(deadBlocksRequest.TaskId).QueryTimeoutSeconds;
                command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinition.TaskDefinitionId;
                command.Parameters.Add("@SearchPeriodBegin", SqlDbType.DateTime).Value =
                    deadBlocksRequest.SearchPeriodBegin;
                command.Parameters.Add("@SearchPeriodEnd", SqlDbType.DateTime).Value =
                    deadBlocksRequest.SearchPeriodEnd;
                command.Parameters.Add("@AttemptLimit", SqlDbType.Int).Value =
                    deadBlocksRequest.RetryLimit + 1; // RetryLimit + 1st attempt

                using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        var blockType = (BlockType)reader.GetInt32("BlockType");
                        if (blockType == deadBlocksRequest.BlockType)
                        {
                            var objectBlock = new ObjectBlock<T>();
                            objectBlock.ObjectBlockId = reader.GetInt64("BlockId");
                            objectBlock.Attempt = reader.GetInt32("Attempt");
                            objectBlock.Object =
                                SerializedValueReader.ReadValue<T>(reader, "ObjectData", "CompressedObjectData");

                            results.Add(objectBlock);
                        }
                        else
                        {
                            throw new NotSupportedException(UnexpectedBlockTypeMessage);
                        }
                    }
                }
            }
        }
        catch (SqlException sqlEx)
        {
            if (TransientErrorDetector.IsTransient(sqlEx))
                throw new TransientException("A transient exception has occurred", sqlEx);

            throw;
        }

        return results;
    }

    private async Task<IList<ObjectBlock<T>>> FindObjectBlocksOfTaskAsync<T>(
        FindBlocksOfTaskRequest blocksOfTaskRequest,
        Func<BlocksOfTaskQueryBuilder.BlocksOfTaskQueryParams, Expression<Func<BlocksOfTaskQueryBuilder.BlocksOfTaskQueryItem, bool>>> query,
        ReprocessOption reprocessOption)
    {
        var results = new List<ObjectBlock<T>>();
        var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(blocksOfTaskRequest.TaskId)
            .ConfigureAwait(false);

        try
        {
            using (var dbContext = await GetDbContextAsync(blocksOfTaskRequest.TaskId).ConfigureAwait(false))
            {
                var blockOfTypeQueryParams = new BlocksOfTaskQueryBuilder.BlocksOfTaskQueryParams();


                if (reprocessOption == ReprocessOption.PendingOrFailed)
                {
                    blockOfTypeQueryParams.NotStarted = (int)BlockExecutionStatus.NotStarted;
                    blockOfTypeQueryParams.Started = (int)BlockExecutionStatus.Started;
                    blockOfTypeQueryParams.Failed = (int)BlockExecutionStatus.Failed;
                }

                var items = await BlocksOfTaskQueryBuilder.GetBlocksOfTaskQueryItems(dbContext, taskDefinition.TaskDefinitionId,
                    blocksOfTaskRequest.ReferenceValueOfTask, query(blockOfTypeQueryParams)).ConfigureAwait(false);

                foreach (var item in items)
                {
                    var blockType = (BlockType)item.BlockType;
                    if (blockType != blocksOfTaskRequest.BlockType)
                        throw new ExecutionException(
                            "The block with this reference value is of a different BlockType. BlockType resuested: " +
                            blocksOfTaskRequest.BlockType + " BlockType found: " + blockType);

                    var objectBlock = new ObjectBlock<T>();
                    objectBlock.ObjectBlockId = item.BlockId;
                    objectBlock.Attempt = item.Attempt;
                    objectBlock.Object =
                        SerializedValueReader.ReadValue<T>(item.ObjectData, item.CompressedObjectData);

                    results.Add(objectBlock);
                }
            }
        }
        catch (SqlException sqlEx)
        {
            if (TransientErrorDetector.IsTransient(sqlEx))
                throw new TransientException("A transient exception has occurred", sqlEx);

            throw;
        }

        return results;
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
        var results = new List<ForcedRangeBlockQueueItem>();
        var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(queuedForcedBlocksRequest.TaskId)
            .ConfigureAwait(false);

        try
        {
            using (var dbContext = await GetDbContextAsync(queuedForcedBlocksRequest.TaskId))

            {
                var items = await ForcedBlockQueueQueryBuilder.GetForcedBlockQueueQueryItems(dbContext, taskDefinition.TaskDefinitionId,
                    queuedForcedBlocksRequest.BlockType).ConfigureAwait(false);

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
            }
        }
        catch (SqlException sqlEx)
        {
            if (TransientErrorDetector.IsTransient(sqlEx))
                throw new TransientException("A transient exception has occurred", sqlEx);

            throw;
        }

        return results;
    }

    private async Task<IList<ForcedListBlockQueueItem>> GetForcedListBlocksAsync(
        QueuedForcedBlocksRequest queuedForcedBlocksRequest)
    {
        var results = new List<ForcedListBlockQueueItem>();
        var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(queuedForcedBlocksRequest.TaskId)
            .ConfigureAwait(false);

        try
        {
            using (var dbContext = await GetDbContextAsync(queuedForcedBlocksRequest.TaskId).ConfigureAwait(false))


            {
                var items = await ForcedBlockQueueQueryBuilder.GetForcedBlockQueueQueryItems(dbContext, taskDefinition.TaskDefinitionId,
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
        }
        catch (SqlException sqlEx)
        {
            if (TransientErrorDetector.IsTransient(sqlEx))
                throw new TransientException("A transient exception has occurred", sqlEx);

            throw;
        }

        return results;
    }

    private async Task<IList<ForcedObjectBlockQueueItem<T>>> GetForcedObjectBlocksAsync<T>(
        QueuedForcedBlocksRequest queuedForcedBlocksRequest)
    {
        var results = new List<ForcedObjectBlockQueueItem<T>>();
        var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(queuedForcedBlocksRequest.TaskId)
            .ConfigureAwait(false);

        try
        {
            using (var dbContext = await GetDbContextAsync(queuedForcedBlocksRequest.TaskId).ConfigureAwait(false))


            {
                var items = await ForcedBlockQueueQueryBuilder.GetForcedBlockQueueQueryItems(dbContext, taskDefinition.TaskDefinitionId,
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
        }
        catch (SqlException sqlEx)
        {
            if (TransientErrorDetector.IsTransient(sqlEx))
                throw new TransientException("A transient exception has occurred", sqlEx);

            throw;
        }

        return results;
    }

    private static ExecutionException GetBlockTypeException(BlockRequestBase queuedForcedBlocksRequest,
        BlockType blockType)
    {
        return new ExecutionException(
            @"The block type of the process does not match the block type of the queued item. 
This could occur if the block type of the process has been changed during a new development. Expected: " +
            queuedForcedBlocksRequest.BlockType + " but queued block is: " + blockType);
    }

    private async Task UpdateForcedBlocksAsync(DequeueForcedBlocksRequest dequeueForcedBlocksRequest)
    {
        try
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
        }
        catch (SqlException sqlEx)
        {
            if (TransientErrorDetector.IsTransient(sqlEx))
                throw new TransientException("A transient exception has occurred", sqlEx);

            throw;
        }
    }

    #endregion .: Force Block Queue :.


    private DateTime EnsureSqlSafeDateTime(DateTime dateTime)
    {
        if (dateTime.Year < 1900)
            return new DateTime(1900, 1, 1);

        return dateTime;
    }

    private async Task<long> AddBlockExecutionAsync(BlockExecutionCreateRequest executionCreateRequest)
    {
        long blockExecutionId = 0;
        try
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
        }
        catch (SqlException sqlEx)
        {
            if (TransientErrorDetector.IsTransient(sqlEx))
                throw new TransientException("A transient exception has occurred", sqlEx);

            throw;
        }

        return blockExecutionId;
    }

    #endregion .: Private Methods :.

  
}