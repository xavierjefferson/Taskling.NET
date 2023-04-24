﻿using System.Data;
using System.Data.SqlClient;
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
using Taskling.Tasks;

namespace Taskling.SqlServer.Blocks;

public class BlockRepository : DbOperationsService, IBlockRepository
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
        var query = string.Empty;
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

    public async Task<IList<RangeBlock>> FindFailedRangeBlocksAsync(FindFailedBlocksRequest failedBlocksRequest)
    {
        var query = string.Empty;
        switch (failedBlocksRequest.BlockType)
        {
            case BlockType.DateRange:
                query = FailedBlocksQueryBuilder.GetFindFailedDateRangeBlocksQuery(failedBlocksRequest.BlockCountLimit);
                break;
            case BlockType.NumericRange:
                query = FailedBlocksQueryBuilder.GetFindFailedNumericRangeBlocksQuery(failedBlocksRequest
                    .BlockCountLimit);
                break;
            default:
                throw new NotSupportedException("This range type is not supported");
        }

        return await FindFailedDateRangeBlocksAsync(failedBlocksRequest, query).ConfigureAwait(false);
    }

    public async Task<IList<RangeBlock>> FindDeadRangeBlocksAsync(FindDeadBlocksRequest deadBlocksRequest)
    {
        var query = string.Empty;
        switch (deadBlocksRequest.BlockType)
        {
            case BlockType.DateRange:
                if (deadBlocksRequest.TaskDeathMode == TaskDeathMode.KeepAlive)
                    query = DeadBlocksQueryBuilder.GetFindDeadDateRangeBlocksWithKeepAliveQuery(deadBlocksRequest
                        .BlockCountLimit);
                else
                    query = DeadBlocksQueryBuilder.GetFindDeadDateRangeBlocksQuery(deadBlocksRequest.BlockCountLimit);
                break;
            case BlockType.NumericRange:
                if (deadBlocksRequest.TaskDeathMode == TaskDeathMode.KeepAlive)
                    query = DeadBlocksQueryBuilder.GetFindDeadNumericRangeBlocksWithKeepAliveQuery(deadBlocksRequest
                        .BlockCountLimit);
                else
                    query = DeadBlocksQueryBuilder.GetFindDeadNumericRangeBlocksQuery(deadBlocksRequest
                        .BlockCountLimit);
                break;
            default:
                throw new NotSupportedException("This range type is not supported");
        }

        return await FindDeadDateRangeBlocksAsync(deadBlocksRequest, query).ConfigureAwait(false);
    }

    public async Task<IList<RangeBlock>> FindRangeBlocksOfTaskAsync(FindBlocksOfTaskRequest blocksOfTaskRequest)
    {
        var query = string.Empty;
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

    public async Task<IList<ProtoListBlock>> FindFailedListBlocksAsync(FindFailedBlocksRequest failedBlocksRequest)
    {
        if (failedBlocksRequest.BlockType == BlockType.List)
        {
            var query = FailedBlocksQueryBuilder.GetFindFailedListBlocksQuery(failedBlocksRequest.BlockCountLimit);
            return await FindFailedListBlocksAsync(failedBlocksRequest, query).ConfigureAwait(false);
        }

        throw new NotSupportedException(UnexpectedBlockTypeMessage);
    }

    public async Task<IList<ProtoListBlock>> FindDeadListBlocksAsync(FindDeadBlocksRequest deadBlocksRequest)
    {
        if (deadBlocksRequest.BlockType == BlockType.List)
        {
            var query = string.Empty;
            if (deadBlocksRequest.TaskDeathMode == TaskDeathMode.KeepAlive)
                query = DeadBlocksQueryBuilder.GetFindDeadListBlocksWithKeepAliveQuery(
                    deadBlocksRequest.BlockCountLimit);
            else
                query = DeadBlocksQueryBuilder.GetFindDeadListBlocksQuery(deadBlocksRequest.BlockCountLimit);

            return await FindDeadListBlocksAsync(deadBlocksRequest, query).ConfigureAwait(false);
        }

        throw new NotSupportedException(UnexpectedBlockTypeMessage);
    }

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

    public async Task<IList<ObjectBlock<T>>> FindFailedObjectBlocksAsync<T>(FindFailedBlocksRequest failedBlocksRequest)
    {
        if (failedBlocksRequest.BlockType == BlockType.Object)
        {
            var query = FailedBlocksQueryBuilder.GetFindFailedObjectBlocksQuery(failedBlocksRequest.BlockCountLimit);
            return await FindFailedObjectBlocksAsync<T>(failedBlocksRequest, query).ConfigureAwait(false);
        }

        throw new NotSupportedException(UnexpectedBlockTypeMessage);
    }

    public async Task<IList<ObjectBlock<T>>> FindDeadObjectBlocksAsync<T>(FindDeadBlocksRequest deadBlocksRequest)
    {
        if (deadBlocksRequest.BlockType == BlockType.Object)
        {
            var query = string.Empty;
            if (deadBlocksRequest.TaskDeathMode == TaskDeathMode.KeepAlive)
                query = DeadBlocksQueryBuilder.GetFindDeadObjectBlocksWithKeepAliveQuery(deadBlocksRequest
                    .BlockCountLimit);
            else
                query = DeadBlocksQueryBuilder.GetFindDeadObjectBlocksQuery(deadBlocksRequest.BlockCountLimit);

            return await FindDeadObjectBlocksAsync<T>(deadBlocksRequest, query).ConfigureAwait(false);
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

    private async Task<IList<RangeBlock>> FindFailedDateRangeBlocksAsync(FindFailedBlocksRequest failedBlocksRequest,
        string query)
    {
        var results = new List<RangeBlock>();
        var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(failedBlocksRequest.TaskId)
            .ConfigureAwait(false);

        try
        {
            using (var connection = await CreateNewConnectionAsync(failedBlocksRequest.TaskId).ConfigureAwait(false))
            {
                var command = connection.CreateCommand();
                command.CommandText = query;
                command.CommandTimeout = ConnectionStore.Instance.GetConnection(failedBlocksRequest.TaskId)
                    .QueryTimeoutSeconds;
                command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinition.TaskDefinitionId;
                command.Parameters.Add("@SearchPeriodBegin", SqlDbType.DateTime).Value =
                    failedBlocksRequest.SearchPeriodBegin;
                command.Parameters.Add("@SearchPeriodEnd", SqlDbType.DateTime).Value =
                    failedBlocksRequest.SearchPeriodEnd;
                command.Parameters.Add("@AttemptLimit", SqlDbType.Int).Value =
                    failedBlocksRequest.RetryLimit + 1; // RetryLimit + 1st attempt
                using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        var blockType = (BlockType)reader.GetInt32("BlockType");
                        if (blockType == failedBlocksRequest.BlockType)
                        {
                            var rangeBlockId = reader.GetInt64("BlockId");
                            var attempt = reader.GetInt32("Attempt");

                            long rangeBegin;
                            long rangeEnd;

                            if (failedBlocksRequest.BlockType == BlockType.DateRange)
                            {
                                rangeBegin = reader.GetDateTime(1).Ticks;
                                rangeEnd = reader.GetDateTime(2).Ticks;
                            }
                            else
                            {
                                rangeBegin = reader.GetInt64("FromNumber");
                                rangeEnd = reader.GetInt64("ToNumber");
                            }

                            results.Add(new RangeBlock(rangeBlockId, attempt, rangeBegin, rangeEnd,
                                failedBlocksRequest.BlockType));
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

    private async Task<IList<RangeBlock>> FindDeadDateRangeBlocksAsync(FindDeadBlocksRequest deadBlocksRequest,
        string query)
    {
        var results = new List<RangeBlock>();
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
                            var rangeBlockId = reader.GetInt64("BlockId");
                            var attempt = reader.GetInt32("Attempt");

                            long rangeBegin;
                            long rangeEnd;
                            if (deadBlocksRequest.BlockType == BlockType.DateRange)
                            {
                                rangeBegin = reader.GetDateTime(1).Ticks;
                                rangeEnd = reader.GetDateTime(2).Ticks;
                            }
                            else
                            {
                                rangeBegin = reader.GetInt64("FromNumber");
                                rangeEnd = reader.GetInt64("ToNumber");
                            }

                            results.Add(new RangeBlock(rangeBlockId, attempt, rangeBegin, rangeEnd, blockType));
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

    private async Task<IList<RangeBlock>> FindRangeBlocksOfTaskAsync(FindBlocksOfTaskRequest blocksOfTaskRequest,
        string query)
    {
        var results = new List<RangeBlock>();
        var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(blocksOfTaskRequest.TaskId)
            .ConfigureAwait(false);

        try
        {
            using (var connection = await CreateNewConnectionAsync(blocksOfTaskRequest.TaskId).ConfigureAwait(false))
            {
                var command = connection.CreateCommand();
                command.CommandText = query;
                command.CommandTimeout = ConnectionStore.Instance.GetConnection(blocksOfTaskRequest.TaskId)
                    .QueryTimeoutSeconds;
                command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinition.TaskDefinitionId;
                command.Parameters.Add("@ReferenceValue", SqlDbType.NVarChar, 200).Value =
                    blocksOfTaskRequest.ReferenceValueOfTask;

                using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        var blockType = (BlockType)reader.GetInt32("BlockType");
                        if (blockType != blocksOfTaskRequest.BlockType)
                            throw new ExecutionException(
                                "The block with this reference value is of a different BlockType. BlockType resuested: " +
                                blocksOfTaskRequest.BlockType + " BlockType found: " + blockType);

                        var rangeBlockId = reader.GetInt64("BlockId");
                        var attempt = reader.GetInt32("Attempt");
                        long rangeBegin;
                        long rangeEnd;
                        if (blocksOfTaskRequest.BlockType == BlockType.DateRange)
                        {
                            rangeBegin = reader.GetDateTime(1).Ticks; //reader.GetDateTime("FromDate").Ticks;
                            rangeEnd = reader.GetDateTime(2).Ticks; //reader.GetDateTime("ToDate").Ticks;
                        }
                        else
                        {
                            rangeBegin = reader.GetInt64("FromNumber");
                            rangeEnd = reader.GetInt64("ToNumber");
                        }

                        results.Add(new RangeBlock(rangeBlockId, attempt, rangeBegin, rangeEnd,
                            blocksOfTaskRequest.BlockType));
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

    private async Task<RangeBlock> AddDateRangeRangeBlockAsync(RangeBlockCreateRequest dateRangeBlockCreateRequest,
        int taskDefinitionId)
    {
        try
        {
            using (var connection =
                   await CreateNewConnectionAsync(dateRangeBlockCreateRequest.TaskId).ConfigureAwait(false))
            {
                var command = connection.CreateCommand();
                command.CommandTimeout = ConnectionStore.Instance.GetConnection(dateRangeBlockCreateRequest.TaskId)
                    .QueryTimeoutSeconds;
                command.CommandText = RangeBlockQueryBuilder.InsertDateRangeBlock;
                command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
                command.Parameters.Add("@FromDate", SqlDbType.DateTime).Value =
                    new DateTime(dateRangeBlockCreateRequest.From);
                command.Parameters.Add("@ToDate", SqlDbType.DateTime).Value =
                    new DateTime(dateRangeBlockCreateRequest.To);
                command.Parameters.Add("@BlockType", SqlDbType.Int).Value = (int)BlockType.DateRange;
                var id = (long)await command.ExecuteScalarAsync().ConfigureAwait(false);

                return new RangeBlock(id,
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
            using (var connection =
                   await CreateNewConnectionAsync(dateRangeBlockCreateRequest.TaskId).ConfigureAwait(false))
            {
                var command = connection.CreateCommand();
                command.CommandTimeout = ConnectionStore.Instance.GetConnection(dateRangeBlockCreateRequest.TaskId)
                    .QueryTimeoutSeconds;
                command.CommandText = RangeBlockQueryBuilder.InsertNumericRangeBlock;
                command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
                command.Parameters.Add("@FromNumber", SqlDbType.BigInt).Value = dateRangeBlockCreateRequest.From;
                command.Parameters.Add("@ToNumber", SqlDbType.BigInt).Value = dateRangeBlockCreateRequest.To;
                command.Parameters.Add("@BlockType", SqlDbType.Int).Value = (int)BlockType.NumericRange;
                var id = (long)await command.ExecuteScalarAsync().ConfigureAwait(false);

                return new RangeBlock(id, 0, dateRangeBlockCreateRequest.From, dateRangeBlockCreateRequest.To,
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

    private async Task<IList<ProtoListBlock>> FindFailedListBlocksAsync(FindFailedBlocksRequest failedBlocksRequest,
        string query)
    {
        var results = new List<ProtoListBlock>();
        var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(failedBlocksRequest.TaskId)
            .ConfigureAwait(false);

        try
        {
            using (var connection = await CreateNewConnectionAsync(failedBlocksRequest.TaskId).ConfigureAwait(false))
            {
                var command = connection.CreateCommand();
                command.CommandText = query;
                command.CommandTimeout = ConnectionStore.Instance.GetConnection(failedBlocksRequest.TaskId)
                    .QueryTimeoutSeconds;
                command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinition.TaskDefinitionId;
                command.Parameters.Add("@SearchPeriodBegin", SqlDbType.DateTime).Value =
                    failedBlocksRequest.SearchPeriodBegin;
                command.Parameters.Add("@SearchPeriodEnd", SqlDbType.DateTime).Value =
                    failedBlocksRequest.SearchPeriodEnd;
                command.Parameters.Add("@AttemptLimit", SqlDbType.Int).Value =
                    failedBlocksRequest.RetryLimit + 1; // RetryLimit + 1st attempt
                using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        var blockType = (BlockType)reader.GetInt32("BlockType");
                        if (blockType == failedBlocksRequest.BlockType)
                        {
                            var listBlock = new ProtoListBlock();
                            listBlock.ListBlockId = reader.GetInt64("BlockId");
                            listBlock.Attempt = reader.GetInt32("Attempt");
                            listBlock.Header =
                                SerializedValueReader.ReadValueAsString(reader, "ObjectData", "CompressedObjectData");

                            results.Add(listBlock);
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

    private async Task<IList<ProtoListBlock>> FindDeadListBlocksAsync(FindDeadBlocksRequest deadBlocksRequest,
        string query)
    {
        var results = new List<ProtoListBlock>();
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
                            var listBlock = new ProtoListBlock();

                            listBlock.ListBlockId = reader.GetInt64("BlockId");
                            listBlock.Attempt = reader.GetInt32("Attempt");
                            listBlock.Header =
                                SerializedValueReader.ReadValueAsString(reader, "ObjectData", "CompressedObjectData");

                            results.Add(listBlock);
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

    private async Task<IList<ProtoListBlock>> FindListBlocksOfTaskAsync(FindBlocksOfTaskRequest blocksOfTaskRequest,
        string query, ReprocessOption reprocessOption)
    {
        var results = new List<ProtoListBlock>();
        var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(blocksOfTaskRequest.TaskId)
            .ConfigureAwait(false);

        try
        {
            using (var connection = await CreateNewConnectionAsync(blocksOfTaskRequest.TaskId).ConfigureAwait(false))
            {
                var command = connection.CreateCommand();
                command.CommandText = query;
                command.CommandTimeout = ConnectionStore.Instance.GetConnection(blocksOfTaskRequest.TaskId)
                    .QueryTimeoutSeconds;
                command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinition.TaskDefinitionId;
                command.Parameters.Add("@ReferenceValue", SqlDbType.NVarChar, 200).Value =
                    blocksOfTaskRequest.ReferenceValueOfTask;

                if (reprocessOption == ReprocessOption.PendingOrFailed)
                {
                    command.Parameters.Add("@NotStarted", SqlDbType.Int).Value = (int)BlockExecutionStatus.NotStarted;
                    command.Parameters.Add("@Started", SqlDbType.Int).Value = (int)BlockExecutionStatus.Started;
                    command.Parameters.Add("@Failed", SqlDbType.Int).Value = (int)BlockExecutionStatus.Failed;
                }

                using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        var blockType = (BlockType)reader.GetInt32("BlockType");
                        if (blockType != blocksOfTaskRequest.BlockType)
                            throw new ExecutionException(
                                "The block with this reference value is of a different BlockType. BlockType resuested: " +
                                blocksOfTaskRequest.BlockType + " BlockType found: " + blockType);

                        var listBlock = new ProtoListBlock();
                        listBlock.ListBlockId = reader.GetInt64("BlockId");
                        listBlock.Attempt = reader.GetInt32("Attempt");
                        listBlock.Header =
                            SerializedValueReader.ReadValueAsString(reader, "ObjectData", "CompressedObjectData");

                        results.Add(listBlock);
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
            using (var connection = await CreateNewConnectionAsync(taskId).ConfigureAwait(false))
            {
                var command = connection.CreateCommand();
                command.CommandTimeout = ConnectionStore.Instance.GetConnection(taskId).QueryTimeoutSeconds;
                command.CommandText = ListBlockQueryBuilder.InsertListBlock;
                command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
                command.Parameters.Add("@BlockType", SqlDbType.Int).Value = (int)BlockType.List;

                if (isLargeTextValue)
                {
                    command.Parameters.Add("@ObjectData", SqlDbType.NVarChar, -1).Value = DBNull.Value;
                    command.Parameters.Add("@CompressedObjectData", SqlDbType.VarBinary, -1).Value = compressedData;
                }
                else
                {
                    command.Parameters.Add("@ObjectData", SqlDbType.NVarChar, -1).Value = header;
                    command.Parameters.Add("@CompressedObjectData", SqlDbType.VarBinary, -1).Value = DBNull.Value;
                }

                return (long)await command.ExecuteScalarAsync().ConfigureAwait(false);
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
        using (var connection = await CreateNewConnectionAsync(createRequest.TaskId).ConfigureAwait(false))
        {
            var command = connection.CreateCommand();
            var transaction = connection.BeginTransaction();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandTimeout = ConnectionStore.Instance.GetConnection(createRequest.TaskId).QueryTimeoutSeconds;

            try
            {
                var dt = GenerateDataTable(blockId, createRequest.SerializedValues, createRequest.CompressionThreshold);
                await BulkLoadInTransactionOperationAsync(dt, "Taskling.ListBlockItem", connection, transaction)
                    .ConfigureAwait(false);

                transaction.Commit();
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
            using (var connection = await CreateNewConnectionAsync(taskId).ConfigureAwait(false))
            {
                var command = connection.CreateCommand();
                command.CommandTimeout = ConnectionStore.Instance.GetConnection(taskId).QueryTimeoutSeconds;
                command.CommandText = ObjectBlockQueryBuilder.InsertObjectBlock;
                command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;

                if (isLargeTextValue)
                {
                    command.Parameters.Add("@ObjectData", SqlDbType.NVarChar, -1).Value = DBNull.Value;
                    command.Parameters.Add("@CompressedObjectData", SqlDbType.VarBinary, -1).Value = compressedData;
                }
                else
                {
                    command.Parameters.Add("@ObjectData", SqlDbType.NVarChar, -1).Value = jsonValue;
                    command.Parameters.Add("@CompressedObjectData", SqlDbType.VarBinary, -1).Value = DBNull.Value;
                }

                command.Parameters.Add("@BlockType", SqlDbType.Int).Value = (int)BlockType.Object;
                return (long)await command.ExecuteScalarAsync().ConfigureAwait(false);
            }
        }
        catch (SqlException sqlEx)
        {
            if (TransientErrorDetector.IsTransient(sqlEx))
                throw new TransientException("A transient exception has occurred", sqlEx);

            throw;
        }
    }

    private async Task<IList<ObjectBlock<T>>> FindFailedObjectBlocksAsync<T>(
        FindFailedBlocksRequest failedBlocksRequest, string query)
    {
        var results = new List<ObjectBlock<T>>();
        var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(failedBlocksRequest.TaskId)
            .ConfigureAwait(false);

        try
        {
            using (var connection = await CreateNewConnectionAsync(failedBlocksRequest.TaskId).ConfigureAwait(false))
            {
                var command = connection.CreateCommand();
                command.CommandText = query;
                command.CommandTimeout = ConnectionStore.Instance.GetConnection(failedBlocksRequest.TaskId)
                    .QueryTimeoutSeconds;
                command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinition.TaskDefinitionId;
                command.Parameters.Add("@SearchPeriodBegin", SqlDbType.DateTime).Value =
                    failedBlocksRequest.SearchPeriodBegin;
                command.Parameters.Add("@SearchPeriodEnd", SqlDbType.DateTime).Value =
                    failedBlocksRequest.SearchPeriodEnd;
                command.Parameters.Add("@AttemptLimit", SqlDbType.Int).Value =
                    failedBlocksRequest.RetryLimit + 1; // RetryLimit + 1st attempt
                using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        var blockType = (BlockType)reader.GetInt32("BlockType");
                        if (blockType == failedBlocksRequest.BlockType)
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
        FindBlocksOfTaskRequest blocksOfTaskRequest, string query, ReprocessOption reprocessOption)
    {
        var results = new List<ObjectBlock<T>>();
        var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(blocksOfTaskRequest.TaskId)
            .ConfigureAwait(false);

        try
        {
            using (var connection = await CreateNewConnectionAsync(blocksOfTaskRequest.TaskId).ConfigureAwait(false))
            {
                var command = connection.CreateCommand();
                command.CommandText = query;
                command.CommandTimeout = ConnectionStore.Instance.GetConnection(blocksOfTaskRequest.TaskId)
                    .QueryTimeoutSeconds;
                command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinition.TaskDefinitionId;
                command.Parameters.Add("@ReferenceValue", SqlDbType.NVarChar, 200).Value =
                    blocksOfTaskRequest.ReferenceValueOfTask;

                if (reprocessOption == ReprocessOption.PendingOrFailed)
                {
                    command.Parameters.Add("@NotStarted", SqlDbType.Int).Value = (int)BlockExecutionStatus.NotStarted;
                    command.Parameters.Add("@Started", SqlDbType.Int).Value = (int)BlockExecutionStatus.Started;
                    command.Parameters.Add("@Failed", SqlDbType.Int).Value = (int)BlockExecutionStatus.Failed;
                }

                using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        var blockType = (BlockType)reader.GetInt32("BlockType");
                        if (blockType != blocksOfTaskRequest.BlockType)
                            throw new ExecutionException(
                                "The block with this reference value is of a different BlockType. BlockType resuested: " +
                                blocksOfTaskRequest.BlockType + " BlockType found: " + blockType);

                        var objectBlock = new ObjectBlock<T>();
                        objectBlock.ObjectBlockId = reader.GetInt64("BlockId");
                        objectBlock.Attempt = reader.GetInt32("Attempt");
                        objectBlock.Object =
                            SerializedValueReader.ReadValue<T>(reader, "ObjectData", "CompressedObjectData");

                        results.Add(objectBlock);
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

    #endregion .: Object Blocks :.

    #region .: Force Block Queue :.

    private async Task<IList<ForcedRangeBlockQueueItem>> GetForcedDateRangeBlocksAsync(
        QueuedForcedBlocksRequest queuedForcedBlocksRequest)
    {
        var query = ForcedBlockQueueQueryBuilder.GetDateRangeBlocksQuery();
        return await GetForcedRangeBlocksAsync(queuedForcedBlocksRequest, query).ConfigureAwait(false);
    }

    private async Task<IList<ForcedRangeBlockQueueItem>> GetForcedNumericRangeBlocksAsync(
        QueuedForcedBlocksRequest queuedForcedBlocksRequest)
    {
        var query = ForcedBlockQueueQueryBuilder.GetNumericRangeBlocksQuery();
        return await GetForcedRangeBlocksAsync(queuedForcedBlocksRequest, query).ConfigureAwait(false);
    }

    private async Task<IList<ForcedRangeBlockQueueItem>> GetForcedRangeBlocksAsync(
        QueuedForcedBlocksRequest queuedForcedBlocksRequest, string query)
    {
        var results = new List<ForcedRangeBlockQueueItem>();
        var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(queuedForcedBlocksRequest.TaskId)
            .ConfigureAwait(false);

        try
        {
            using (var connection =
                   await CreateNewConnectionAsync(queuedForcedBlocksRequest.TaskId).ConfigureAwait(false))
            {
                var command = connection.CreateCommand();
                command.CommandText = query;
                command.CommandTimeout = ConnectionStore.Instance.GetConnection(queuedForcedBlocksRequest.TaskId)
                    .QueryTimeoutSeconds;
                command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinition.TaskDefinitionId;
                using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        var blockType = (BlockType)reader.GetInt32("BlockType");
                        if (blockType == queuedForcedBlocksRequest.BlockType)
                        {
                            var blockId = reader.GetInt64("BlockId");
                            var attempt = reader.GetInt32("Attempt");
                            var forceBlockQueueId = 0;

                            long rangeBegin;
                            long rangeEnd;

                            RangeBlock rangeBlock = null;
                            if (queuedForcedBlocksRequest.BlockType == BlockType.DateRange)
                            {
                                rangeBegin = reader.GetDateTime(1).Ticks;
                                rangeEnd = reader.GetDateTime(2).Ticks;
                                rangeBlock = new RangeBlock(blockId, attempt + 1, rangeBegin, rangeEnd,
                                    queuedForcedBlocksRequest.BlockType);
                                forceBlockQueueId = reader.GetInt32(5);
                            }
                            else if (queuedForcedBlocksRequest.BlockType == BlockType.NumericRange)
                            {
                                rangeBegin = reader.GetInt64("FromNumber");
                                rangeEnd = reader.GetInt64("ToNumber");
                                rangeBlock = new RangeBlock(blockId, attempt + 1, rangeBegin, rangeEnd,
                                    queuedForcedBlocksRequest.BlockType);
                                forceBlockQueueId = reader.GetInt32(5);
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
                            throw new ExecutionException(
                                @"The block type of the process does not match the block type of the queued item. 
This could occur if the block type of the process has been changed during a new development. Expected: " +
                                queuedForcedBlocksRequest.BlockType + " but queued block is: " + blockType);
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

    private async Task<IList<ForcedListBlockQueueItem>> GetForcedListBlocksAsync(
        QueuedForcedBlocksRequest queuedForcedBlocksRequest)
    {
        var results = new List<ForcedListBlockQueueItem>();
        var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(queuedForcedBlocksRequest.TaskId)
            .ConfigureAwait(false);

        try
        {
            using (var connection =
                   await CreateNewConnectionAsync(queuedForcedBlocksRequest.TaskId).ConfigureAwait(false))
            {
                var command = connection.CreateCommand();
                command.CommandText = ForcedBlockQueueQueryBuilder.GetListBlocksQuery();
                ;
                command.CommandTimeout = ConnectionStore.Instance.GetConnection(queuedForcedBlocksRequest.TaskId)
                    .QueryTimeoutSeconds;
                command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinition.TaskDefinitionId;
                using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        var blockType = (BlockType)reader.GetInt32("BlockType");
                        if (blockType == queuedForcedBlocksRequest.BlockType)
                        {
                            var blockId = reader.GetInt64("BlockId");
                            var attempt = reader.GetInt32("Attempt");
                            var forceBlockQueueId = reader.GetInt32("ForceBlockQueueId");

                            var listBlock = new ProtoListBlock
                            {
                                ListBlockId = blockId,
                                Attempt = attempt + 1,
                                Header = SerializedValueReader.ReadValueAsString(reader, "ObjectData",
                                    "CompressedObjectData")
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
                            throw new ExecutionException(
                                @"The block type of the process does not match the block type of the queued item. 
This could occur if the block type of the process has been changed during a new development. Expected: " +
                                queuedForcedBlocksRequest.BlockType + " but queued block is: " + blockType);
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

    private async Task<IList<ForcedObjectBlockQueueItem<T>>> GetForcedObjectBlocksAsync<T>(
        QueuedForcedBlocksRequest queuedForcedBlocksRequest)
    {
        var results = new List<ForcedObjectBlockQueueItem<T>>();
        var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(queuedForcedBlocksRequest.TaskId)
            .ConfigureAwait(false);

        try
        {
            using (var connection =
                   await CreateNewConnectionAsync(queuedForcedBlocksRequest.TaskId).ConfigureAwait(false))
            {
                var command = connection.CreateCommand();
                command.CommandText = ForcedBlockQueueQueryBuilder.GetObjectBlocksQuery();
                command.CommandTimeout = ConnectionStore.Instance.GetConnection(queuedForcedBlocksRequest.TaskId)
                    .QueryTimeoutSeconds;
                command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinition.TaskDefinitionId;
                using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        var blockType = (BlockType)reader.GetInt32("BlockType");
                        if (blockType == queuedForcedBlocksRequest.BlockType)
                        {
                            var blockId = reader.GetInt64("BlockId");
                            var attempt = reader.GetInt32("Attempt");
                            var forceBlockQueueId = reader.GetInt32(4);
                            var objectData =
                                SerializedValueReader.ReadValue<T>(reader, "ObjectData", "CompressedObjectData");

                            var objectBlock = new ObjectBlock<T>
                            {
                                ObjectBlockId = blockId,
                                Attempt = attempt + 1,
                                Object = objectData
                            };

                            var queueItem = new ForcedObjectBlockQueueItem<T>
                            {
                                BlockType = queuedForcedBlocksRequest.BlockType,
                                ForcedBlockQueueId = forceBlockQueueId,
                                ObjectBlock = objectBlock
                            };

                            results.Add(queueItem);
                        }
                        else
                        {
                            throw new ExecutionException(
                                @"The block type of the process does not match the block type of the queued item. 
This could occur if the block type of the process has been changed during a new development. Expected: " +
                                queuedForcedBlocksRequest.BlockType + " but queued block is: " + blockType);
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

    private async Task UpdateForcedBlocksAsync(DequeueForcedBlocksRequest dequeueForcedBlocksRequest)
    {
        var blockCount = dequeueForcedBlocksRequest.ForcedBlockQueueIds.Count;

        try
        {
            using (var connection =
                   await CreateNewConnectionAsync(dequeueForcedBlocksRequest.TaskId).ConfigureAwait(false))
            {
                var command = connection.CreateCommand();
                command.CommandText = ForcedBlockQueueQueryBuilder.GetUpdateQuery(blockCount);
                command.CommandTimeout = ConnectionStore.Instance.GetConnection(dequeueForcedBlocksRequest.TaskId)
                    .QueryTimeoutSeconds;

                for (var i = 0; i < blockCount; i++)
                    command.Parameters.Add("@P" + i, SqlDbType.Int).Value =
                        dequeueForcedBlocksRequest.ForcedBlockQueueIds[i];

                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
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

    private DataTable GenerateDataTable(long blockId, List<string> values, int compressionThreshold)
    {
        var dt = GenerateEmptyDataTable();

        foreach (var value in values)
        {
            var dr = dt.NewRow();
            dr["BlockId"] = blockId;

            if (value.Length > compressionThreshold)
            {
                dr["Value"] = DBNull.Value;
                dr["CompressedValue"] = LargeValueCompressor.Zip(value);
            }
            else
            {
                dr["Value"] = value;
                dr["CompressedValue"] = DBNull.Value;
            }

            dr["Status"] = (int)ItemStatus.Pending;
            dr["LastUpdated"] = DateTime.UtcNow;
            dt.Rows.Add(dr);
        }

        return dt;
    }

    private DataTable GenerateEmptyDataTable()
    {
        var dt = new DataTable();
        dt.Columns.Add("BlockId", typeof(long));
        dt.Columns.Add("Value", typeof(string));
        dt.Columns.Add("CompressedValue", typeof(byte[]));
        dt.Columns.Add("Status", typeof(int));
        dt.Columns.Add("LastUpdated", typeof(DateTime));

        return dt;
    }

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
            using (var connection = await CreateNewConnectionAsync(executionCreateRequest.TaskId).ConfigureAwait(false))
            {
                var command = connection.CreateCommand();
                command.CommandTimeout = ConnectionStore.Instance.GetConnection(executionCreateRequest.TaskId)
                    .QueryTimeoutSeconds;
                command.CommandText = RangeBlockQueryBuilder.InsertBlockExecution;
                command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value =
                    executionCreateRequest.TaskExecutionId;
                command.Parameters.Add("@BlockId", SqlDbType.BigInt).Value = executionCreateRequest.BlockId;
                command.Parameters.Add("@Attempt", SqlDbType.Int).Value = executionCreateRequest.Attempt;
                command.Parameters.Add("@Status", SqlDbType.Int).Value = (int)BlockExecutionStatus.NotStarted;
                blockExecutionId = (long)await command.ExecuteScalarAsync().ConfigureAwait(false);
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

    private List<string> Serialize<T>(List<T> values)
    {
        var jsonValues = new List<string>();
        foreach (var value in values) jsonValues.Add(JsonGenericSerializer.Serialize(value));

        return jsonValues;
    }

    #endregion .: Private Methods :.
}