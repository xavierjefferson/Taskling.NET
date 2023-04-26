using System.Data;
using System.Data.SqlClient;
using Taskling.Blocks.Common;
using Taskling.Blocks.ObjectBlocks;
using Taskling.Blocks.RangeBlocks;
using Taskling.Exceptions;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.Blocks.ListBlocks;
using Taskling.SqlServer.AncilliaryServices;
using Taskling.SqlServer.Blocks.Serialization;
using Taskling.Tasks;

namespace Taskling.SqlServer.Blocks;

public partial class BlockRepository
{
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
}