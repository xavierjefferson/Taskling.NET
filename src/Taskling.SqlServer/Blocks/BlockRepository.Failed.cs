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

namespace Taskling.SqlServer.Blocks
{
    public partial class BlockRepository
    {
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

        public async Task<IList<ObjectBlock<T>>> FindFailedObjectBlocksAsync<T>(FindFailedBlocksRequest failedBlocksRequest)
        {
            if (failedBlocksRequest.BlockType == BlockType.Object)
            {
                var query = GetFindFailedObjectBlocksQuery(failedBlocksRequest.BlockCountLimit);
                return await FindFailedObjectBlocksAsync<T>(failedBlocksRequest, query).ConfigureAwait(false);
            }

            throw new NotSupportedException(UnexpectedBlockTypeMessage);
        }
        public async Task<IList<ProtoListBlock>> FindFailedListBlocksAsync(FindFailedBlocksRequest failedBlocksRequest)
        {
            if (failedBlocksRequest.BlockType == BlockType.List)
            {
                var query = GetFindFailedListBlocksQuery(failedBlocksRequest.BlockCountLimit);
                return await FindFailedListBlocksAsync(failedBlocksRequest, query).ConfigureAwait(false);
            }

            throw new NotSupportedException(UnexpectedBlockTypeMessage);
        }
        private const string FindFailedBlocksQuery = @"
WITH OrderedBlocks As (
	SELECT ROW_NUMBER() OVER (PARTITION BY BE.BlockId ORDER BY BE.BlockExecutionId DESC) AS RowNo
			,BE.[BlockExecutionId]
	FROM [Taskling].[BlockExecution] BE WITH(NOLOCK)
	JOIN [Taskling].[TaskExecution] TE ON BE.TaskExecutionId = TE.TaskExecutionId
	WHERE TE.TaskDefinitionId = @TaskDefinitionId
	AND TE.StartedAt >= @SearchPeriodBegin
    AND TE.StartedAt < @SearchPeriodEnd
)

SELECT TOP {0} B.[BlockId]
    {1}
    ,BE.Attempt
    ,B.BlockType
    ,TE.ReferenceValue
    ,B.ObjectData
    ,B.CompressedObjectData
FROM [Taskling].[Block] B WITH(NOLOCK)
JOIN [Taskling].[BlockExecution] BE WITH(NOLOCK) ON B.BlockId = BE.BlockId
JOIN [Taskling].[TaskExecution] TE ON BE.TaskExecutionId = TE.TaskExecutionId
JOIN OrderedBlocks OB ON BE.BlockExecutionId = OB.BlockExecutionId
WHERE B.TaskDefinitionId = @TaskDefinitionId
AND B.IsPhantom = 0
AND BE.BlockExecutionStatus = 4
AND BE.Attempt < @AttemptLimit
AND OB.RowNo = 1
ORDER BY B.CreatedDate ASC";

        public static string GetFindFailedDateRangeBlocksQuery(int top)
        {
            return string.Format(FindFailedBlocksQuery, top, ",B.FromDate,B.ToDate");
        }

        public static string GetFindFailedNumericRangeBlocksQuery(int top)
        {
            return string.Format(FindFailedBlocksQuery, top, ",B.FromNumber,B.ToNumber");
        }

        public static string GetFindFailedListBlocksQuery(int top)
        {
            return string.Format(FindFailedBlocksQuery, top, "");
        }

        public static string GetFindFailedObjectBlocksQuery(int top)
        {
            return string.Format(FindFailedBlocksQuery, top, ",B.ObjectData");
        }
        public async Task<IList<RangeBlock>> FindFailedRangeBlocksAsync(FindFailedBlocksRequest failedBlocksRequest)
        {
            var query = string.Empty;
            switch (failedBlocksRequest.BlockType)
            {
                case BlockType.DateRange:
                    query = GetFindFailedDateRangeBlocksQuery(failedBlocksRequest.BlockCountLimit);
                    break;
                case BlockType.NumericRange:
                    query = GetFindFailedNumericRangeBlocksQuery(failedBlocksRequest
                        .BlockCountLimit);
                    break;
                default:
                    throw new NotSupportedException("This range type is not supported");
            }

            return await FindFailedDateRangeBlocksAsync(failedBlocksRequest, query).ConfigureAwait(false);
        }

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

    }
}
