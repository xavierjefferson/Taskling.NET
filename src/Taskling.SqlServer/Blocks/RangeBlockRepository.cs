using System.Data;
using System.Data.SqlClient;
using Taskling.Blocks.Common;
using Taskling.Blocks.RangeBlocks;
using Taskling.Exceptions;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.AncilliaryServices;

namespace Taskling.SqlServer.Blocks;

public class RangeBlockRepository : DbOperationsService, IRangeBlockRepository
{
    private readonly ITaskRepository _taskRepository;

    public RangeBlockRepository(ITaskRepository taskRepository)
    {
        _taskRepository = taskRepository;
    }

    public async Task ChangeStatusAsync(BlockExecutionChangeStatusRequest changeStatusRequest)
    {
        switch (changeStatusRequest.BlockType)
        {
            case BlockType.DateRange:
                await ChangeStatusOfDateRangeExecutionAsync(changeStatusRequest).ConfigureAwait(false);
                break;
            case BlockType.NumericRange:
                await ChangeStatusOfNumericRangeExecutionAsync(changeStatusRequest).ConfigureAwait(false);
                break;
            default:
                throw new NotSupportedException("This range type is not supported");
        }
    }

    public async Task<RangeBlock> GetLastRangeBlockAsync(LastBlockRequest lastRangeBlockRequest)
    {
        var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(lastRangeBlockRequest.TaskId)
            .ConfigureAwait(false);

        var query = string.Empty;
        if (lastRangeBlockRequest.BlockType == BlockType.DateRange)
            query = RangeBlockQueryBuilder.GetLastDateRangeBlock(lastRangeBlockRequest.LastBlockOrder);
        else if (lastRangeBlockRequest.BlockType == BlockType.NumericRange)
            query = RangeBlockQueryBuilder.GetLastNumericRangeBlock(lastRangeBlockRequest.LastBlockOrder);
        else
            throw new ArgumentException("An invalid BlockType was supplied: " + lastRangeBlockRequest.BlockType);

        try
        {
            using (var connection = await CreateNewConnectionAsync(lastRangeBlockRequest.TaskId).ConfigureAwait(false))
            {
                var command = connection.CreateCommand();
                command.CommandText = query;
                command.CommandTimeout = ConnectionStore.Instance.GetConnection(lastRangeBlockRequest.TaskId)
                    .QueryTimeoutSeconds;
                command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinition.TaskDefinitionId;
                using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        var rangeBlockId = reader.GetInt64("BlockId");
                        long rangeBegin;
                        long rangeEnd;

                        if (lastRangeBlockRequest.BlockType == BlockType.DateRange)
                        {
                            rangeBegin = reader.GetDateTime(2).Ticks; //reader.GetDateTime("FromDate").Ticks; 
                            rangeEnd = reader.GetDateTime(3).Ticks; //reader.GetDateTime("ToDate").Ticks;
                        }
                        else
                        {
                            rangeBegin = reader.GetInt64("FromNumber");
                            rangeEnd = reader.GetInt64("ToNumber");
                        }

                        return new RangeBlock(rangeBlockId, 0, rangeBegin, rangeEnd, lastRangeBlockRequest.BlockType);
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

        return null;
    }


    private async Task ChangeStatusOfDateRangeExecutionAsync(BlockExecutionChangeStatusRequest changeStatusRequest)
    {
        try
        {
            using (var connection = await CreateNewConnectionAsync(changeStatusRequest.TaskId).ConfigureAwait(false))
            {
                var command = connection.CreateCommand();
                command.CommandTimeout = ConnectionStore.Instance.GetConnection(changeStatusRequest.TaskId)
                    .QueryTimeoutSeconds;
                command.CommandText = GetDateRangeUpdateQuery(changeStatusRequest.BlockExecutionStatus);
                command.Parameters.Add("@BlockExecutionId", SqlDbType.BigInt).Value =
                    changeStatusRequest.BlockExecutionId;
                command.Parameters.Add("@BlockExecutionStatus", SqlDbType.Int).Value =
                    (int)changeStatusRequest.BlockExecutionStatus;
                command.Parameters.Add("@ItemsCount", SqlDbType.Int).Value = changeStatusRequest.ItemsProcessed;

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

    private async Task ChangeStatusOfNumericRangeExecutionAsync(BlockExecutionChangeStatusRequest changeStatusRequest)
    {
        try
        {
            using (var connection = await CreateNewConnectionAsync(changeStatusRequest.TaskId).ConfigureAwait(false))
            {
                var command = connection.CreateCommand();
                command.CommandTimeout = ConnectionStore.Instance.GetConnection(changeStatusRequest.TaskId)
                    .QueryTimeoutSeconds;
                command.CommandText = GetNumericRangeUpdateQuery(changeStatusRequest.BlockExecutionStatus);
                command.Parameters.Add("@BlockExecutionId", SqlDbType.BigInt).Value =
                    changeStatusRequest.BlockExecutionId;
                command.Parameters.Add("@BlockExecutionStatus", SqlDbType.Int).Value =
                    (int)changeStatusRequest.BlockExecutionStatus;
                command.Parameters.Add("@ItemsCount", SqlDbType.Int).Value = changeStatusRequest.ItemsProcessed;
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

    private string GetDateRangeUpdateQuery(BlockExecutionStatus executionStatus)
    {
        if (executionStatus == BlockExecutionStatus.Completed || executionStatus == BlockExecutionStatus.Failed)
            return BlockExecutionQueryBuilder.SetRangeBlockExecutionAsCompleted;

        return BlockExecutionQueryBuilder.SetBlockExecutionStatusToStarted;
    }

    private string GetNumericRangeUpdateQuery(BlockExecutionStatus executionStatus)
    {
        if (executionStatus == BlockExecutionStatus.Completed || executionStatus == BlockExecutionStatus.Failed)
            return BlockExecutionQueryBuilder.SetRangeBlockExecutionAsCompleted;

        return BlockExecutionQueryBuilder.SetBlockExecutionStatusToStarted;
    }
}