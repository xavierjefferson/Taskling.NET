using System.Data;
using System.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Taskling.Blocks.Common;
using Taskling.Blocks.ListBlocks;
using Taskling.Exceptions;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.Blocks.ListBlocks;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.AncilliaryServices;
using Taskling.SqlServer.Blocks.Serialization;
using System;
using System.Linq;
using System.Linq.Expressions;
using System.Collections.Generic;
using Taskling.SqlServer.Models;
 
namespace Taskling.SqlServer.Blocks;

public class ListBlockRepository : DbOperationsService, IListBlockRepository
{
    private readonly ITaskRepository _taskRepository;

    public ListBlockRepository(ITaskRepository taskRepository)
    {
        _taskRepository = taskRepository;
    }

    public async Task ChangeStatusAsync(BlockExecutionChangeStatusRequest changeStatusRequest)
    {
        try
        {
            using (var dbContext = await GetDbContextAsync(changeStatusRequest.TaskId))
            {
                var blockExecution = await dbContext.BlockExecutions.FirstOrDefaultAsync(i =>
                    i.BlockExecutionId == changeStatusRequest.BlockExecutionId).ConfigureAwait(false);
                if (blockExecution != null)
                {


                    blockExecution.BlockExecutionStatus = (int)changeStatusRequest.BlockExecutionStatus;
                    switch (changeStatusRequest.BlockExecutionStatus)
                    {
                        case BlockExecutionStatus.Completed:
                        case BlockExecutionStatus.Failed:
                            blockExecution.CompletedAt = DateTime.UtcNow;

                            break;
                        default:
                            blockExecution.StartedAt = DateTime.UtcNow;
                            break;
                    }

                    dbContext.BlockExecutions.Update(blockExecution);
                    await dbContext.SaveChangesAsync().ConfigureAwait(false);

                }
            }
        }
        catch (SqlException sqlEx)
        {
            if (TransientErrorDetector.IsTransient(sqlEx))
                throw new TransientException("A transient exception has occurred", sqlEx);

            throw;
        }
    }

    public async Task<IList<ProtoListBlockItem>> GetListBlockItemsAsync(TaskId taskId, long listBlockId)
    {
        var results = new List<ProtoListBlockItem>();

        try
        {
            using (var dbContext = await GetDbContextAsync(taskId))
            {
                var items = await dbContext.ListBlockItems.Where(i => i.BlockId == listBlockId).ToListAsync()
                    .ConfigureAwait(false);


                foreach (var item in items)
                {
                    var listBlock = new ProtoListBlockItem
                    {
                        ListBlockItemId = item.ListBlockItemId,
                        Value = SerializedValueReader.ReadValueAsString(item, i => i.Value, i => i.CompressedValue),
                        Status = (ItemStatus)item.Status,
                        LastUpdated = item.LastUpdated ?? DateTime.MinValue,
                        StatusReason = item.StatusReason,
                        Step = item.Step
                    };


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

    //private async Task UpdateListBlockItemAsync(List<SingleUpdateRequest> singeUpdateRequest)
    //{
    //    try
    //    {
    //        using (var dbContext = await GetDbContextAsync(singeUpdateRequest.TaskId))
    //        {
    //            var listBlockItems = await dbContext.ListBlockItems.Where(i =>
    //                    i.BlockId == singeUpdateRequest.ListBlockId &&
    //                    i.ListBlockItemId == singeUpdateRequest.ListBlockItem.ListBlockItemId)
    //                .ToListAsync().ConfigureAwait(false);
    //            foreach (var listBlockItem in listBlockItems)
    //            {
    //                listBlockItem.Status = (int)singeUpdateRequest.ListBlockItem.Status;
    //                listBlockItem.StatusReason = singeUpdateRequest.ListBlockItem.StatusReason;
    //                listBlockItem.Step = singeUpdateRequest.ListBlockItem.Step;
    //                dbContext.ListBlockItems.Update(listBlockItem);
    //            }

    //            await dbContext.SaveChangesAsync().ConfigureAwait(false);
    //        }
    //    }
    //    catch (SqlException sqlEx)
    //    {
    //        if (TransientErrorDetector.IsTransient(sqlEx))
    //            throw new TransientException("A transient exception has occurred", sqlEx);

    //        throw;
    //    }
    //}
    public async Task UpdateListBlockItemAsync(SingleUpdateRequest singeUpdateRequest)
    {
        await BatchUpdateListBlockItemsAsync(new BatchUpdateRequest()
        {
            ListBlockId = singeUpdateRequest.ListBlockId,
            ListBlockItems = new List<ProtoListBlockItem>() { singeUpdateRequest.ListBlockItem },
            TaskId = singeUpdateRequest.TaskId
        });


    }

    public async Task BatchUpdateListBlockItemsAsync(BatchUpdateRequest batchUpdateRequest)
    {
        try
        {
            using (var dbContext = await GetDbContextAsync(batchUpdateRequest.TaskId))
            {
                using var transaction = await dbContext.Database.BeginTransactionAsync(IsolationLevel.Serializable).ConfigureAwait(false);


                var listBlockItemIds = batchUpdateRequest.ListBlockItems.Select(i => i.ListBlockItemId).ToList();
                var listBlockItems = await dbContext.ListBlockItems.Where(i => i.BlockId == batchUpdateRequest.ListBlockId)
                    .Where(i => listBlockItemIds.Contains(i.ListBlockItemId)).ToListAsync().ConfigureAwait(false);
                if (!listBlockItems.Any())
                {
                    await transaction.RollbackAsync();
                    return;
                }
                foreach (var item in batchUpdateRequest.ListBlockItems)
                {
                    var listBlockItem = listBlockItems.FirstOrDefault(i =>
                        i.ListBlockItemId == item.ListBlockItemId && i.BlockId == batchUpdateRequest.ListBlockId);
                    if (listBlockItem != null)
                    {
                        var singeUpdateRequest = item;
                        listBlockItem.Status = (int)singeUpdateRequest.Status;
                        listBlockItem.StatusReason = singeUpdateRequest.StatusReason;
                        listBlockItem.Step = singeUpdateRequest.Step;
                        dbContext.ListBlockItems.Update(listBlockItem);
                    }
                }

                await dbContext.SaveChangesAsync().ConfigureAwait(false);
                await transaction.CommitAsync();
            }
        }
        catch (SqlException sqlEx)
        {
            if (TransientErrorDetector.IsTransient(sqlEx))
                throw new TransientException("A transient exception has occurred", sqlEx);

            throw;
        }
        //using (var connection = await CreateNewConnectionAsync(batchUpdateRequest.TaskId).ConfigureAwait(false))
        //{
        //    var command = connection.CreateCommand();
        //    var transaction = connection.BeginTransaction();
        //    command.Connection = connection;
        //    command.Transaction = transaction;
        //    command.CommandTimeout =
        //        ConnectionStore.Instance.GetConnection(batchUpdateRequest.TaskId).QueryTimeoutSeconds;
        //    ;

        //    try
        //    {
        //        var tableName = await CreateTemporaryTableAsync(command).ConfigureAwait(false);
        //        var dt = GenerateDataTable(batchUpdateRequest.ListBlockId, batchUpdateRequest.ListBlockItems);
        //        await BulkLoadInTransactionOperationAsync(dt, tableName, connection, transaction).ConfigureAwait(false);
        //        await PerformBulkUpdateAsync(command, tableName).ConfigureAwait(false);

        //        transaction.Commit();
        //    }
        //    catch (SqlException sqlEx)
        //    {
        //        TryRollBack(transaction, sqlEx);
        //    }
        //    catch (Exception ex)
        //    {
        //        TryRollback(transaction, ex);
        //    }
        //}
    }

    public async Task<ProtoListBlock> GetLastListBlockAsync(LastBlockRequest lastRangeBlockRequest)
    {
        var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(lastRangeBlockRequest.TaskId)
            .ConfigureAwait(false);

        try
        {
            using (var dbContext = await GetDbContextAsync(lastRangeBlockRequest.TaskId))
            {
                var blockData = await dbContext.Blocks.OrderByDescending(i => i.BlockId).Where(i =>
                        i.TaskDefinitionId == taskDefinition.TaskDefinitionId && i.IsPhantom == false)
                    .Select(i => new { i.BlockId, i.ObjectData, i.CompressedObjectData }).FirstOrDefaultAsync()
                    .ConfigureAwait(false);
                if (blockData != null)
                {
                    var listBlock = new ProtoListBlock();
                    listBlock.ListBlockId = blockData.BlockId;
                    listBlock.Items =
                        await GetListBlockItemsAsync(lastRangeBlockRequest.TaskId, listBlock.ListBlockId)
                            .ConfigureAwait(false);
                    listBlock.Header =
                        SerializedValueReader.ReadValueAsString(blockData, i => i.ObjectData,
                            i => i.CompressedObjectData);

                    return listBlock;
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


    

   
 
     
}