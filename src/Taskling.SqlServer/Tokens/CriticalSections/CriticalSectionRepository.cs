using Microsoft.EntityFrameworkCore;
using Taskling.Blocks.Factories;
using Taskling.Exceptions;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.CriticalSections;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.AncilliaryServices;
using Taskling.SqlServer.Models;
using Taskling.Tasks;
using TransactionScopeRetryHelper;

namespace Taskling.SqlServer.Tokens.CriticalSections;

public class CriticalSectionRepository : DbOperationsService, ICriticalSectionRepository
{
    private readonly ICommonTokenRepository _commonTokenRepository;
    private readonly ITaskRepository _taskRepository;

    public CriticalSectionRepository(ITaskRepository taskRepository, TasklingOptions tasklingOptions,
        ICommonTokenRepository commonTokenRepository, IConnectionStore connectionStore,
        IDbContextFactoryEx dbContextFactoryEx) : base(connectionStore, dbContextFactoryEx)
    {
        _taskRepository = taskRepository;
        _commonTokenRepository = commonTokenRepository;
    }

    public async Task<StartCriticalSectionResponse> StartAsync(StartCriticalSectionRequest startRequest)
    {
        ValidateStartRequest(startRequest);
        var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(startRequest.TaskId).ConfigureAwait(false);
        var granted = await TryAcquireCriticalSectionAsync(startRequest.TaskId, taskDefinition.TaskDefinitionId,
            startRequest.TaskExecutionId, startRequest.Type).ConfigureAwait(false);

        return new StartCriticalSectionResponse
        {
            GrantStatus = granted ? GrantStatus.Granted : GrantStatus.Denied
        };
    }

    public async Task<CompleteCriticalSectionResponse> CompleteAsync(CompleteCriticalSectionRequest completeRequest)
    {
        var taskDefinition =
            await _taskRepository.EnsureTaskDefinitionAsync(completeRequest.TaskId).ConfigureAwait(false);
        return await ReturnCriticalSectionTokenAsync(completeRequest.TaskId, taskDefinition.TaskDefinitionId,
            completeRequest.TaskExecutionId, completeRequest.Type).ConfigureAwait(false);
    }

    private void ValidateStartRequest(StartCriticalSectionRequest startRequest)
    {
        if (startRequest.TaskDeathMode == TaskDeathMode.KeepAlive)
        {
            if (!startRequest.KeepAliveDeathThreshold.HasValue)
                throw new ExecutionArgumentsException("KeepAliveDeathThreshold must be set when using KeepAlive mode");
        }
        else if (startRequest.TaskDeathMode == TaskDeathMode.Override)
        {
            if (!startRequest.OverrideThreshold.HasValue)
                throw new ExecutionArgumentsException("OverrideThreshold must be set when using Override mode");
        }
    }

    private async Task<CompleteCriticalSectionResponse> ReturnCriticalSectionTokenAsync(TaskId taskId,
        int taskDefinitionId, int taskExecutionId, CriticalSectionType criticalSectionType)
    {
        return await RetryHelper.WithRetryAsync(async () =>
        {
            var response = new CompleteCriticalSectionResponse();

            using (var dbContext = await GetDbContextAsync(taskId).ConfigureAwait(false))
            {
                //var exampleEntity = dbContext.TaskDefinitions.Attach(new TaskDefinition { TaskDefinitionId = taskDefinitionId });
                //if (criticalSectionType == CriticalSectionType.User)
                //{
                //    exampleEntity.Entity.UserCsStatus = 1;
                //    exampleEntity.Property(i => i.UserCsStatus).IsModified = true;
                //    taskDefinition.UserCsStatus = 1;
                //}
                //else
                //{
                //    exampleEntity.Entity.ClientCsStatus = 1;
                //    exampleEntity.Property(i => i.ClientCsStatus).IsModified = true;

                //}


                //dbContext.TaskDefinitions.Update(taskDefinition);
                //await dbContext.SaveChangesAsync().ConfigureAwait(false);
                //exampleEntity.Entity.HoldLockTaskExecutionId = taskExecutionId;
                //exampleEntity.Property(i => i.HoldLockTaskExecutionId).IsModified = true;
                //await dbContext.SaveChangesAsync();
                //exampleEntity.State = EntityState.Detached;

                var entityEntry = dbContext.TaskDefinitions.Attach(new Models.TaskDefinition() { TaskDefinitionId = taskDefinitionId });
                if (criticalSectionType == CriticalSectionType.User)
                {
                    entityEntry.Entity.UserCsStatus = 1;
                    entityEntry.Property(i => i.UserCsStatus).IsModified = true;
                }
                else
                {
                    entityEntry.Entity.ClientCsStatus = 1;
                    entityEntry.Property(i => i.ClientCsStatus).IsModified = true;
                }

                await dbContext.SaveChangesAsync().ConfigureAwait(false);
            }

            return response;
        });
    }

    private async Task<bool> TryAcquireCriticalSectionAsync(TaskId taskId, int taskDefinitionId, int taskExecutionId,
        CriticalSectionType criticalSectionType)
    {
        return await RetryHelper.WithRetryAsync(async () =>
        {
            var granted = false;
            using (var dbContext = await GetDbContextAsync(taskId).ConfigureAwait(false))
            {
                await AcquireRowLockAsync(taskDefinitionId, taskExecutionId, dbContext).ConfigureAwait(false);
                var csState =
                    await GetCriticalSectionStateAsync(taskDefinitionId, criticalSectionType, dbContext)
                        .ConfigureAwait(false);
                await CleanseOfExpiredExecutionsAsync(csState, dbContext).ConfigureAwait(false);

                if (csState.IsGranted)
                {
                    // if the critical section is still granted to another execution after cleansing
                    // then we rejected the request. If the execution is not in the queue then we add it
                    if (!csState.ExistsInQueue(taskExecutionId))
                        csState.AddToQueue(taskExecutionId);

                    granted = false;
                }
                else
                {
                    if (csState.GetQueue().Any())
                    {
                        if (csState.GetFirstExecutionIdInQueue() == taskExecutionId)
                        {
                            GrantCriticalSection(csState, taskExecutionId);
                            csState.RemoveFirstInQueue();
                            granted = true;
                        }
                        else
                        {
                            // not next in queue so cannot be granted the critical section
                            granted = false;
                        }
                    }
                    else
                    {
                        GrantCriticalSection(csState, taskExecutionId);
                        granted = true;
                    }
                }

                if (csState.HasBeenModified)
                    await UpdateCriticalSectionStateAsync(taskDefinitionId, csState, criticalSectionType, dbContext)
                        .ConfigureAwait(false);
            }

            return granted;
        }, 10, 60000);
    }

    private async Task AcquireRowLockAsync(int taskDefinitionId, int taskExecutionId,
        TasklingDbContext dbContext)
    {
        await _commonTokenRepository.AcquireRowLockAsync(taskDefinitionId, taskExecutionId, dbContext)
            .ConfigureAwait(false);
    }

    private async Task<CriticalSectionState> GetCriticalSectionStateAsync(int taskDefinitionId,
        CriticalSectionType criticalSectionType, TasklingDbContext dbContext)
    {
        var tmp = dbContext.TaskDefinitions.Where(i => i.TaskDefinitionId == taskDefinitionId);

        if (criticalSectionType == CriticalSectionType.User)
        {
            var tuple = await tmp.Select(i => new { i.UserCsQueue, i.UserCsStatus, i.UserCsTaskExecutionId })
                .FirstOrDefaultAsync().ConfigureAwait(false);
            if (tuple != null)

            {
                var csState = new CriticalSectionState();
                csState.IsGranted = tuple.UserCsStatus == 0;
                csState.GrantedToExecution = tuple.UserCsTaskExecutionId;
                csState.SetQueue(tuple.UserCsQueue);
                return csState;
            }
        }
        else
        {
            var tuple = await tmp.Select(i => new { i.ClientCsQueue, i.ClientCsStatus, i.ClientCsTaskExecutionId })
                .FirstOrDefaultAsync();
            if (tuple != null)

            {
                var csState = new CriticalSectionState();
                csState.IsGranted = tuple.ClientCsStatus == 0;
                csState.GrantedToExecution = tuple.ClientCsTaskExecutionId;
                csState.SetQueue(tuple.ClientCsQueue);
                return csState;
            }
        }


        throw new CriticalSectionException("No Task exists with id " + taskDefinitionId);
    }

    private List<int> GetActiveTaskExecutionIds(CriticalSectionState csState)
    {
        var taskExecutionIds = new List<int>();

        if (!HasEmptyGranteeValue(csState))
            taskExecutionIds.Add(csState.GrantedToExecution.Value);

        if (csState.HasQueuedExecutions())
            taskExecutionIds.AddRange(csState.GetQueue().Select(x => x.TaskExecutionId));

        return taskExecutionIds;
    }

    private async Task CleanseOfExpiredExecutionsAsync(CriticalSectionState csState,
        TasklingDbContext dbContext)
    {
        var csQueue = csState.GetQueue();
        var activeExecutionIds = GetActiveTaskExecutionIds(csState);
        if (activeExecutionIds.Any())
        {
            var taskExecutionStates =
                await GetTaskExecutionStatesAsync(activeExecutionIds, dbContext).ConfigureAwait(false);

            CleanseCurrentGranteeIfExpired(csState, taskExecutionStates);
            CleanseQueueOfExpiredExecutions(csState, taskExecutionStates, csQueue);
        }
    }

    private void CleanseCurrentGranteeIfExpired(CriticalSectionState csState,
        List<TaskExecutionState> taskExecutionStates)
    {
        if (!HasEmptyGranteeValue(csState) && csState.IsGranted)
        {
            var csStateOfGranted = taskExecutionStates.First(x => x.TaskExecutionId == csState.GrantedToExecution);
            if (HasCriticalSectionExpired(csStateOfGranted)) csState.IsGranted = false;
        }
    }

    private void CleanseQueueOfExpiredExecutions(CriticalSectionState csState,
        List<TaskExecutionState> taskExecutionStates, List<CriticalSectionQueueItem> csQueue)
    {
        var validQueuedExecutions = (from tes in taskExecutionStates
            join q in csQueue on tes.TaskExecutionId equals q.TaskExecutionId
            where HasCriticalSectionExpired(tes) == false
            select q).ToList();

        if (validQueuedExecutions.Count != csQueue.Count)
        {
            var updatedQueue = new List<CriticalSectionQueueItem>();
            var newQueueIndex = 1;
            foreach (var validQueuedExecution in validQueuedExecutions.OrderBy(x => x.Index))
                updatedQueue.Add(new CriticalSectionQueueItem(newQueueIndex, validQueuedExecution.TaskExecutionId));

            csState.UpdateQueue(updatedQueue);
        }
    }

    private bool HasEmptyGranteeValue(CriticalSectionState csState)
    {
        return csState.GrantedToExecution == null || csState.GrantedToExecution == 0;
    }

    private void GrantCriticalSection(CriticalSectionState csState, int taskExecutionId)
    {
        csState.IsGranted = true;
        csState.GrantedToExecution = taskExecutionId;
    }

    private async Task UpdateCriticalSectionStateAsync(int taskDefinitionId, CriticalSectionState csState,
        CriticalSectionType criticalSectionType, TasklingDbContext dbContext)
    {
        var taskDefinition =
            await dbContext.TaskDefinitions.FirstOrDefaultAsync(i => i.TaskDefinitionId == taskDefinitionId)
                .ConfigureAwait(false);
        if (taskDefinition != null)
        {
            if (criticalSectionType == CriticalSectionType.User)
            {
                taskDefinition.UserCsQueue = csState.GetQueueString();
                taskDefinition.UserCsStatus = csState.IsGranted ? 1 : 0;
                taskDefinition.UserCsTaskExecutionId = taskDefinitionId;
            }
            else
            {
                taskDefinition.ClientCsQueue = csState.GetQueueString();
                taskDefinition.ClientCsStatus = csState.IsGranted ? 1 : 0;
                taskDefinition.ClientCsTaskExecutionId = taskDefinitionId;
            }

            dbContext.TaskDefinitions.Update(taskDefinition);
            await dbContext.SaveChangesAsync();
        }
    }

    private async Task<List<TaskExecutionState>> GetTaskExecutionStatesAsync(List<int> taskExecutionIds,
        TasklingDbContext dbContext)
    {
        return await _commonTokenRepository.GetTaskExecutionStatesAsync(taskExecutionIds, dbContext)
            .ConfigureAwait(false);
    }

    private bool HasCriticalSectionExpired(TaskExecutionState taskExecutionState)
    {
        return _commonTokenRepository.HasExpired(taskExecutionState);
    }
}