using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Taskling.Exceptions;
using Taskling.Extensions;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.CriticalSections;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.AncilliaryServices;
using Taskling.SqlServer.Models;
using Taskling.Tasks;
using TransactionScopeRetryHelper;
using TaskDefinition = Taskling.SqlServer.Models.TaskDefinition;

namespace Taskling.SqlServer.Tokens.CriticalSections;

public class CsQueueSerializer
{
    public static List<CriticalSectionQueueItem> Deserialize(string? json)
    {
        if (string.IsNullOrWhiteSpace(json)) return new List<CriticalSectionQueueItem>();
        return JsonConvert.DeserializeObject<List<CriticalSectionQueueItem>>(json);
    }
    public static string Serialize(IEnumerable<CriticalSectionQueueItem> items)
    {
        return JsonConvert.SerializeObject(items);
    }
}
public class CriticalSectionRepository : DbOperationsService, ICriticalSectionRepository
{
    private readonly ICommonTokenRepository _commonTokenRepository;
    private readonly ILogger<CriticalSectionRepository> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ITaskRepository _taskRepository;

    public CriticalSectionRepository(ITaskRepository taskRepository, TasklingOptions tasklingOptions,
        ICommonTokenRepository commonTokenRepository, IConnectionStore connectionStore,
        ILogger<CriticalSectionRepository> logger,
        IDbContextFactoryEx dbContextFactoryEx, ILoggerFactory loggerFactory) : base(connectionStore,
        dbContextFactoryEx, loggerFactory.CreateLogger<DbOperationsService>())
    {
        _taskRepository = taskRepository;
        _commonTokenRepository = commonTokenRepository;
        _logger = logger;
        _loggerFactory = loggerFactory;
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
        _logger.LogDebug($"TaskDeathMode={startRequest.TaskDeathMode}");
        if (startRequest.TaskDeathMode == TaskDeathMode.KeepAlive)
        {
            if (!startRequest.KeepAliveDeathThreshold.HasValue)
            {
                _logger.LogWarning("KeepAliveDeathThreshold must be set when using KeepAlive mode");
                throw new ExecutionArgumentsException("KeepAliveDeathThreshold must be set when using KeepAlive mode");
            }
        }
        else if (startRequest.TaskDeathMode == TaskDeathMode.Override)
        {
            if (!startRequest.OverrideThreshold.HasValue)
            {
                _logger.LogWarning("OverrideThreshold must be set when using Override mode");
                throw new ExecutionArgumentsException("OverrideThreshold must be set when using Override mode");
            }
        }
    }

    private async Task<CompleteCriticalSectionResponse> ReturnCriticalSectionTokenAsync(TaskId taskId,
        long taskDefinitionId, long taskExecutionId, CriticalSectionType criticalSectionType)
    {

        return await RetryHelper.WithRetryAsync(async () =>
        {
            var response = new CompleteCriticalSectionResponse();

            using (var dbContext = await GetDbContextAsync(taskId).ConfigureAwait(false))
            {
                var entityEntry = dbContext.TaskDefinitions.Attach(new TaskDefinition
                    { TaskDefinitionId = taskDefinitionId });
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

    private async Task<bool> TryAcquireCriticalSectionAsync(TaskId taskId, long taskDefinitionId, long taskExecutionId,
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
                    if (csState.HasQueuedExecutions())
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

    private async Task AcquireRowLockAsync(long taskDefinitionId, long taskExecutionId,
        TasklingDbContext dbContext)
    {

        await _commonTokenRepository.AcquireRowLockAsync(taskDefinitionId, taskExecutionId, dbContext)
            .ConfigureAwait(false);
    }

    private async Task<CriticalSectionState> GetCriticalSectionStateAsync(long taskDefinitionId,
        CriticalSectionType criticalSectionType, TasklingDbContext dbContext)
    {
        var tmp = dbContext.TaskDefinitions.Where(i => i.TaskDefinitionId == taskDefinitionId);

        QueueItemInfo? tuple = null;
        if (criticalSectionType == CriticalSectionType.User)
        {
            tuple = await tmp.Select(i => new QueueItemInfo
                    { Queue = i.UserCsQueue, Status = i.UserCsStatus, TaskExecutionId = i.UserCsTaskExecutionId })
                .FirstOrDefaultAsync().ConfigureAwait(false);
        }
        else
        {
            tuple = await tmp.Select(i => new QueueItemInfo
                    { Queue = i.ClientCsQueue, Status = i.ClientCsStatus, TaskExecutionId = i.ClientCsTaskExecutionId })
                .FirstOrDefaultAsync();
        }

        if (tuple != null)

        {
            var csState = new CriticalSectionState(_loggerFactory.CreateLogger<CriticalSectionState>());
            csState.IsGranted = tuple.Status == 0;
            csState.GrantedToExecution = tuple.TaskExecutionId;
            csState.SetQueue(tuple.Queue);
            csState.StartTrackingModifications();
            return csState;
        }

        throw new CriticalSectionException("No Task exists with id " + taskDefinitionId);
    }

    private List<long> GetActiveTaskExecutionIds(CriticalSectionState csState)
    {

        var taskExecutionIds = new List<long>();

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
            csState.UpdateQueue(validQueuedExecutions.OrderBy(i=>i.CreatedAt).ToList());
        }
    }

    private bool HasEmptyGranteeValue(CriticalSectionState csState)
    {

        return csState.GrantedToExecution == null || csState.GrantedToExecution == 0;
    }

    private void GrantCriticalSection(CriticalSectionState csState, long taskExecutionId)
    {

        csState.IsGranted = true;
        csState.GrantedToExecution = taskExecutionId;
    }

    private async Task UpdateCriticalSectionStateAsync(long taskDefinitionId, CriticalSectionState csState,
        CriticalSectionType criticalSectionType, TasklingDbContext dbContext)
    {

        var taskDefinition = new TaskDefinition { TaskDefinitionId = taskDefinitionId };
        var entityEntry = dbContext.TaskDefinitions.Attach(taskDefinition);

        if (criticalSectionType == CriticalSectionType.User)
        {
            taskDefinition.UserCsQueue = csState.GetQueueString();
            taskDefinition.UserCsStatus = csState.IsGranted ? 1 : 0;
            taskDefinition.UserCsTaskExecutionId = taskDefinitionId;
            entityEntry.Property(i => i.UserCsQueue).IsModified = true;
            entityEntry.Property(i => i.UserCsStatus).IsModified = true;
            entityEntry.Property(i => i.UserCsTaskExecutionId).IsModified = true;
        }
        else
        {
            taskDefinition.ClientCsQueue = csState.GetQueueString();
            taskDefinition.ClientCsStatus = csState.IsGranted ? 1 : 0;
            taskDefinition.ClientCsTaskExecutionId = taskDefinitionId;
            entityEntry.Property(i => i.ClientCsQueue).IsModified = true;
            entityEntry.Property(i => i.ClientCsStatus).IsModified = true;
            entityEntry.Property(i => i.ClientCsTaskExecutionId).IsModified = true;
        }


        await dbContext.SaveChangesAsync();
    }

    private async Task<List<TaskExecutionState>> GetTaskExecutionStatesAsync(List<long> taskExecutionIds,
        TasklingDbContext dbContext)
    {

        return await _commonTokenRepository.GetTaskExecutionStatesAsync(taskExecutionIds, dbContext)
            .ConfigureAwait(false);
    }

    private bool HasCriticalSectionExpired(TaskExecutionState taskExecutionState)
    {

        return _commonTokenRepository.HasExpired(taskExecutionState);
    }

    private class QueueItemInfo
    {
        public string? Queue { get; set; }
        public int Status { get; set; }
        public long? TaskExecutionId { get; set; }
    }
}