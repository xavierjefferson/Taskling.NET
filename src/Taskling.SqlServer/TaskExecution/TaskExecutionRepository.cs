using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Taskling.Events;
using Taskling.Exceptions;
using Taskling.Extensions;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.AncilliaryServices;
using Taskling.SqlServer.Events;
using Taskling.SqlServer.Tokens.Executions;
using Taskling.Tasks;
using TransactionScopeRetryHelper;

namespace Taskling.SqlServer.TaskExecution;

public class TaskExecutionRepository : DbOperationsService, ITaskExecutionRepository
{
    private readonly IEventsRepository _eventsRepository;
    private readonly IExecutionTokenRepository _executionTokenRepository;
    private readonly ILogger<TaskExecutionRepository> _logger;
    private readonly ITaskRepository _taskRepository;

    public TaskExecutionRepository(ITaskRepository taskRepository,
        IExecutionTokenRepository executionTokenRepository,
        IEventsRepository eventsRepository, IConnectionStore connectionStore, IDbContextFactoryEx dbContextFactoryEx,
        ILogger<TaskExecutionRepository> logger, ILoggerFactory loggerFactory) :
        base(connectionStore, dbContextFactoryEx, loggerFactory.CreateLogger<DbOperationsService>())
    {
        _taskRepository = taskRepository;
        _executionTokenRepository = executionTokenRepository;
        _eventsRepository = eventsRepository;
        _logger = logger;
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
    }

    public async Task<TaskExecutionStartResponse> StartAsync(TaskExecutionStartRequest startRequest)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        ValidateStartRequest(startRequest);
        var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(startRequest.TaskId).ConfigureAwait(false);

        if (startRequest.TaskDeathMode == TaskDeathMode.KeepAlive)
            return await StartKeepAliveExecutionAsync(startRequest, taskDefinition.TaskDefinitionId)
                .ConfigureAwait(false);

        if (startRequest.TaskDeathMode == TaskDeathMode.Override)
            return await StartOverrideExecutionAsync(startRequest, taskDefinition.TaskDefinitionId)
                .ConfigureAwait(false);

        throw new ExecutionException("Unsupported TaskDeathMode");
    }

    public async Task<TaskExecutionCompleteResponse> CompleteAsync(TaskExecutionCompleteRequest completeRequest)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await SetCompletedDateOnTaskExecutionAsync(completeRequest.TaskId, completeRequest.TaskExecutionId)
            .ConfigureAwait(false);
        await RegisterEventAsync(completeRequest.TaskId, completeRequest.TaskExecutionId, EventType.End, null)
            .ConfigureAwait(false);
        return await ReturnExecutionTokenAsync(completeRequest).ConfigureAwait(false);
    }

    public async Task CheckpointAsync(TaskExecutionCheckpointRequest taskExecutionRequest)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await RegisterEventAsync(taskExecutionRequest.TaskId, taskExecutionRequest.TaskExecutionId,
            EventType.CheckPoint, taskExecutionRequest.Message).ConfigureAwait(false);
    }

    public async Task ErrorAsync(TaskExecutionErrorRequest taskExecutionErrorRequest)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        if (taskExecutionErrorRequest.TreatTaskAsFailed)
            await SetTaskExecutionAsFailedAsync(taskExecutionErrorRequest.TaskId,
                taskExecutionErrorRequest.TaskExecutionId).ConfigureAwait(false);

        await RegisterEventAsync(taskExecutionErrorRequest.TaskId, taskExecutionErrorRequest.TaskExecutionId,
            EventType.Error, taskExecutionErrorRequest.Error).ConfigureAwait(false);
    }

    public async Task SendKeepAliveAsync(SendKeepAliveRequest sendKeepAliveRequest)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await UpdateTaskExecution(i => i.LastKeepAlive = DateTime.UtcNow, i => i.LastKeepAlive,
            sendKeepAliveRequest.TaskExecutionId, sendKeepAliveRequest.TaskId);
    }

    public async Task<TaskExecutionMetaResponse> GetLastExecutionMetasAsync(
        TaskExecutionMetaRequest taskExecutionMetaRequest)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        return await RetryHelper.WithRetryAsync(async () =>
        {
            var response = new TaskExecutionMetaResponse();
            var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(taskExecutionMetaRequest.TaskId)
                .ConfigureAwait(false);
            using (var dbContext = await GetDbContextAsync(taskExecutionMetaRequest.TaskId))

            {
                var items = await dbContext.TaskExecutions
                    .Where(i => i.TaskDefinitionId == taskDefinition.TaskDefinitionId)
                    .OrderByDescending(i => i.TaskExecutionId)
                    .Take(taskExecutionMetaRequest.ExecutionsToRetrieve)
                    .ToListAsync()
                    .ConfigureAwait(false);


                var now = DateTime.UtcNow;


                foreach (var taskExecution in items)
                {
                    var executionMeta = new TaskExecutionMetaItem();
                    executionMeta.StartedAt = taskExecution.StartedAt;
                    executionMeta.CompletedAt = taskExecution.CompletedAt;
                    if (taskExecution.CompletedAt != null)
                    {
                        var failed = taskExecution.Failed;
                        var blocked = taskExecution.Blocked;

                        if (failed)
                            executionMeta.Status = TaskExecutionStatus.Failed;
                        else if (blocked)
                            executionMeta.Status = TaskExecutionStatus.Blocked;
                        else
                            executionMeta.Status = TaskExecutionStatus.Completed;
                    }
                    else
                    {
                        var taskDeathMode = (TaskDeathMode)taskExecution.TaskDeathMode;
                        if (taskDeathMode == TaskDeathMode.KeepAlive)
                        {
                            var lastKeepAlive = taskExecution.LastKeepAlive;
                            var keepAliveThreshold = taskExecution.KeepAliveDeathThreshold;

                            var timeSinceLastKeepAlive = now - lastKeepAlive;
                            if (timeSinceLastKeepAlive > keepAliveThreshold)
                                executionMeta.Status = TaskExecutionStatus.Dead;
                            else
                                executionMeta.Status = TaskExecutionStatus.InProgress;
                        }
                    }


                    executionMeta.Header = taskExecution.ExecutionHeader;
                    executionMeta.ReferenceValue = taskExecution.ReferenceValue;
                    response.Executions.Add(executionMeta);
                }
            }

            return response;
        });
    }

    private async Task UpdateTaskExecution<TProperty>(Action<Models.TaskExecution> action,
        Expression<Func<Models.TaskExecution, TProperty>> changedPropertyExpression, int taskExecutionId, TaskId taskId)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await RetryHelper.WithRetryAsync(async () =>
        {
            using (var dbContext = await GetDbContextAsync(taskId).ConfigureAwait(false))
            {
                var entry = dbContext.TaskExecutions.Attach(
                    new Models.TaskExecution { TaskExecutionId = taskExecutionId });
                action(entry.Entity);
                entry.Property(changedPropertyExpression).IsModified = true;
                await dbContext.SaveChangesAsync().ConfigureAwait(false);
            }
        });
    }

    private void ValidateStartRequest(TaskExecutionStartRequest startRequest)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        if (startRequest.TaskDeathMode == TaskDeathMode.KeepAlive)
        {
            if (!startRequest.KeepAliveInterval.HasValue)
                throw new ExecutionArgumentsException("KeepAliveInterval must be set when using KeepAlive mode");

            if (!startRequest.KeepAliveDeathThreshold.HasValue)
                throw new ExecutionArgumentsException("KeepAliveDeathThreshold must be set when using KeepAlive mode");
        }
        else if (startRequest.TaskDeathMode == TaskDeathMode.Override)
        {
            if (!startRequest.OverrideThreshold.HasValue)
                throw new ExecutionArgumentsException("OverrideThreshold must be set when using Override mode");
        }
    }

    private async Task<TaskExecutionStartResponse> StartKeepAliveExecutionAsync(TaskExecutionStartRequest startRequest,
        int taskDefinitionId)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var taskExecutionId = await CreateKeepAliveTaskExecutionAsync(startRequest.TaskId,
            taskDefinitionId,
            startRequest.KeepAliveInterval.Value,
            startRequest.KeepAliveDeathThreshold.Value,
            startRequest.ReferenceValue,
            startRequest.FailedTaskRetryLimit,
            startRequest.DeadTaskRetryLimit,
            startRequest.TasklingVersion,
            startRequest.TaskExecutionHeader).ConfigureAwait(false);

        await RegisterEventAsync(startRequest.TaskId, taskExecutionId, EventType.Start, null).ConfigureAwait(false);
        var tokenResponse =
            await TryGetExecutionTokenAsync(startRequest.TaskId, taskDefinitionId, taskExecutionId,
                startRequest.ConcurrencyLimit).ConfigureAwait(false);
        if (tokenResponse.GrantStatus == GrantStatus.Denied)
        {
            await SetBlockedOnTaskExecutionAsync(startRequest.TaskId, taskExecutionId).ConfigureAwait(false);
            if (tokenResponse.Ex == null)
                await RegisterEventAsync(startRequest.TaskId, taskExecutionId, EventType.Blocked, null)
                    .ConfigureAwait(false);
            else
                await RegisterEventAsync(startRequest.TaskId, taskExecutionId, EventType.Blocked,
                    tokenResponse.Ex.ToString()).ConfigureAwait(false);
        }

        return tokenResponse;
    }

    private async Task<TaskExecutionStartResponse> StartOverrideExecutionAsync(TaskExecutionStartRequest startRequest,
        int taskDefinitionId)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var taskExecutionId = await CreateOverrideTaskExecutionAsync(startRequest.TaskId, taskDefinitionId,
            startRequest.OverrideThreshold.Value,
            startRequest.ReferenceValue, startRequest.FailedTaskRetryLimit, startRequest.DeadTaskRetryLimit,
            startRequest.TasklingVersion, startRequest.TaskExecutionHeader).ConfigureAwait(false);
        await RegisterEventAsync(startRequest.TaskId, taskExecutionId, EventType.Start, null).ConfigureAwait(false);

        var tokenResponse =
            await TryGetExecutionTokenAsync(startRequest.TaskId, taskDefinitionId, taskExecutionId,
                startRequest.ConcurrencyLimit).ConfigureAwait(false);

        if (tokenResponse.GrantStatus == GrantStatus.Denied)
        {
            await SetBlockedOnTaskExecutionAsync(startRequest.TaskId, taskExecutionId).ConfigureAwait(false);

            if (tokenResponse.Ex == null)
                await RegisterEventAsync(startRequest.TaskId, taskExecutionId, EventType.Blocked, null)
                    .ConfigureAwait(false);
            else
                await RegisterEventAsync(startRequest.TaskId, taskExecutionId, EventType.Blocked,
                    tokenResponse.Ex.ToString()).ConfigureAwait(false);
        }

        return tokenResponse;
    }

    private async Task<TaskExecutionStartResponse> TryGetExecutionTokenAsync(TaskId taskId, int taskDefinitionId,
        int taskExecutionId, int concurrencyLimit)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var tokenRequest = new TokenRequest
        {
            TaskId = taskId,
            TaskDefinitionId = taskDefinitionId,
            TaskExecutionId = taskExecutionId,
            ConcurrencyLimit = concurrencyLimit
        };

        try
        {
            var tokenResponse = await _executionTokenRepository.TryAcquireExecutionTokenAsync(tokenRequest)
                .ConfigureAwait(false);

            var response = new TaskExecutionStartResponse();
            response.ExecutionTokenId = tokenResponse.ExecutionTokenId;
            response.GrantStatus = tokenResponse.GrantStatus;
            response.StartedAt = tokenResponse.StartedAt;
            response.TaskExecutionId = taskExecutionId;

            return response;
        }
        catch (Exception ex)
        {
            var response = new TaskExecutionStartResponse();
            response.StartedAt = DateTime.UtcNow;
            response.GrantStatus = GrantStatus.Denied;
            response.ExecutionTokenId = Guid.Empty;
            response.TaskExecutionId = taskExecutionId;
            response.Ex = ex;

            return response;
        }
    }

    private async Task<int> CreateKeepAliveTaskExecutionAsync(TaskId taskId, int taskDefinitionId,
        TimeSpan keepAliveInterval, TimeSpan keepAliveDeathThreshold, Guid referenceValue,
        int failedTaskRetryLimit, int deadTaskRetryLimit, string tasklingVersion, string executionHeader)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _logger.Debug("49cb1df8-5df1-4e22-b071-8df2830ad51e");
        using (var dbContext = await GetDbContextAsync(taskId))
        {
            var taskExecution = new Models.TaskExecution
            {
                TaskDefinitionId = taskDefinitionId,
                ServerName = Environment.MachineName,
                TaskDeathMode = (int)TaskDeathMode.KeepAlive,
                KeepAliveInterval = keepAliveInterval,
                KeepAliveDeathThreshold = keepAliveDeathThreshold,
                FailedTaskRetryLimit = failedTaskRetryLimit,
                DeadTaskRetryLimit = deadTaskRetryLimit,
                TasklingVersion = tasklingVersion,
                ExecutionHeader = executionHeader,
                ReferenceValue = referenceValue,
                LastKeepAlive = DateTime.UtcNow,
                StartedAt = DateTime.UtcNow
            };
            await dbContext.TaskExecutions.AddAsync(taskExecution).ConfigureAwait(false);
            await dbContext.SaveChangesAsync().ConfigureAwait(false);
            return taskExecution.TaskExecutionId;
        }
    }

    private async Task<int> CreateOverrideTaskExecutionAsync(TaskId taskId, int taskDefinitionId,
        TimeSpan overrideThreshold, Guid referenceValue,
        int failedTaskRetryLimit, int deadTaskRetryLimit, string tasklingVersion, string executionHeader)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        using (var dbContext = await GetDbContextAsync(taskId))
        {
            var lastKeepAlive = DateTime.UtcNow;
            var taskExecution = new Models.TaskExecution
            {
                TaskDefinitionId = taskDefinitionId,
                StartedAt = lastKeepAlive,
                ServerName = Environment.MachineName,
                LastKeepAlive = lastKeepAlive,
                TaskDeathMode = (int)TaskDeathMode.Override,
                OverrideThreshold = overrideThreshold,
                FailedTaskRetryLimit = failedTaskRetryLimit,
                DeadTaskRetryLimit = deadTaskRetryLimit,
                ReferenceValue = referenceValue,
                Failed = false,
                Blocked = false,
                TasklingVersion = tasklingVersion,
                ExecutionHeader = executionHeader
            };

            await dbContext.TaskExecutions.AddAsync(taskExecution).ConfigureAwait(false);
            await dbContext.SaveChangesAsync().ConfigureAwait(false);
            return taskExecution.TaskExecutionId;
        }
    }

    private async Task<TaskExecutionCompleteResponse> ReturnExecutionTokenAsync(
        TaskExecutionCompleteRequest taskExecutionCompleteRequest)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(taskExecutionCompleteRequest.TaskId)
            .ConfigureAwait(false);

        var tokenRequest = new TokenRequest
        {
            TaskId = taskExecutionCompleteRequest.TaskId,
            TaskDefinitionId = taskDefinition.TaskDefinitionId,
            TaskExecutionId = taskExecutionCompleteRequest.TaskExecutionId
        };

        await _executionTokenRepository
            .ReturnExecutionTokenAsync(tokenRequest, taskExecutionCompleteRequest.ExecutionTokenId)
            .ConfigureAwait(false);

        var response = new TaskExecutionCompleteResponse();
        response.CompletedAt = DateTime.UtcNow;
        return response;
    }

    private async Task SetBlockedOnTaskExecutionAsync(TaskId taskId, int taskExecutionId)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await UpdateTaskExecution(i => i.Blocked = true, i => i.Blocked, taskExecutionId, taskId);
    }

    private async Task SetCompletedDateOnTaskExecutionAsync(TaskId taskId, int taskExecutionId)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await UpdateTaskExecution(i => i.CompletedAt = DateTime.UtcNow, i => i.CompletedAt, taskExecutionId, taskId);
    }

    private async Task SetTaskExecutionAsFailedAsync(TaskId taskId, int taskExecutionId)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await UpdateTaskExecution(i => i.Failed = true, i => i.Failed, taskExecutionId, taskId);
    }

    private async Task RegisterEventAsync(TaskId taskId, int taskExecutionId, EventType eventType, string? message)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        await _eventsRepository.LogEventAsync(taskId, taskExecutionId, eventType, message).ConfigureAwait(false);
    }
}