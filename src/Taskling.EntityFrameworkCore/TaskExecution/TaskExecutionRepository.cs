﻿using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Taskling.EntityFrameworkCore.AncilliaryServices;
using Taskling.EntityFrameworkCore.Events;
using Taskling.EntityFrameworkCore.Tokens.Executions;
using Taskling.Enums;
using Taskling.Exceptions;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.EntityFrameworkCore.TaskExecution;

public class TaskExecutionRepository : DbOperationsService, ITaskExecutionRepository
{
    private readonly IEventsRepository _eventsRepository;
    private readonly IExecutionTokenRepository _executionTokenRepository;
    private readonly ILogger<TaskExecutionRepository> _logger;
    private readonly ITaskRepository _taskRepository;

    public TaskExecutionRepository(ITaskRepository taskRepository,
        IExecutionTokenRepository executionTokenRepository,
        IEventsRepository eventsRepository,  IDbContextFactoryEx dbContextFactoryEx,
        ILogger<TaskExecutionRepository> logger, ILoggerFactory loggerFactory) :
        base(dbContextFactoryEx, loggerFactory.CreateLogger<DbOperationsService>())
    {
        _taskRepository = taskRepository;
        _executionTokenRepository = executionTokenRepository;
        _eventsRepository = eventsRepository;
        _logger = logger;
    }

    public async Task<TaskExecutionStartResponse> StartAsync(TaskExecutionStartRequest startRequest)
    {
        ValidateStartRequest(startRequest);
        var taskDefinition = await _taskRepository.EnsureTaskDefinitionAsync(startRequest.TaskId).ConfigureAwait(false);

        if (startRequest.TaskDeathMode == TaskDeathModeEnum.KeepAlive)
            return await StartKeepAliveExecutionAsync(startRequest, taskDefinition.TaskDefinitionId)
                .ConfigureAwait(false);

        if (startRequest.TaskDeathMode == TaskDeathModeEnum.Override)
            return await StartOverrideExecutionAsync(startRequest, taskDefinition.TaskDefinitionId)
                .ConfigureAwait(false);

        throw new ExecutionException("Unsupported TaskDeathMode");
    }

    public async Task<TaskExecutionCompleteResponse> CompleteAsync(TaskExecutionCompleteRequest completeRequest)
    {
        await SetCompletedDateOnTaskExecutionAsync(completeRequest.TaskId, completeRequest.TaskExecutionId)
            .ConfigureAwait(false);
        await RegisterEventAsync(completeRequest.TaskId, completeRequest.TaskExecutionId, EventTypeEnum.End, null)
            .ConfigureAwait(false);
        return await ReturnExecutionTokenAsync(completeRequest).ConfigureAwait(false);
    }

    public async Task CheckpointAsync(TaskExecutionCheckpointRequest taskExecutionRequest)
    {
        await RegisterEventAsync(taskExecutionRequest.TaskId, taskExecutionRequest.TaskExecutionId,
            EventTypeEnum.CheckPoint, taskExecutionRequest.Message).ConfigureAwait(false);
    }

    public async Task ErrorAsync(TaskExecutionErrorRequest taskExecutionErrorRequest)
    {
        if (taskExecutionErrorRequest.TreatTaskAsFailed)
            await SetTaskExecutionAsFailedAsync(taskExecutionErrorRequest.TaskId,
                taskExecutionErrorRequest.TaskExecutionId).ConfigureAwait(false);

        await RegisterEventAsync(taskExecutionErrorRequest.TaskId, taskExecutionErrorRequest.TaskExecutionId,
            EventTypeEnum.Error, taskExecutionErrorRequest.Error).ConfigureAwait(false);
    }

    public async Task SendKeepAliveAsync(SendKeepAliveRequest sendKeepAliveRequest)
    {
        await UpdateTaskExecution(i => i.LastKeepAlive = DateTime.UtcNow, i => i.LastKeepAlive,
            sendKeepAliveRequest.TaskExecutionId, sendKeepAliveRequest.TaskId);
    }

    public async Task<TaskExecutionMetaResponse> GetLastExecutionMetasAsync(
        TaskExecutionMetaRequest taskExecutionMetaRequest)
    {
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
                            executionMeta.Status = TaskExecutionStatusEnum.Failed;
                        else if (blocked)
                            executionMeta.Status = TaskExecutionStatusEnum.Blocked;
                        else
                            executionMeta.Status = TaskExecutionStatusEnum.Completed;
                    }
                    else
                    {
                        var taskDeathMode = (TaskDeathModeEnum)taskExecution.TaskDeathMode;
                        if (taskDeathMode == TaskDeathModeEnum.KeepAlive)
                        {
                            var lastKeepAlive = taskExecution.LastKeepAlive;
                            var keepAliveThreshold = taskExecution.KeepAliveDeathThreshold;

                            var timeSinceLastKeepAlive = now - lastKeepAlive;
                            if (timeSinceLastKeepAlive > keepAliveThreshold)
                                executionMeta.Status = TaskExecutionStatusEnum.Dead;
                            else
                                executionMeta.Status = TaskExecutionStatusEnum.InProgress;
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
        Expression<Func<Models.TaskExecution, TProperty>> changedPropertyExpression, long taskExecutionId,
        TaskId taskId)
    {
        await RetryHelper.WithRetryAsync(async () =>
        {
            using (var dbContext = await GetDbContextAsync(taskId).ConfigureAwait(false))
            {
                var entityEntry = dbContext.TaskExecutions.Attach(
                    new Models.TaskExecution { TaskExecutionId = taskExecutionId });
                action(entityEntry.Entity);
                entityEntry.Property(changedPropertyExpression).IsModified = true;
                await dbContext.SaveChangesAsync().ConfigureAwait(false);
            }
        });
    }

    private void ValidateStartRequest(TaskExecutionStartRequest startRequest)
    {
        if (startRequest.TaskDeathMode == TaskDeathModeEnum.KeepAlive)
        {
            if (!startRequest.KeepAliveInterval.HasValue)
                throw new ExecutionArgumentsException("KeepAliveInterval must be set when using KeepAlive mode");

            if (!startRequest.KeepAliveDeathThreshold.HasValue)
                throw new ExecutionArgumentsException("KeepAliveDeathThreshold must be set when using KeepAlive mode");
        }
        else if (startRequest.TaskDeathMode == TaskDeathModeEnum.Override)
        {
            if (!startRequest.OverrideThreshold.HasValue)
                throw new ExecutionArgumentsException("OverrideThreshold must be set when using Override mode");
        }
    }

    private async Task<TaskExecutionStartResponse> StartKeepAliveExecutionAsync(TaskExecutionStartRequest startRequest,
        long taskDefinitionId)
    {
        var taskExecutionId = await CreateKeepAliveTaskExecutionAsync(startRequest.TaskId,
            taskDefinitionId,
            startRequest.KeepAliveInterval.Value,
            startRequest.KeepAliveDeathThreshold.Value,
            startRequest.ReferenceValue,
            startRequest.FailedTaskRetryLimit,
            startRequest.DeadTaskRetryLimit,
            startRequest.TasklingVersion,
            startRequest.TaskExecutionHeader).ConfigureAwait(false);

        await RegisterEventAsync(startRequest.TaskId, taskExecutionId, EventTypeEnum.Start, null).ConfigureAwait(false);
        var tokenResponse =
            await TryGetExecutionTokenAsync(startRequest.TaskId, taskDefinitionId, taskExecutionId,
                startRequest.ConcurrencyLimit).ConfigureAwait(false);
        if (tokenResponse.GrantStatus == GrantStatusEnum.Denied)
        {
            await SetBlockedOnTaskExecutionAsync(startRequest.TaskId, taskExecutionId).ConfigureAwait(false);
            if (tokenResponse.Exception == null)
                await RegisterEventAsync(startRequest.TaskId, taskExecutionId, EventTypeEnum.Blocked, null)
                    .ConfigureAwait(false);
            else
                await RegisterEventAsync(startRequest.TaskId, taskExecutionId, EventTypeEnum.Blocked,
                    tokenResponse.Exception.ToString()).ConfigureAwait(false);
        }

        return tokenResponse;
    }

    private async Task<TaskExecutionStartResponse> StartOverrideExecutionAsync(TaskExecutionStartRequest startRequest,
        long taskDefinitionId)
    {
        var taskExecutionId = await CreateOverrideTaskExecutionAsync(startRequest.TaskId, taskDefinitionId,
            startRequest.OverrideThreshold.Value,
            startRequest.ReferenceValue, startRequest.FailedTaskRetryLimit, startRequest.DeadTaskRetryLimit,
            startRequest.TasklingVersion, startRequest.TaskExecutionHeader).ConfigureAwait(false);
        await RegisterEventAsync(startRequest.TaskId, taskExecutionId, EventTypeEnum.Start, null).ConfigureAwait(false);

        var tokenResponse =
            await TryGetExecutionTokenAsync(startRequest.TaskId, taskDefinitionId, taskExecutionId,
                startRequest.ConcurrencyLimit).ConfigureAwait(false);

        if (tokenResponse.GrantStatus == GrantStatusEnum.Denied)
        {
            await SetBlockedOnTaskExecutionAsync(startRequest.TaskId, taskExecutionId).ConfigureAwait(false);

            if (tokenResponse.Exception == null)
                await RegisterEventAsync(startRequest.TaskId, taskExecutionId, EventTypeEnum.Blocked, null)
                    .ConfigureAwait(false);
            else
                await RegisterEventAsync(startRequest.TaskId, taskExecutionId, EventTypeEnum.Blocked,
                    tokenResponse.Exception.ToString()).ConfigureAwait(false);
        }

        return tokenResponse;
    }

    private async Task<TaskExecutionStartResponse> TryGetExecutionTokenAsync(TaskId taskId, long taskDefinitionId,
        long taskExecutionId, int concurrencyLimit)
    {
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
            response.GrantStatus = GrantStatusEnum.Denied;
            response.ExecutionTokenId = Guid.Empty;
            response.TaskExecutionId = taskExecutionId;
            response.Exception = ex;

            return response;
        }
    }

    private async Task<long> CreateKeepAliveTaskExecutionAsync(TaskId taskId, long taskDefinitionId,
        TimeSpan keepAliveInterval, TimeSpan keepAliveDeathThreshold, Guid referenceValue,
        int failedTaskRetryLimit, int deadTaskRetryLimit, string tasklingVersion, string? executionHeader)
    {
        using (var dbContext = await GetDbContextAsync(taskId))
        {
            var taskExecution = new Models.TaskExecution
            {
                TaskDefinitionId = taskDefinitionId,
                ServerName = Environment.MachineName,
                TaskDeathMode = (int)TaskDeathModeEnum.KeepAlive,
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

    private async Task<long> CreateOverrideTaskExecutionAsync(TaskId taskId, long taskDefinitionId,
        TimeSpan overrideThreshold, Guid referenceValue,
        int failedTaskRetryLimit, int deadTaskRetryLimit, string tasklingVersion, string? executionHeader)
    {
        using (var dbContext = await GetDbContextAsync(taskId))
        {
            var lastKeepAlive = DateTime.UtcNow;
            var taskExecution = new Models.TaskExecution
            {
                TaskDefinitionId = taskDefinitionId,
                StartedAt = lastKeepAlive,
                ServerName = Environment.MachineName,
                LastKeepAlive = lastKeepAlive,
                TaskDeathMode = (int)TaskDeathModeEnum.Override,
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

    private async Task SetBlockedOnTaskExecutionAsync(TaskId taskId, long taskExecutionId)
    {
        await UpdateTaskExecution(i => i.Blocked = true, i => i.Blocked, taskExecutionId, taskId);
    }

    private async Task SetCompletedDateOnTaskExecutionAsync(TaskId taskId, long taskExecutionId)
    {
        await UpdateTaskExecution(i => i.CompletedAt = DateTime.UtcNow, i => i.CompletedAt, taskExecutionId, taskId);
    }

    private async Task SetTaskExecutionAsFailedAsync(TaskId taskId, long taskExecutionId)
    {
        await UpdateTaskExecution(i => i.Failed = true, i => i.Failed, taskExecutionId, taskId);
    }

    private async Task RegisterEventAsync(TaskId taskId, long taskExecutionId, EventTypeEnum eventType, string? message)
    {
        await _eventsRepository.LogEventAsync(taskId, taskExecutionId, eventType, message).ConfigureAwait(false);
    }
}