﻿using System;
using System.Threading.Tasks;
using Taskling.Configuration;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.CleanUp;
using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.CleanUp;

 

 
public class CleanUpService : ICleanUpService
{
    private readonly ICleanUpRepository _cleanUpRepository;
    private readonly ITaskExecutionRepository _taskExecutionRepository;


    public CleanUpService (ICleanUpRepository cleanUpRepository,
        ITaskExecutionRepository taskExecutionRepository)
    {
        _cleanUpRepository = cleanUpRepository;
        
        _taskExecutionRepository = taskExecutionRepository;
    }

    public void CleanOldData(string applicationName, string taskName, int taskExecutionId,
        ITaskConfigurationRepository taskConfigurationRepository)
    {

        Task.Run(async () =>
            await StartCleanOldDataAsync(applicationName, taskName, taskExecutionId, taskConfigurationRepository).ConfigureAwait(false));
    }

    private async Task StartCleanOldDataAsync(string applicationName, string taskName, int taskExecutionId, ITaskConfigurationRepository taskConfigurationRepository)
    {
        var checkpoint = new TaskExecutionCheckpointRequest
        {
            TaskExecutionId = taskExecutionId,
            TaskId = new TaskId(applicationName, taskName)
        };

        try
        {
            var configuration = taskConfigurationRepository.GetTaskConfiguration(applicationName, taskName);
            var request = new CleanUpRequest
            {
                TaskId = new TaskId(applicationName, taskName),
                GeneralDateThreshold = DateTime.UtcNow.AddDays(-1 * configuration.KeepGeneralDataForDays),
                ListItemDateThreshold = DateTime.UtcNow.AddDays(-1 * configuration.KeepListItemsForDays),
                TimeSinceLastCleaningThreashold = new TimeSpan(configuration.MinimumCleanUpIntervalHours, 0, 0)
            };
            var cleaned = await _cleanUpRepository.CleanOldDataAsync(request).ConfigureAwait(false);

            if (cleaned)
                checkpoint.Message = "Data clean up performed";
            else
                checkpoint.Message = "Data clean up skipped";
        }
        catch (Exception ex)
        {
            checkpoint.Message = "Failed to clean old data. " + ex;
        }

        await LogCleanupAsync(checkpoint).ConfigureAwait(false);
    }

    private async Task LogCleanupAsync(TaskExecutionCheckpointRequest checkpoint)
    {
        try
        {
            if (_taskExecutionRepository != null)
                await _taskExecutionRepository.CheckpointAsync(checkpoint).ConfigureAwait(false);
        }
        catch (Exception)
        {
        }
    }
}