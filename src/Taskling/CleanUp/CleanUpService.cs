using System;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Taskling.Configuration;
using Taskling.Extensions;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.CleanUp;
using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.CleanUp;

public class CleanUpService : ICleanUpService
{
    private readonly ICleanUpRepository _cleanUpRepository;
    private readonly ILogger<CleanUpService> _logger;
    private readonly ITaskExecutionRepository _taskExecutionRepository;


    public CleanUpService(ICleanUpRepository cleanUpRepository,
        ITaskExecutionRepository taskExecutionRepository, ILogger<CleanUpService> logger)
    {
        _logger = logger;
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _cleanUpRepository = cleanUpRepository;

        _taskExecutionRepository = taskExecutionRepository;
    }

    public void CleanOldData(TaskId taskId, long taskExecutionId,
        ITaskConfigurationRepository taskConfigurationRepository)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        Task.Run(async () =>
            await StartCleanOldDataAsync(taskId, taskExecutionId, taskConfigurationRepository)
                .ConfigureAwait(false));
    }

    private async Task StartCleanOldDataAsync(TaskId taskId, long taskExecutionId,
        ITaskConfigurationRepository taskConfigurationRepository)
    {
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        var checkpoint = new TaskExecutionCheckpointRequest(taskId)
        {
            TaskExecutionId = taskExecutionId
        };

        try
        {
            var configuration = taskConfigurationRepository.GetTaskConfiguration(taskId);
            var request = new CleanUpRequest(taskId)
            {
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
        _logger.LogDebug(Constants.GetEnteredMessage(MethodBase.GetCurrentMethod()));
        _logger.Debug("683af1f0-a933-4134-896f-b087f700c148");
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