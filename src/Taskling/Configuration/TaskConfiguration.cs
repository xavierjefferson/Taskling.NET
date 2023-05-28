using System;
using Taskling.InfrastructureContracts;

namespace Taskling.Configuration;

public class TaskConfiguration : ConfigurationOptions
{
    public TaskConfiguration(TaskId taskId, IConfigurationOptions source)
    {
        TaskId = taskId;
        var connectionString =
            source.ConnectionString ??
            throw new ArgumentNullException(nameof(IConfigurationOptions.ConnectionString));
        this.ConnectionString = source.ConnectionString;
        this.CommandTimeoutSeconds = source.CommandTimeoutSeconds;
        this.Enabled = source.Enabled;
        this.ConcurrencyLimit = source.ConcurrencyLimit;
        this.KeepListItemsForDays = source.KeepListItemsForDays;
        this.KeepGeneralDataForDays = source.KeepGeneralDataForDays;
        this.MinimumCleanUpIntervalHours = source.MinimumCleanUpIntervalHours;
        this.UseKeepAliveMode = source.UseKeepAliveMode;
        this.KeepAliveIntervalMinutes = source.KeepAliveIntervalMinutes;
        this.KeepAliveDeathThresholdMinutes = source.KeepAliveDeathThresholdMinutes;
        this.TimePeriodDeathThresholdMinutes = source.TimePeriodDeathThresholdMinutes;
        this.ReprocessFailedTasks = source.ReprocessFailedTasks;
        this.FailedTaskDetectionRangeMinutes = source.FailedTaskDetectionRangeMinutes;
        this.FailedTaskRetryLimit = source.FailedTaskRetryLimit;
        this.ReprocessDeadTasks = source.ReprocessDeadTasks;
        this.DeadTaskDetectionRangeMinutes = source.DeadTaskDetectionRangeMinutes;
        this.DeadTaskRetryLimit = source.DeadTaskRetryLimit;
        this.MaxBlocksToGenerate = source.MaxBlocksToGenerate;
        this.MaxLengthForNonCompressedData = source.MaxLengthForNonCompressedData;
        this.MaxStatusReason = source.MaxStatusReason;
    }

    public TaskId TaskId { get; }
    public DateTime DateLoaded { get; } = DateTime.UtcNow;


}