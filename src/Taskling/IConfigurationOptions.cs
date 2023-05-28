namespace Taskling;

public interface IConfigurationOptions
{
    string ConnectionString { get; }
    int CommandTimeoutSeconds { get; }
    bool Enabled { get; }
    int ConcurrencyLimit { get; }
    int KeepListItemsForDays { get; }
    int KeepGeneralDataForDays { get; }
    int MinimumCleanUpIntervalHours { get; }
    bool UseKeepAliveMode { get; }
    double KeepAliveIntervalMinutes { get; }
    double KeepAliveDeathThresholdMinutes { get; }
    double TimePeriodDeathThresholdMinutes { get; }
    bool ReprocessFailedTasks { get; }
    int FailedTaskDetectionRangeMinutes { get; }
    int FailedTaskRetryLimit { get; }
    bool ReprocessDeadTasks { get; }
    int DeadTaskDetectionRangeMinutes { get; }
    int DeadTaskRetryLimit { get; }
    int MaxBlocksToGenerate { get; }
    int MaxLengthForNonCompressedData { get; }
    int MaxStatusReason { get; }
}