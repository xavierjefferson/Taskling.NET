using System;
using Newtonsoft.Json;

namespace Taskling;

public class ConfigurationOptions : IConfigurationOptions
{
    protected TimeSpan _deadTaskDetectionRange;
    protected TimeSpan _failedTaskDetectionRange;

    public ConfigurationOptions()
    {
        CommandTimeoutSeconds = 120;
        Enabled = true;
        ConcurrencyLimit = -1;
        KeepListItemsForDays = 14;
        KeepGeneralDataForDays = 40;
        MinimumCleanUpIntervalHours = 1;
        UseKeepAliveMode = true;
        KeepAliveIntervalMinutes = 1;
        KeepAliveDeathThresholdMinutes = 10;
        ReprocessFailedTasks = false;
        ReprocessDeadTasks = false;
        MaxBlocksToGenerate = 10000;
        MaxLengthForNonCompressedData = 2000;
        MaxStatusReason = 1000;
    }

    [JsonIgnore]
    [System.Text.Json.Serialization.JsonIgnore]
    public TimeSpan FailedTaskDetectionRange => _failedTaskDetectionRange;

    [JsonIgnore]
    [System.Text.Json.Serialization.JsonIgnore]
    public TimeSpan DeadTaskDetectionRange => _deadTaskDetectionRange;

    public string ConnectionString { get; set; }
    public int CommandTimeoutSeconds { get; set; }

    /// <summary>
    ///     concurrency
    /// </summary>
    public bool Enabled { get; set; }

    /// <summary>
    ///     concurrency
    /// </summary>
    public int ConcurrencyLimit { get; set; }

    /// <summary>
    ///     clean up
    /// </summary>
    public int KeepListItemsForDays { get; set; }

    /// <summary>
    ///     clean up
    /// </summary>
    public int KeepGeneralDataForDays { get; set; }

    /// <summary>
    ///     clean up
    /// </summary>
    public int MinimumCleanUpIntervalHours { get; set; }

    /// <summary>
    ///     death detection
    /// </summary>
    public bool UseKeepAliveMode { get; set; }

    /// <summary>
    ///     death detection
    /// </summary>
    public double KeepAliveIntervalMinutes { get; set; }

    /// <summary>
    ///     death detection
    /// </summary>
    public double KeepAliveDeathThresholdMinutes { get; set; }

    /// <summary>
    ///     death detection
    /// </summary>
    public double TimePeriodDeathThresholdMinutes { get; set; }

    /// <summary>
    ///     reprocess failed tasks
    /// </summary>
    public bool ReprocessFailedTasks { get; set; }

    /// <summary>
    ///     reprocess failed tasks
    /// </summary>
    public int FailedTaskDetectionRangeMinutes
    {
        get => Convert.ToInt32(_failedTaskDetectionRange.TotalMinutes);
        set => _failedTaskDetectionRange = TimeSpan.FromMinutes(value);
    }

    /// <summary>
    ///     reprocess failed tasks
    /// </summary>
    public int FailedTaskRetryLimit { get; set; }

    /// <summary>
    ///     reprocess dead tasks
    /// </summary>
    public bool ReprocessDeadTasks { get; set; }

    /// <summary>
    ///     reprocess dead tasks
    /// </summary>
    public int DeadTaskDetectionRangeMinutes
    {
        get => Convert.ToInt32(_deadTaskDetectionRange.TotalMinutes);
        set => _deadTaskDetectionRange = TimeSpan.FromMinutes(value);
    }

    /// <summary>
    ///     reprocess dead tasks
    /// </summary>
    public int DeadTaskRetryLimit { get; set; }

    /// <summary>
    ///     blocks
    /// </summary>
    public int MaxBlocksToGenerate { get; set; }

    /// <summary>
    ///     blocks
    /// </summary>
    public int MaxLengthForNonCompressedData { get; set; }

    /// <summary>
    ///     blocks
    /// </summary>
    public int MaxStatusReason { get; set; }

    public int ExpiresInSeconds { get; set; } = 10 * 60;
}