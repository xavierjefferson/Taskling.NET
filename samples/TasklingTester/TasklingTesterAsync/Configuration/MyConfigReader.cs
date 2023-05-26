using Taskling;
using Taskling.Configuration;
using Taskling.InfrastructureContracts;

namespace TasklingTesterAsync.Configuration;

public class MyConfigReader : ITaskConfigurationReader
{
    private readonly Dictionary<string, IConfigurationOptions> _config;

    public MyConfigReader()
    {
        _config = new Dictionary<string, IConfigurationOptions>();
        var configurationOptions = new ConfigurationOptions
        {
            ConnectionString = "Server=(local);Database=MyAppDb;Trusted_Connection=True;",
            DatabaseTimeoutSeconds = 120,
            Enabled = true,
            ConcurrencyLimit = 1,
            KeepListItemsForDays = 2,
            KeepGeneralDataForDays = 40,
            MinimumCleanUpIntervalHours = 1,
            UseKeepAliveMode = true,
            KeepAliveIntervalMinutes = 1,
            KeepAliveDeathThresholdMinutes = 10,
            TimePeriodDeathThresholdMinutes = 0,
            ReprocessFailedTasks = true,
            FailedTaskDetectionRangeMinutes = 600,
            FailedTaskRetryLimit = 3,
            ReprocessDeadTasks = true,
            DeadTaskDetectionRangeMinutes = 600,
            DeadTaskRetryLimit = 3,
            MaxBlocksToGenerate = 2000
        };
        _config.Add("MyApplication::MyDateBasedBatchJob", configurationOptions);
        _config.Add("MyApplication::MyNumericBasedBatchJob", configurationOptions);
        _config.Add("MyApplication::MyListBasedBatchJob", configurationOptions);
    }

    IConfigurationOptions ITaskConfigurationReader.GetTaskConfiguration(TaskId taskId)
    {
        var key = taskId.GetUniqueKey();
        return _config[key];
    }
}