using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Taskling.Configuration;
using Taskling.Contexts;
using Taskling.Enums;
using Taskling.InfrastructureContracts;
using Taskling.Tasks;

namespace Taskling.EntityFrameworkCore;

public class TasklingClient : ITasklingClient
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ITaskConfigurationRepository _taskConfigurationRepository;


    public TasklingClient(IServiceProvider serviceProvider, ITaskConfigurationReader taskConfigurationReader)
    {
        _serviceProvider = serviceProvider;
        _taskConfigurationRepository = new TaskConfigurationRepository(taskConfigurationReader,
            serviceProvider.GetRequiredService<ILogger<TaskConfigurationRepository>>(),
            serviceProvider.GetRequiredService<IMemoryCache>(),
            serviceProvider.GetRequiredService<StartupOptions>());
    }

    public ITaskExecutionContext CreateTaskExecutionContext(string applicationName, string taskName)
    {
        return CreateTaskExecutionContext(new TaskId(applicationName, taskName));
    }

    public ITaskExecutionContext CreateTaskExecutionContext(TaskId taskId)
    {
        var taskExecutionContext = _serviceProvider.GetRequiredService<ITaskExecutionContext>();
        taskExecutionContext.SetOptions(taskId,
            LoadTaskExecutionOptions(taskId), _taskConfigurationRepository);
        return taskExecutionContext;
    }

    private TaskExecutionOptions LoadTaskExecutionOptions(TaskId taskId)
    {
        var taskConfiguration = _taskConfigurationRepository.GetTaskConfiguration(taskId);

        var executionOptions = new TaskExecutionOptions();
        executionOptions.TaskDeathMode =
            taskConfiguration.UseKeepAliveMode ? TaskDeathModeEnum.KeepAlive : TaskDeathModeEnum.Override;
        executionOptions.KeepAliveDeathThreshold =
            TimeSpan.FromMinutes(taskConfiguration.KeepAliveDeathThresholdMinutes);
        executionOptions.KeepAliveInterval = TimeSpan.FromMinutes(taskConfiguration.KeepAliveIntervalMinutes);
        executionOptions.OverrideThreshold = TimeSpan.FromMinutes(taskConfiguration.TimePeriodDeathThresholdMinutes);
        executionOptions.ConcurrencyLimit = taskConfiguration.ConcurrencyLimit;
        executionOptions.Enabled = taskConfiguration.Enabled;

        return executionOptions;
    }
}