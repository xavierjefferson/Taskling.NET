using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Taskling.Blocks.Factories;
using Taskling.CleanUp;
using Taskling.Configuration;
using Taskling.Contexts;
using Taskling.Enums;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.CriticalSections;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.Tasks;

namespace Taskling.EntityFrameworkCore;

public class TasklingClient : ITasklingClient
{
    private readonly IBlockFactory _blockFactory;
    private readonly ICleanUpService _cleanUpService;

    private readonly IConnectionStore _connectionStore;
    private readonly ICriticalSectionRepository _criticalSectionRepository;
    private readonly IListBlockRepository _listBlockRepository;
    private readonly IObjectBlockRepository _objectBlockRepository;
    private readonly IRangeBlockRepository _rangeBlockRepository;
    private readonly IServiceProvider _serviceProvider;
    private readonly ITaskConfigurationRepository _taskConfigurationRepository;

    private readonly ITaskExecutionRepository _taskExecutionRepository;

    public TasklingClient(IServiceProvider serviceProvider, ITaskConfigurationReader taskConfigurationReader)
    {
        _serviceProvider = serviceProvider;
        _taskConfigurationRepository = new TaskConfigurationRepository(taskConfigurationReader,
            serviceProvider.GetRequiredService<ILogger<TaskConfigurationRepository>>(),
            serviceProvider.GetRequiredService<IMemoryCache>(),
            serviceProvider.GetRequiredService<StartupOptions>());
        _connectionStore = serviceProvider.GetRequiredService<IConnectionStore>() ??
                           throw new NullReferenceException(nameof(IConnectionStore));

        _taskExecutionRepository = serviceProvider.GetRequiredService<ITaskExecutionRepository>();
        _criticalSectionRepository = serviceProvider.GetRequiredService<ICriticalSectionRepository>() ??
                                     throw new NullReferenceException(nameof(ICriticalSectionRepository));
        _rangeBlockRepository = serviceProvider.GetRequiredService<IRangeBlockRepository>() ??
                                throw new NullReferenceException(nameof(IRangeBlockRepository));
        _listBlockRepository = serviceProvider.GetRequiredService<IListBlockRepository>() ??
                               throw new NullReferenceException(nameof(IListBlockRepository));
        _objectBlockRepository = serviceProvider.GetRequiredService<IObjectBlockRepository>() ??
                                 throw new NullReferenceException(nameof(IObjectBlockRepository));
        _blockFactory = serviceProvider.GetRequiredService<IBlockFactory>() ??
                        throw new NullReferenceException(nameof(IBlockRepository));
        _cleanUpService = serviceProvider.GetRequiredService<ICleanUpService>() ??
                          throw new NullReferenceException(nameof(ICleanUpService));
    }

    public ITaskExecutionContext CreateTaskExecutionContext(string applicationName, string taskName)
    {
        return CreateTaskExecutionContext(new TaskId(applicationName, taskName));
    }

    public ITaskExecutionContext CreateTaskExecutionContext(TaskId taskId)
    {
        LoadConnectionSettings(taskId);

        var taskExecutionContext = _serviceProvider.GetRequiredService<ITaskExecutionContext>();
        taskExecutionContext.SetOptions(taskId,
            LoadTaskExecutionOptions(taskId), _taskConfigurationRepository);
        return taskExecutionContext;
    }

    private void LoadConnectionSettings(TaskId taskId)
    {
        var taskConfiguration = _taskConfigurationRepository.GetTaskConfiguration(taskId);
        var connectionSettings = new ClientConnectionSettings(taskConfiguration.ConnectionString,
            TimeSpan.FromSeconds(taskConfiguration.DatabaseTimeoutSeconds));

        _connectionStore.SetConnection(taskId, connectionSettings);
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