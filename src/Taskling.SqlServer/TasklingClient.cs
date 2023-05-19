using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Taskling.Blocks.Factories;
using Taskling.CleanUp;
using Taskling.Configuration;
using Taskling.Contexts;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.CriticalSections;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.Tasks;

namespace Taskling.SqlServer;

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
    // public TasklingClient(IConfigurationReader configurationReader,
    //    ITaskRepository taskRepository = null,
    //    ITasklingConfiguration configuration = null,
    //    ITaskExecutionRepository taskExecutionRepository = null,
    //    IExecutionTokenRepository executionTokenRepository = null,
    //    ICommonTokenRepository commonTokenRepository = null,
    //    IEventsRepository eventsRepository = null,
    //    ICriticalSectionRepository criticalSectionRepository = null,
    //    IBlockFactory blockFactory = null,
    //    IBlockRepository blockRepository = null,
    //    IRangeBlockRepository rangeBlockRepository = null,
    //    IListBlockRepository listBlockRepository = null,
    //    IObjectBlockRepository objectBlockRepository = null,
    //    ICleanUpService cleanUpService = null,
    //    ICleanUpRepository cleanUpRepository = null, ITasklingConfigurationFactory tasklingConfigurationFactory = null)
    //{
    //    _tasklingConfigurationFactory = tasklingConfigurationFactory;
    //    if (taskRepository == null)
    //        taskRepository = new TaskRepository();


    //    if (commonTokenRepository == null)
    //        commonTokenRepository = new CommonTokenRepository();

    //    if (executionTokenRepository == null)
    //        executionTokenRepository = new ExecutionTokenRepository(commonTokenRepository);

    //    if (eventsRepository == null)
    //        eventsRepository = new EventsRepository();

    //    if (taskExecutionRepository != null)
    //        _taskExecutionRepository = taskExecutionRepository;
    //    else
    //        _taskExecutionRepository =
    //            new TaskExecutionRepository(taskRepository, executionTokenRepository, eventsRepository);

    //    if (criticalSectionRepository != null)
    //        _criticalSectionRepository = criticalSectionRepository;
    //    else
    //        _criticalSectionRepository = new CriticalSectionRepository(taskRepository, commonTokenRepository);

    //    if (blockRepository == null)
    //        blockRepository = new BlockRepository(taskRepository);

    //    if (rangeBlockRepository != null)
    //        _rangeBlockRepository = rangeBlockRepository;
    //    else
    //        _rangeBlockRepository = new RangeBlockRepository(taskRepository);

    //    if (listBlockRepository != null)
    //        _listBlockRepository = listBlockRepository;
    //    else
    //        _listBlockRepository = new ListBlockRepository(taskRepository);

    //    if (objectBlockRepository != null)
    //        _objectBlockRepository = objectBlockRepository;
    //    else
    //        _objectBlockRepository = new ObjectBlockRepository(taskRepository);

    //    if (blockFactory != null)
    //        _blockFactory = blockFactory;
    //    else
    //        _blockFactory = new BlockFactory(blockRepository, _rangeBlockRepository, _listBlockRepository,
    //            _objectBlockRepository, _taskExecutionRepository);

    //    if (cleanUpRepository == null)
    //        cleanUpRepository = new CleanUpRepository(taskRepository);

    //    if (tasklingConfigurationFactory == null) tasklingConfigurationFactory = new TasklingConfigurationFactory();

    //    if (tasklingConfigurationFactory.Configuration == null)
    //    {
    //        tasklingConfigurationFactory.Configuration = new TasklingConfiguration(configurationReader);
    //    }

    //    if (cleanUpService != null)
    //        _cleanUpService = cleanUpService;
    //    else
    //        _cleanUpService =
    //            new CleanUpService(tasklingConfigurationFactory, cleanUpRepository, taskExecutionRepository);
    //}

    public TasklingClient(IServiceProvider serviceProvider, IConfigurationReader configurationReader)
    {
        _serviceProvider = serviceProvider;
        _taskConfigurationRepository = new TaskConfigurationRepository(configurationReader,
            serviceProvider.GetRequiredService<ILogger<TaskConfigurationRepository>>());
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

    //public TasklingClient(IConfigurationReader configurationReader,
    //    CustomDependencies customDependencies)
    //{
    //    if (customDependencies.TaskRepository == null)
    //        customDependencies.TaskRepository = new TaskRepository();

    //    if (customDependencies.Configuration == null)
    //        _configuration = new TasklingConfiguration(configurationReader);

    //    if (customDependencies.CommonTokenRepository == null)
    //        customDependencies.CommonTokenRepository = new CommonTokenRepository();

    //    if (customDependencies.ExecutionTokenRepository == null)
    //        customDependencies.ExecutionTokenRepository =
    //            new ExecutionTokenRepository(customDependencies.CommonTokenRepository);

    //    if (customDependencies.EventsRepository == null)
    //        customDependencies.EventsRepository = new EventsRepository();

    //    if (customDependencies.TaskExecutionRepository != null)
    //        _taskExecutionRepository = customDependencies.TaskExecutionRepository;
    //    else
    //        _taskExecutionRepository = new TaskExecutionRepository(customDependencies.TaskRepository,
    //            customDependencies.ExecutionTokenRepository, customDependencies.EventsRepository);

    //    if (customDependencies.CriticalSectionRepository != null)
    //        _criticalSectionRepository = customDependencies.CriticalSectionRepository;
    //    else
    //        _criticalSectionRepository = new CriticalSectionRepository(customDependencies.TaskRepository,
    //            customDependencies.CommonTokenRepository);

    //    if (customDependencies.BlockRepository == null)
    //        customDependencies.BlockRepository = new BlockRepository(customDependencies.TaskRepository);

    //    if (customDependencies.RangeBlockRepository != null)
    //        _rangeBlockRepository = customDependencies.RangeBlockRepository;
    //    else
    //        _rangeBlockRepository = new RangeBlockRepository(customDependencies.TaskRepository);

    //    if (customDependencies.ListBlockRepository != null)
    //        _listBlockRepository = customDependencies.ListBlockRepository;
    //    else
    //        _listBlockRepository = new ListBlockRepository(customDependencies.TaskRepository);

    //    if (customDependencies.ObjectBlockRepository != null)
    //        _objectBlockRepository = customDependencies.ObjectBlockRepository;
    //    else
    //        _objectBlockRepository = new ObjectBlockRepository(customDependencies.TaskRepository);

    //    if (customDependencies.BlockFactory != null)
    //        _blockFactory = customDependencies.BlockFactory;
    //    else
    //        _blockFactory = new BlockFactory(customDependencies.BlockRepository, _rangeBlockRepository,
    //            _listBlockRepository, _objectBlockRepository, _taskExecutionRepository);

    //    if (customDependencies.CleanUpRepository == null)
    //        customDependencies.CleanUpRepository = new CleanUpRepository(customDependencies.TaskRepository);

    //    if (customDependencies.CleanUpService != null)
    //        _cleanUpService = customDependencies.CleanUpService;
    //    else
    //        _cleanUpService = new CleanUpService(_configuration, customDependencies.CleanUpRepository,
    //            customDependencies.TaskExecutionRepository);
    //}
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
        var connectionSettings = new ClientConnectionSettings(taskConfiguration.DatabaseConnectionString,
            TimeSpan.FromSeconds(taskConfiguration.DatabaseTimeoutSeconds));

        _connectionStore.SetConnection(taskId, connectionSettings);
    }


    private TaskExecutionOptions LoadTaskExecutionOptions(TaskId taskId)
    {
        var taskConfiguration = _taskConfigurationRepository.GetTaskConfiguration(taskId);

        var executionOptions = new TaskExecutionOptions();
        executionOptions.TaskDeathMode =
            taskConfiguration.UsesKeepAliveMode ? TaskDeathMode.KeepAlive : TaskDeathMode.Override;
        executionOptions.KeepAliveDeathThreshold =
            TimeSpan.FromMinutes(taskConfiguration.KeepAliveDeathThresholdMinutes);
        executionOptions.KeepAliveInterval = TimeSpan.FromMinutes(taskConfiguration.KeepAliveIntervalMinutes);
        executionOptions.OverrideThreshold = TimeSpan.FromMinutes(taskConfiguration.TimePeriodDeathThresholdMinutes);
        executionOptions.ConcurrencyLimit = taskConfiguration.ConcurrencyLimit;
        executionOptions.Enabled = taskConfiguration.Enabled;

        return executionOptions;
    }
}