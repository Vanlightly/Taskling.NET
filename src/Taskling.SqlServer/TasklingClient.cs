using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.Factories;
using Taskling.CleanUp;
using Taskling.Configuration;
using Taskling.Contexts;
using Taskling.ExecutionContext;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.CleanUp;
using Taskling.InfrastructureContracts.CriticalSections;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.Blocks;
using Taskling.SqlServer.Events;
using Taskling.SqlServer.TaskExecution;
using Taskling.SqlServer.Tasks;
using Taskling.SqlServer.Tokens;
using Taskling.SqlServer.Tokens.CriticalSections;
using Taskling.SqlServer.Tokens.Executions;
using Taskling.Tasks;

namespace Taskling.SqlServer
{
    public class TasklingClient : ITasklingClient
    {
        private readonly ITaskExecutionRepository _taskExecutionRepository;
        private readonly ICriticalSectionRepository _criticalSectionRepository;
        private readonly IBlockFactory _blockFactory;
        private readonly IRangeBlockRepository _rangeBlockRepository;
        private readonly IListBlockRepository _listBlockRepository;
        private readonly IObjectBlockRepository _objectBlockRepository;
        private readonly ICleanUpService _cleanUpService;
        private readonly ITasklingConfiguration _configuration;

        public TasklingClient(IConfigurationReader configurationReader,
            ITaskRepository taskRepository = null,
            ITasklingConfiguration configuration = null,
            ITaskExecutionRepository taskExecutionRepository = null,
            IExecutionTokenRepository executionTokenRepository = null,
            ICommonTokenRepository commonTokenRepository = null,
            IEventsRepository eventsRepository = null,
            ICriticalSectionRepository criticalSectionRepository = null,
            IBlockFactory blockFactory = null,
            IBlockRepository blockRepository = null,
            IRangeBlockRepository rangeBlockRepository = null,
            IListBlockRepository listBlockRepository = null,
            IObjectBlockRepository objectBlockRepository = null,
            ICleanUpService cleanUpService = null,
            ICleanUpRepository cleanUpRepository = null)
        {
            if (taskRepository == null)
                taskRepository = new TaskRepository();

            if (configuration == null)
                _configuration = new TasklingConfiguration(configurationReader);

            if (commonTokenRepository == null)
                commonTokenRepository = new CommonTokenRepository();

            if (executionTokenRepository == null)
                executionTokenRepository = new ExecutionTokenRepository(commonTokenRepository);

            if (eventsRepository == null)
                eventsRepository = new EventsRepository();

            if (taskExecutionRepository != null)
                _taskExecutionRepository = taskExecutionRepository;
            else
                _taskExecutionRepository = new TaskExecutionRepository(taskRepository, executionTokenRepository, eventsRepository);

            if (criticalSectionRepository != null)
                _criticalSectionRepository = criticalSectionRepository;
            else
                _criticalSectionRepository = new CriticalSectionRepository(taskRepository, commonTokenRepository);

            if (blockRepository == null)
                blockRepository = new BlockRepository(taskRepository);

            if (rangeBlockRepository != null)
                _rangeBlockRepository = rangeBlockRepository;
            else
                _rangeBlockRepository = new RangeBlockRepository(taskRepository);

            if (listBlockRepository != null)
                _listBlockRepository = listBlockRepository;
            else
                _listBlockRepository = new ListBlockRepository(taskRepository);

            if (objectBlockRepository != null)
                _objectBlockRepository = objectBlockRepository;
            else
                _objectBlockRepository = new ObjectBlockRepository(taskRepository);

            if (blockFactory != null)
                _blockFactory = blockFactory;
            else
                _blockFactory = new BlockFactory(blockRepository, _rangeBlockRepository, _listBlockRepository, _objectBlockRepository, _taskExecutionRepository);

            if (cleanUpRepository == null)
                cleanUpRepository = new CleanUpRepository(taskRepository);

            if (cleanUpService != null)
                _cleanUpService = cleanUpService;
            else
                _cleanUpService = new CleanUpService(_configuration, cleanUpRepository, taskExecutionRepository);
        }

        public TasklingClient(IConfigurationReader configurationReader,
            CustomDependencies customDependencies)
        {
            if (customDependencies.TaskRepository == null)
                customDependencies.TaskRepository = new TaskRepository();

            if (customDependencies.Configuration == null)
                _configuration = new TasklingConfiguration(configurationReader);

            if (customDependencies.CommonTokenRepository == null)
                customDependencies.CommonTokenRepository = new CommonTokenRepository();

            if (customDependencies.ExecutionTokenRepository == null)
                customDependencies.ExecutionTokenRepository = new ExecutionTokenRepository(customDependencies.CommonTokenRepository);

            if (customDependencies.EventsRepository == null)
                customDependencies.EventsRepository = new EventsRepository();

            if (customDependencies.TaskExecutionRepository != null)
                _taskExecutionRepository = customDependencies.TaskExecutionRepository;
            else
                _taskExecutionRepository = new TaskExecutionRepository(customDependencies.TaskRepository, customDependencies.ExecutionTokenRepository, customDependencies.EventsRepository);

            if (customDependencies.CriticalSectionRepository != null)
                _criticalSectionRepository = customDependencies.CriticalSectionRepository;
            else
                _criticalSectionRepository = new CriticalSectionRepository(customDependencies.TaskRepository, customDependencies.CommonTokenRepository);

            if (customDependencies.BlockRepository == null)
                customDependencies.BlockRepository = new BlockRepository(customDependencies.TaskRepository);

            if (customDependencies.RangeBlockRepository != null)
                _rangeBlockRepository = customDependencies.RangeBlockRepository;
            else
                _rangeBlockRepository = new RangeBlockRepository(customDependencies.TaskRepository);

            if (customDependencies.ListBlockRepository != null)
                _listBlockRepository = customDependencies.ListBlockRepository;
            else
                _listBlockRepository = new ListBlockRepository(customDependencies.TaskRepository);

            if (customDependencies.ObjectBlockRepository != null)
                _objectBlockRepository = customDependencies.ObjectBlockRepository;
            else
                _objectBlockRepository = new ObjectBlockRepository(customDependencies.TaskRepository);

            if (customDependencies.BlockFactory != null)
                _blockFactory = customDependencies.BlockFactory;
            else
                _blockFactory = new BlockFactory(customDependencies.BlockRepository, _rangeBlockRepository, _listBlockRepository, _objectBlockRepository, _taskExecutionRepository);

            if (customDependencies.CleanUpRepository == null)
                customDependencies.CleanUpRepository = new CleanUpRepository(customDependencies.TaskRepository);

            if (customDependencies.CleanUpService != null)
                _cleanUpService = customDependencies.CleanUpService;
            else
                _cleanUpService = new CleanUpService(_configuration, customDependencies.CleanUpRepository, customDependencies.TaskExecutionRepository);
        }

        public ITaskExecutionContext CreateTaskExecutionContext(string applicationName,
            string taskName)
        {
            LoadConnectionSettings(applicationName, taskName);

            return new TaskExecutionContext(_configuration,
                _taskExecutionRepository,
                _criticalSectionRepository,
                _blockFactory,
                _rangeBlockRepository,
                _listBlockRepository,
                _objectBlockRepository,
                _cleanUpService,
                applicationName,
                taskName,
                LoadTaskExecutionOptions(applicationName, taskName));
        }

        private void LoadConnectionSettings(string applicationName, string taskName)
        {
            var taskConfiguration = _configuration.GetTaskConfiguration(applicationName, taskName);
            var connectionSettings = new ClientConnectionSettings(taskConfiguration.DatabaseConnectionString,
                new TimeSpan(0, 0, taskConfiguration.DatabaseTimeoutSeconds));

            ConnectionStore.Instance.SetConnection(new TaskId(applicationName, taskName), connectionSettings);
        }

        private TaskExecutionOptions LoadTaskExecutionOptions(string applicationName, string taskName)
        {
            var taskConfiguration = _configuration.GetTaskConfiguration(applicationName, taskName);

            var executionOptions = new TaskExecutionOptions();
            executionOptions.TaskDeathMode = taskConfiguration.UsesKeepAliveMode ? TaskDeathMode.KeepAlive : TaskDeathMode.Override;
            executionOptions.KeepAliveDeathThreshold = new TimeSpan((long)(taskConfiguration.KeepAliveDeathThresholdMinutes * TimeSpan.TicksPerMinute));
            executionOptions.KeepAliveInterval = new TimeSpan((long)(taskConfiguration.KeepAliveIntervalMinutes * TimeSpan.TicksPerMinute));
            executionOptions.OverrideThreshold = new TimeSpan((long)(taskConfiguration.TimePeriodDeathThresholdMinutes * TimeSpan.TicksPerMinute));
            executionOptions.ConcurrencyLimit = taskConfiguration.ConcurrencyLimit;
            executionOptions.Enabled = taskConfiguration.Enabled;

            return executionOptions;
        }
    }
}
