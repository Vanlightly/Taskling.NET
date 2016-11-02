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
using Taskling.InfrastructureContracts.CriticalSections;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.Tasks;

namespace Taskling
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

        public TasklingClient(ITasklingConfiguration configuration,
            ITaskExecutionRepository taskExecutionRepository,
            ICriticalSectionRepository criticalSectionRepository,
            IBlockFactory blockFactory,
            IRangeBlockRepository rangeBlockRepository,
            IListBlockRepository listBlockRepository,
            IObjectBlockRepository objectBlockRepository,
            ICleanUpService cleanUpService)
        {
            _taskExecutionRepository = taskExecutionRepository;
            _criticalSectionRepository = criticalSectionRepository;
            _blockFactory = blockFactory;
            _rangeBlockRepository = rangeBlockRepository;
            _listBlockRepository = listBlockRepository;
            _objectBlockRepository = objectBlockRepository;
            _cleanUpService = cleanUpService;
            _configuration = configuration;
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
