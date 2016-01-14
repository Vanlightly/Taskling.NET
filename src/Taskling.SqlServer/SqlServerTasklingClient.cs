using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks;
using Taskling.Client;
using Taskling.ExecutionContext;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.Blocks;
using Taskling.SqlServer.Configuration;
using Taskling.SqlServer.CriticalSections;
using Taskling.SqlServer.TaskExecution;
using Taskling.SqlServer.Tasks;

namespace Taskling.SqlServer
{
    public class SqlServerTasklingClient : ITasklingClient
    {
        private readonly SqlServerClientConnectionSettings _clientConnectionSettings;
        private readonly ITaskExecutionService _taskExecutionService;
        private readonly ICriticalSectionService _criticalSectionService;
        private readonly IBlockService _blockService;
        private readonly IRangeBlockService _rangeBlockService;
        private readonly IListBlockService _listBlockService;

        public SqlServerTasklingClient(SqlServerClientConnectionSettings clientConnectionSettings,
            ITaskExecutionService taskExecutionService = null,
            ICriticalSectionService criticalSectionService = null,
            ITaskService taskService = null,
            IBlockService blockService = null,
            IRangeBlockService rangeBlockService = null,
            IListBlockService listBlockService = null)
        {
            _clientConnectionSettings = clientConnectionSettings;

            if(taskService == null)
                taskService = new TaskService(_clientConnectionSettings);

            if (taskExecutionService == null)
                _taskExecutionService = new TaskExecutionService(_clientConnectionSettings, taskService);
            else
                _taskExecutionService = taskExecutionService;

            if (criticalSectionService == null)
                _criticalSectionService = new CriticalSectionService(_clientConnectionSettings, taskService);
            else
                _criticalSectionService = criticalSectionService;

            if (blockService == null)
                _blockService = new BlockService(_clientConnectionSettings, taskService);
            else
                _blockService = blockService;

            if (rangeBlockService == null)
                _rangeBlockService = new RangeBlockService(_clientConnectionSettings, taskService);
            else
                _rangeBlockService = rangeBlockService;

            if (listBlockService == null)
                _listBlockService = new ListBlockService(_clientConnectionSettings, taskService);
            else
                _listBlockService = listBlockService;

        }

        public ITaskExecutionContext CreateTaskExecutionContext(string applicationName, string taskName, TaskExecutionOptions taskExecutionOptions)
        {
            return new TaskExecutionContext(_taskExecutionService, 
                _criticalSectionService, 
                new BlockFactory(_blockService, _rangeBlockService, _listBlockService), 
                _rangeBlockService,
                _listBlockService,
                applicationName, 
                taskName, 
                taskExecutionOptions);
        }
    }
}
