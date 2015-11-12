using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Client;
using Taskling.ExecutionContext;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.Configuration;
using Taskling.SqlServer.CriticalSections;
using Taskling.SqlServer.TaskExecution;

namespace Taskling.SqlServer
{
    public class SqlServerTasklingClient : ITasklingClient
    {
        private readonly SqlServerClientConnectionSettings _clientConnectionSettings;
        private readonly ITaskExecutionService _taskExecutionService;
        private readonly ICriticalSectionService _criticalSectionService;

        public SqlServerTasklingClient(SqlServerClientConnectionSettings clientConnectionSettings,
            ITaskExecutionService taskExecutionService = null,
            ICriticalSectionService criticalSectionService = null)
        {
            _clientConnectionSettings = clientConnectionSettings;

            if(taskExecutionService == null)
                _taskExecutionService = new TaskExecutionService(_clientConnectionSettings);

            if(criticalSectionService == null)
                _criticalSectionService = new CriticalSectionService(_clientConnectionSettings);
        }

        public ITaskExecutionContext CreateTaskExecutionContext(string applicationName, string taskName, TaskExecutionOptions taskExecutionOptions)
        {
            return new TaskExecutionContext(_taskExecutionService, _criticalSectionService, applicationName, taskName, taskExecutionOptions);
        }
    }
}
