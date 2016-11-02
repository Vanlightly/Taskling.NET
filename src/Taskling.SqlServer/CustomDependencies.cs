using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.Factories;
using Taskling.CleanUp;
using Taskling.Configuration;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.CleanUp;
using Taskling.InfrastructureContracts.CriticalSections;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.Events;
using Taskling.SqlServer.Tokens;
using Taskling.SqlServer.Tokens.Executions;

namespace Taskling.SqlServer
{
    public class CustomDependencies
    {
        public ITaskRepository TaskRepository { get; set; }
        public ITasklingConfiguration Configuration { get; set; }
        public ITaskExecutionRepository TaskExecutionRepository { get; set; }
        public IExecutionTokenRepository ExecutionTokenRepository { get; set; }
        public ICommonTokenRepository CommonTokenRepository { get; set; }
        public IEventsRepository EventsRepository { get; set; }
        public ICriticalSectionRepository CriticalSectionRepository { get; set; }
        public IBlockFactory BlockFactory { get; set; }
        public IBlockRepository BlockRepository { get; set; }
        public IRangeBlockRepository RangeBlockRepository { get; set; }
        public IListBlockRepository ListBlockRepository { get; set; }
        public IObjectBlockRepository ObjectBlockRepository { get; set; }
        public ICleanUpService CleanUpService { get; set; }
        public ICleanUpRepository CleanUpRepository { get; set; }
    }
}
