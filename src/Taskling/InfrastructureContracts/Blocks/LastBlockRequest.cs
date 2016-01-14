using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks;
using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.InfrastructureContracts.Blocks
{
    public class LastBlockRequest
    {
        public LastBlockRequest(string applicationName,
            string taskName,
            BlockType blockType)
        {
            ApplicationName = applicationName;
            TaskName = taskName;
            BlockType = blockType;
        }

        public string ApplicationName { get; set; }
        public string TaskName { get; set; }
        public BlockType BlockType { get; set; }
    }
}
