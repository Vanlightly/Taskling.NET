using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks;
using Taskling.Blocks.Common;
using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.InfrastructureContracts.Blocks
{
    public class LastBlockRequest
    {
        public LastBlockRequest(TaskId taskId,
            BlockType blockType)
        {
            TaskId = taskId;
            BlockType = blockType;
        }

        public TaskId TaskId { get; set; }
        public BlockType BlockType { get; set; }
        public LastBlockOrder LastBlockOrder { get; set; }
    }
}
