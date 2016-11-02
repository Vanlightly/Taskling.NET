using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks;
using Taskling.Blocks.Common;
using Taskling.InfrastructureContracts.Blocks;

namespace Taskling.InfrastructureContracts.Blocks.CommonRequests
{
    public class BlockExecutionChangeStatusRequest : BlockRequestBase
    {
        public BlockExecutionChangeStatusRequest(TaskId taskId,
            string taskExecutionId,
            BlockType blockType,
            string blockExecutionId,
            BlockExecutionStatus blockExecutionStatus)
            : base(taskId, taskExecutionId, blockType, blockExecutionId)
        {
            BlockExecutionStatus = blockExecutionStatus;
        }

        public BlockExecutionStatus BlockExecutionStatus { get; set; }
        public int ItemsProcessed { get; set; }
    }
}
