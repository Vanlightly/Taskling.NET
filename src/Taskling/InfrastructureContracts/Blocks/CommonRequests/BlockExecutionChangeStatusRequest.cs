using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks;
using Taskling.InfrastructureContracts.Blocks;

namespace Taskling.InfrastructureContracts.Blocks.CommonRequests
{
    public class BlockExecutionChangeStatusRequest : BlockRequestBase
    {
        public BlockExecutionChangeStatusRequest(string applicationName,
            string taskName,
            int taskExecutionId,
            BlockType blockType,
            string blockExecutionId,
            BlockExecutionStatus blockExecutionStatus)
            : base(applicationName, taskName, taskExecutionId, blockType, blockExecutionId)
        {
            BlockExecutionStatus = blockExecutionStatus;
        }

        public BlockExecutionStatus BlockExecutionStatus { get; set; }
    }
}
