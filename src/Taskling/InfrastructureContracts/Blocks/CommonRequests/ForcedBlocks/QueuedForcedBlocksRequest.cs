using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.Common;

namespace Taskling.InfrastructureContracts.Blocks.CommonRequests.ForcedBlocks
{
    public class QueuedForcedBlocksRequest : BlockRequestBase
    {
        public QueuedForcedBlocksRequest(TaskId taskId,
            string taskExecutionId,
            BlockType blockType)
            : base(taskId, taskExecutionId, blockType)
        {
        }


    }
}
