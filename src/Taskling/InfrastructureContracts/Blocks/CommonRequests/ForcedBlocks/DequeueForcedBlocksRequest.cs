using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.Common;

namespace Taskling.InfrastructureContracts.Blocks.CommonRequests.ForcedBlocks
{
    public class DequeueForcedBlocksRequest : BlockRequestBase
    {
        public DequeueForcedBlocksRequest(TaskId taskId,
            string taskExecutionId,
            BlockType blockType,
            List<string> forcedBlockQueueIds)
            : base(taskId, taskExecutionId, blockType)
        {
            ForcedBlockQueueIds = forcedBlockQueueIds;
        }

        public List<string> ForcedBlockQueueIds { get; set; }
    }
}
