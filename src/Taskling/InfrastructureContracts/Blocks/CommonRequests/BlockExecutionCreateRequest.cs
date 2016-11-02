using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks;
using Taskling.Blocks.Common;

namespace Taskling.InfrastructureContracts.Blocks.RangeBlocks
{
    public class BlockExecutionCreateRequest : BlockRequestBase
    {
        public BlockExecutionCreateRequest(TaskId taskId,
            string taskExecutionId,
            BlockType blockType,
            string blockId,
            int attempt)
            : base(taskId, taskExecutionId, blockType)
        {
            BlockId = blockId;
            Attempt = attempt;
        }

        public string BlockId { get; set; }
        public int Attempt { get; set; }
    }
}
