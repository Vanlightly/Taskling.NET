using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks;

namespace Taskling.InfrastructureContracts.Blocks.RangeBlocks
{
    public class BlockExecutionCreateRequest : BlockRequestBase
    {
        public BlockExecutionCreateRequest(string applicationName, 
            string taskName, 
            string taskExecutionId, 
            BlockType blockType,
            string blockId)
            : base(applicationName, taskName, taskExecutionId, blockType)
        {
            BlockId = blockId;
        }

        public string BlockId { get; set; }
    }
}
