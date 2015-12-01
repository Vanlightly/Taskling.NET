using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks;
using Taskling.InfrastructureContracts.Blocks;

namespace Taskling.InfrastructureContracts.Blocks.CommonRequests
{
    public class FindFailedBlocksRequest : BlockRequestBase
    {
        public FindFailedBlocksRequest(string applicationName, 
            string taskName, 
            string taskExecutionId, 
            BlockType blockType,
            DateTime failedTaskDateLimit,
            int blockCountLimit)
            : base(applicationName, taskName, taskExecutionId, blockType)
        {
            FailedTaskDateLimit = failedTaskDateLimit;
            BlockCountLimit = blockCountLimit;
        }

        public DateTime FailedTaskDateLimit { get; set; }
        public int BlockCountLimit { get; set; }
    }
}
