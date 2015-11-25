using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.InfrastructureContracts.Blocks.RangeBlocks
{
    public class RangeBlockExecutionCreateRequest : BlockRequestBase
    {
        public RangeBlockExecutionCreateRequest(string applicationName, 
            string taskName, 
            int taskExecutionId, 
            RangeBlockType rangeType,
            string rangeBlockId)
            : base(applicationName, taskName, taskExecutionId, rangeType)
        {
            RangeBlockId = rangeBlockId;
        }

        public string RangeBlockId { get; set; }
    }
}
