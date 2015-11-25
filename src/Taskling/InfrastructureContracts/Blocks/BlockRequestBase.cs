using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.InfrastructureContracts.Blocks.RangeBlocks;

namespace Taskling.InfrastructureContracts.Blocks
{
    public class BlockRequestBase
    {
        public BlockRequestBase(string applicationName, string taskName, int taskExecutionId, RangeBlockType rangeType)
        {
            ApplicationName = applicationName;
            TaskName = taskName;
            TaskExecutionId = taskExecutionId;
            RangeType = rangeType;
        }

        public BlockRequestBase(string applicationName, string taskName, int taskExecutionId, RangeBlockType rangeType, string blockExecutionId)
        {
            ApplicationName = applicationName;
            TaskName = taskName;
            TaskExecutionId = taskExecutionId;
            BlockExecutionId = blockExecutionId;
            RangeType = rangeType;
        }

        public string ApplicationName { get; private set; }
        public string TaskName { get; private set; }
        public int TaskExecutionId { get; private set; }
        public string BlockExecutionId { get; private set; }
        public RangeBlockType RangeType { get; private set; }
    }
}
