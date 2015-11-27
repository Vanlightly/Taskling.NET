using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks;
using Taskling.InfrastructureContracts.Blocks.RangeBlocks;

namespace Taskling.InfrastructureContracts.Blocks
{
    public class BlockRequestBase
    {
        public BlockRequestBase(string applicationName, string taskName, int taskExecutionId, BlockType blockType)
        {
            ApplicationName = applicationName;
            TaskName = taskName;
            TaskExecutionId = taskExecutionId;
            BlockType = blockType;
        }

        public BlockRequestBase(string applicationName, string taskName, int taskExecutionId, BlockType blockType, string blockExecutionId)
        {
            ApplicationName = applicationName;
            TaskName = taskName;
            TaskExecutionId = taskExecutionId;
            BlockExecutionId = blockExecutionId;
            BlockType = blockType;
        }

        public string ApplicationName { get; private set; }
        public string TaskName { get; private set; }
        public int TaskExecutionId { get; private set; }
        public string BlockExecutionId { get; private set; }
        public BlockType BlockType { get; private set; }
    }
}
