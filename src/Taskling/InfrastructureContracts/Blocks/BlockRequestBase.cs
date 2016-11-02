using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks;
using Taskling.Blocks.Common;
using Taskling.InfrastructureContracts.Blocks.RangeBlocks;

namespace Taskling.InfrastructureContracts.Blocks
{
    public class BlockRequestBase
    {
        public BlockRequestBase(TaskId taskId, string taskExecutionId, BlockType blockType)
        {
            TaskId = taskId;
            TaskExecutionId = taskExecutionId;
            BlockType = blockType;
        }

        public BlockRequestBase(TaskId taskId, string taskExecutionId, BlockType blockType, string blockExecutionId)
        {
            TaskId = taskId;
            TaskExecutionId = taskExecutionId;
            BlockExecutionId = blockExecutionId;
            BlockType = blockType;
        }

        public TaskId TaskId { get; private set; }
        public string TaskExecutionId { get; private set; }
        public string BlockExecutionId { get; private set; }
        public BlockType BlockType { get; private set; }
    }
}
