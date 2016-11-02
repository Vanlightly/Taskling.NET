using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.Common;

namespace Taskling.InfrastructureContracts.Blocks.ObjectBlocks
{
    public class ObjectBlockCreateRequest<T> : BlockRequestBase
    {
        public ObjectBlockCreateRequest(TaskId taskId,
            string taskExecutionId,
            T objectData,
            int compressionThreshold)
            : base(taskId, taskExecutionId, BlockType.Object)
        {
            Object = objectData;
            CompressionThreshold = compressionThreshold;
        }

        public T Object { get; set; }
        public int CompressionThreshold { get; set; }
    }
}
