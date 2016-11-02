using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks;
using Taskling.Blocks.Common;

namespace Taskling.InfrastructureContracts.Blocks.ListBlocks
{
    public class ListBlockCreateRequest : BlockRequestBase
    {
        public ListBlockCreateRequest(TaskId taskId,
            string taskExecutionId,
            List<string> serializedValues)
            : base(taskId, taskExecutionId, BlockType.List)
        {
            SerializedValues = serializedValues;
        }

        public ListBlockCreateRequest(TaskId taskId,
            string taskExecutionId,
            List<string> serializedValues,
            string serializedHeader,
            int compressionThreshold)
            : base(taskId, taskExecutionId, BlockType.List)
        {
            SerializedValues = serializedValues;
            SerializedHeader = serializedHeader;
            CompressionThreshold = compressionThreshold;
        }

        public List<string> SerializedValues { get; set; }
        public string SerializedHeader { get; set; }
        public int CompressionThreshold { get; set; }
    }
}
