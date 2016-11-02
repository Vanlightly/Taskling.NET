using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.ListBlocks;

namespace Taskling.Blocks.Requests
{
    public class ListBlockRequest : BlockRequest
    {
        public ListBlockRequest()
        {
            BlockType = Taskling.Blocks.Common.BlockType.List;
        }

        public List<string> SerializedValues { get; set; }
        public string SerializedHeader { get; set; }
        public int CompressionThreshold { get; set; }
        public int MaxStatusReasonLength { get; set; }
        public int MaxBlockSize { get; set; }
        public ListUpdateMode ListUpdateMode { get; set; }
        public int UncommittedItemsThreshold { get; set; }
    }
}
