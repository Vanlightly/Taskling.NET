using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Blocks.Requests
{
    public class ListBlockRequest : BlockRequest
    {
        public ListBlockRequest()
        {
            BlockType = BlockType.List;
        }

        public List<string> Values { get; set; }
        public int MaxBlockSize { get; set; }
        public ListUpdateMode ListUpdateMode { get; set; }
        public int UncommittedItemsThreshold { get; set; }
    }
}
