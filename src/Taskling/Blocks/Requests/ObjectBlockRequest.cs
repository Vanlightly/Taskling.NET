using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.Common;

namespace Taskling.Blocks.Requests
{
    public class ObjectBlockRequest<T> : BlockRequest
    {
        public ObjectBlockRequest(T objectData,
            int compressionThreshold)
        {
            BlockType = BlockType.Object;
            Object = objectData;
            CompressionThreshold = compressionThreshold;
        }

        public T Object { get; set; }
        public int CompressionThreshold { get; set; }

    }
}
