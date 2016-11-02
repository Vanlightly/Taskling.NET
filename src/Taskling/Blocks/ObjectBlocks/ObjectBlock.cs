using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Blocks.ObjectBlocks
{
    public class ObjectBlock<T> : IObjectBlock<T>
    {
        public ObjectBlock()
        {

        }

        public string ObjectBlockId { get; set; }
        public int Attempt { get; set; }
        public T Object { get; set; }
    }
}
