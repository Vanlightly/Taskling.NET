using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Blocks.ObjectBlocks
{
    public interface IObjectBlock<T>
    {
        string ObjectBlockId { get; }
        int Attempt { get; set; }
        T Object { get; }
    }
}
