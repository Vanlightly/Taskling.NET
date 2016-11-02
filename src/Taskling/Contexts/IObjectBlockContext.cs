using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.ObjectBlocks;

namespace Taskling.Contexts
{
    public interface IObjectBlockContext<T> : IBlockContext
    {
        IObjectBlock<T> Block { get; }
        string ForcedBlockQueueId { get; }
    }
}
