using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.RangeBlocks;

namespace Taskling.Blocks
{
    public interface IRangeBlockContext : IBlockContext
    {
        RangeBlock Block { get; }
    }
}
