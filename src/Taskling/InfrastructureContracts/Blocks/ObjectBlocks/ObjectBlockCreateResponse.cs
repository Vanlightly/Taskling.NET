using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.ObjectBlocks;

namespace Taskling.InfrastructureContracts.Blocks.ObjectBlocks
{
    public class ObjectBlockCreateResponse<T>
    {
        public ObjectBlock<T> Block { get; set; }
    }
}
