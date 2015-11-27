using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks;
using Taskling.InfrastructureContracts.Blocks.RangeBlocks;

namespace Taskling.ExecutionContext.FluentBlocks
{
    public class FluentListBlockDescriptor : FluentBlockSettingsDescriptor
    {
        public FluentListBlockDescriptor(List<string> values, short maxBlockSize)
        {
            Values = values;
            MaxBlockSize = maxBlockSize;
            BlockType = BlockType.List;
        }
    }
}
