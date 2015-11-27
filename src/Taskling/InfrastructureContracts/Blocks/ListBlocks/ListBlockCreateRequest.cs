using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks;

namespace Taskling.InfrastructureContracts.Blocks.ListBlocks
{
    public class ListBlockCreateRequest : BlockRequestBase
    {
        public ListBlockCreateRequest(string applicationName,
            string taskName,
            int taskExecutionId,
            List<string> values)
            : base(applicationName, taskName, taskExecutionId, BlockType.List)
        {
            Values = values;
        }

        public List<string> Values { get; set; }
    }
}
