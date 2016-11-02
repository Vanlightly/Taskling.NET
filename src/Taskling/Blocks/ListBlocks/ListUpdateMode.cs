using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Blocks.ListBlocks
{
    public enum ListUpdateMode
    {
        SingleItemCommit,
        PeriodicBatchCommit,
        BatchCommitAtEnd
    }
}
