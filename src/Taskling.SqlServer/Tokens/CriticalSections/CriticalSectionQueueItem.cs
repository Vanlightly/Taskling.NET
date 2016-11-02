using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.Tokens.CriticalSections
{
    internal class CriticalSectionQueueItem
    {
        public CriticalSectionQueueItem()
        {
        }

        public CriticalSectionQueueItem(int index, string taskExecutionId)
        {
            Index = index;
            TaskExecutionId = taskExecutionId;
        }

        public int Index { get; set; }
        public string TaskExecutionId { get; set; }
    }
}
