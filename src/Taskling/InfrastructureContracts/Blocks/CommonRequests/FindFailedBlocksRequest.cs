using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks;
using Taskling.Blocks.Common;
using Taskling.InfrastructureContracts.Blocks;

namespace Taskling.InfrastructureContracts.Blocks.CommonRequests
{
    public class FindFailedBlocksRequest : BlockRequestBase
    {
        public FindFailedBlocksRequest(TaskId taskId,
            string taskExecutionId,
            BlockType blockType,
            DateTime searchPeriodBegin,
            DateTime searchPeriodEnd,
            int blockCountLimit,
            int retryLimit)
            : base(taskId, taskExecutionId, blockType)
        {
            SearchPeriodBegin = searchPeriodBegin;
            SearchPeriodEnd = searchPeriodEnd;
            BlockCountLimit = blockCountLimit;
            RetryLimit = retryLimit;
        }

        public DateTime SearchPeriodBegin { get; set; }
        public DateTime SearchPeriodEnd { get; set; }
        public int BlockCountLimit { get; set; }
        public int RetryLimit { get; set; }
    }
}
