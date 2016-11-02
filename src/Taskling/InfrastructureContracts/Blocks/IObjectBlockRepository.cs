using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.ObjectBlocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;

namespace Taskling.InfrastructureContracts.Blocks
{
    public interface IObjectBlockRepository
    {
        void ChangeStatus(BlockExecutionChangeStatusRequest changeStatusRequest);
        ObjectBlock<T> GetLastObjectBlock<T>(LastBlockRequest lastRangeBlockRequest);
    }
}
