using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.Blocks.ListBlocks;

namespace Taskling.InfrastructureContracts.Blocks
{
    public interface IListBlockRepository
    {
        void ChangeStatus(BlockExecutionChangeStatusRequest changeStatusRequest);
        IList<ProtoListBlockItem> GetListBlockItems(TaskId taskId, string listBlockId);
        void UpdateListBlockItem(SingleUpdateRequest singeUpdateRequest);
        void BatchUpdateListBlockItems(BatchUpdateRequest batchUpdateRequest);
        ProtoListBlock GetLastListBlock(LastBlockRequest lastRangeBlockRequest);
    }
}
