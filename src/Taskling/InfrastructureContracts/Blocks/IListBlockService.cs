using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.Blocks.ListBlocks;

namespace Taskling.InfrastructureContracts.Blocks
{
    public interface IListBlockService
    {
        void ChangeStatus(BlockExecutionChangeStatusRequest changeStatusRequest);
        IList<ListBlockItem> GetListBlockItems(string listBlockId);
        void UpdateListBlockItem(SingleUpdateRequest singeUpdateRequest);
        void BatchUpdateListBlockItems(BatchUpdateRequest batchUpdateRequest);
    }
}
