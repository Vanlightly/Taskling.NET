using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.Blocks.ListBlocks;

namespace Taskling.InfrastructureContracts.Blocks
{
    public interface IListBlockRepository
    {
        Task ChangeStatusAsync(BlockExecutionChangeStatusRequest changeStatusRequest);
        Task<IList<ProtoListBlockItem>> GetListBlockItemsAsync(TaskId taskId, string listBlockId);
        Task UpdateListBlockItemAsync(SingleUpdateRequest singeUpdateRequest);
        Task BatchUpdateListBlockItemsAsync(BatchUpdateRequest batchUpdateRequest);
        Task<ProtoListBlock> GetLastListBlockAsync(LastBlockRequest lastRangeBlockRequest);
    }
}
