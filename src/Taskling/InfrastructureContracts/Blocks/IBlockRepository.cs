using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Taskling.Blocks.ObjectBlocks;
using Taskling.Blocks.RangeBlocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.Blocks.CommonRequests.ForcedBlocks;
using Taskling.InfrastructureContracts.Blocks.ListBlocks;
using Taskling.InfrastructureContracts.Blocks.ObjectBlocks;
using Taskling.InfrastructureContracts.Blocks.RangeBlocks;

namespace Taskling.InfrastructureContracts.Blocks
{
    public interface IBlockRepository
    {
        Task<IList<ForcedRangeBlockQueueItem>> GetQueuedForcedRangeBlocksAsync(QueuedForcedBlocksRequest queuedForcedBlocksRequest);
        Task<IList<ForcedListBlockQueueItem>> GetQueuedForcedListBlocksAsync(QueuedForcedBlocksRequest queuedForcedBlocksRequest);
        Task<IList<ForcedObjectBlockQueueItem<T>>> GetQueuedForcedObjectBlocksAsync<T>(QueuedForcedBlocksRequest queuedForcedBlocksRequest);
        Task DequeueForcedBlocksAsync(DequeueForcedBlocksRequest dequeueForcedBlocksRequest);

        Task<IList<RangeBlock>> FindFailedRangeBlocksAsync(FindFailedBlocksRequest failedBlocksRequest);
        Task<IList<RangeBlock>> FindDeadRangeBlocksAsync(FindDeadBlocksRequest deadBlocksRequest);
        Task<IList<RangeBlock>> FindRangeBlocksOfTaskAsync(FindBlocksOfTaskRequest blocksOfTaskRequest);
        Task<RangeBlockCreateResponse> AddRangeBlockAsync(RangeBlockCreateRequest rangeBlockCreateRequest);
        Task<string> AddRangeBlockExecutionAsync(BlockExecutionCreateRequest executionCreateRequest);
        
        Task<IList<ProtoListBlock>> FindFailedListBlocksAsync(FindFailedBlocksRequest failedBlocksRequest);
        Task<IList<ProtoListBlock>> FindDeadListBlocksAsync(FindDeadBlocksRequest deadBlocksRequest);
        Task<IList<ProtoListBlock>> FindListBlocksOfTaskAsync(FindBlocksOfTaskRequest blocksOfTaskRequest);
        Task<ListBlockCreateResponse> AddListBlockAsync(ListBlockCreateRequest createRequest);
        Task<string> AddListBlockExecutionAsync(BlockExecutionCreateRequest executionCreateRequest);

        Task<IList<ObjectBlock<T>>> FindObjectBlocksOfTaskAsync<T>(FindBlocksOfTaskRequest blocksOfTaskRequest);
        Task<IList<ObjectBlock<T>>> FindFailedObjectBlocksAsync<T>(FindFailedBlocksRequest failedBlocksRequest);
        Task<IList<ObjectBlock<T>>> FindDeadObjectBlocksAsync<T>(FindDeadBlocksRequest deadBlocksRequest);
        Task<string> AddObjectBlockExecutionAsync(BlockExecutionCreateRequest executionCreateRequest);
        Task<ObjectBlockCreateResponse<T>> AddObjectBlockAsync<T>(ObjectBlockCreateRequest<T> createRequest);

    }
}
