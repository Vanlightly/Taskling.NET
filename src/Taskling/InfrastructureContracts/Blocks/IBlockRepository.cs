using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
        IList<ForcedRangeBlockQueueItem> GetQueuedForcedRangeBlocks(QueuedForcedBlocksRequest queuedForcedBlocksRequest);
        IList<ForcedListBlockQueueItem> GetQueuedForcedListBlocks(QueuedForcedBlocksRequest queuedForcedBlocksRequest);
        IList<ForcedObjectBlockQueueItem<T>> GetQueuedForcedObjectBlocks<T>(QueuedForcedBlocksRequest queuedForcedBlocksRequest);
        void DequeueForcedBlocks(DequeueForcedBlocksRequest dequeueForcedBlocksRequest);

        IList<RangeBlock> FindFailedRangeBlocks(FindFailedBlocksRequest failedBlocksRequest);
        IList<RangeBlock> FindDeadRangeBlocks(FindDeadBlocksRequest deadBlocksRequest);
        IList<RangeBlock> FindRangeBlocksOfTask(FindBlocksOfTaskRequest blocksOfTaskRequest);
        RangeBlockCreateResponse AddRangeBlock(RangeBlockCreateRequest rangeBlockCreateRequest);
        string AddRangeBlockExecution(BlockExecutionCreateRequest executionCreateRequest);

        IList<ProtoListBlock> FindFailedListBlocks(FindFailedBlocksRequest failedBlocksRequest);
        IList<ProtoListBlock> FindDeadListBlocks(FindDeadBlocksRequest deadBlocksRequest);
        IList<ProtoListBlock> FindListBlocksOfTask(FindBlocksOfTaskRequest blocksOfTaskRequest);
        ListBlockCreateResponse AddListBlock(ListBlockCreateRequest createRequest);
        string AddListBlockExecution(BlockExecutionCreateRequest executionCreateRequest);

        IList<ObjectBlock<T>> FindObjectBlocksOfTask<T>(FindBlocksOfTaskRequest blocksOfTaskRequest);
        IList<ObjectBlock<T>> FindFailedObjectBlocks<T>(FindFailedBlocksRequest failedBlocksRequest);
        IList<ObjectBlock<T>> FindDeadObjectBlocks<T>(FindDeadBlocksRequest deadBlocksRequest);
        string AddObjectBlockExecution(BlockExecutionCreateRequest executionCreateRequest);
        ObjectBlockCreateResponse<T> AddObjectBlock<T>(ObjectBlockCreateRequest<T> createRequest);

    }
}
