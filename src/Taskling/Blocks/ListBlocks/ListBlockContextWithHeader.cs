using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Contexts;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.Blocks.ListBlocks
{
    public class ListBlockContext<TItem, THeader> : ListBlockContextBase<TItem, THeader>, IListBlockContext<TItem, THeader>, IDisposable
    {
        public ListBlockContext(IListBlockRepository listBlockRepository,
            ITaskExecutionRepository taskExecutionRepository,
            string applicationName,
            string taskName,
            string taskExecutionId,
            ListUpdateMode listUpdateMode,
            int uncommittedThreshold,
            ListBlock<TItem, THeader> listBlock,
            string blockExecutionId,
            int maxStatusReasonLength,
            string forcedBlockQueueId = "0")
            : base(listBlockRepository,
                  taskExecutionRepository,
                  applicationName,
                  taskName,
                  taskExecutionId,
                  listUpdateMode,
                  uncommittedThreshold,
                  listBlock,
                  blockExecutionId,
                  maxStatusReasonLength,
                  forcedBlockQueueId)
        {
            _blockWithHeader.SetParentContext(this);
        }

        public IListBlock<TItem, THeader> Block { get { return _blockWithHeader; } }

    }
}
