using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Contexts;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.TaskExecution;

namespace Taskling.Blocks.ListBlocks
{
    public class ListBlockContext<T> : ListBlockContextBase<T, bool>, IListBlockContext<T>, IDisposable
    {


        #region .: Constructor :.

        public ListBlockContext(IListBlockRepository listBlockRepository,
            ITaskExecutionRepository taskExecutionRepository,
            string applicationName,
            string taskName,
            string taskExecutionId,
            ListUpdateMode listUpdateMode,
            int uncommittedThreshold,
            ListBlock<T> listBlock,
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
            _headerlessBlock.SetParentContext(this);
        }

        #endregion .: Constructor :.

        public IListBlock<T> Block { get { return _headerlessBlock; } }




    }
}
