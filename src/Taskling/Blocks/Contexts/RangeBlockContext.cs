using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.ExecutionContext;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.Blocks.RangeBlocks;
using Taskling.Retries;

namespace Taskling.Blocks
{
    public class RangeBlockContext : IRangeBlockContext
    {
        private readonly IRangeBlockService _rangeBlockService;
        private readonly string _applicationName;
        private readonly string _taskName;
        private readonly int _taskExecutionId;

        public RangeBlockContext(IRangeBlockService rangeBlockService,
            string applicationName,
            string taskName,
            int taskExecutionId,
            RangeBlock rangeBlock, 
            string blockExecutionId)
        {
            _rangeBlockService = rangeBlockService;
            Block = rangeBlock;
            BlockExecutionId = blockExecutionId;
            _applicationName = applicationName;
            _taskName = taskName;
            _taskExecutionId = taskExecutionId;
        }

        public RangeBlock Block { get; private set; }
        public string BlockExecutionId { get; private set; }
        
        public void Start()
        {
            var request = new BlockExecutionChangeStatusRequest(_applicationName,
                _taskName,
                _taskExecutionId,
                Block.RangeType,
                BlockExecutionId,
                BlockExecutionStatus.Started);

            Action<BlockExecutionChangeStatusRequest> actionRequest = _rangeBlockService.ChangeStatus;
            RetryService.InvokeWithRetry(actionRequest, request);
        }

        public void Complete()
        {
            var request = new BlockExecutionChangeStatusRequest(_applicationName,
                _taskName,
                _taskExecutionId,
                Block.RangeType,
                BlockExecutionId,
                BlockExecutionStatus.Completed);

            Action<BlockExecutionChangeStatusRequest> actionRequest = _rangeBlockService.ChangeStatus;
            RetryService.InvokeWithRetry(actionRequest, request);
        }

        public void Failed()
        {
            var request = new BlockExecutionChangeStatusRequest(_applicationName,
                _taskName,
                _taskExecutionId,
                Block.RangeType,
                BlockExecutionId,
                BlockExecutionStatus.Failed);

            Action<BlockExecutionChangeStatusRequest> actionRequest = _rangeBlockService.ChangeStatus;
            RetryService.InvokeWithRetry(actionRequest, request);
        }

        
    }
}
