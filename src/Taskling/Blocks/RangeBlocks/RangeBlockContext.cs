using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.Common;
using Taskling.Contexts;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.Retries;

namespace Taskling.Blocks.RangeBlocks
{
    public class RangeBlockContext : IDateRangeBlockContext, INumericRangeBlockContext
    {
        private readonly IRangeBlockRepository _rangeBlockRepository;
        private readonly ITaskExecutionRepository _taskExecutionRepository;
        private readonly string _applicationName;
        private readonly string _taskName;
        private readonly string _taskExecutionId;
        
        public RangeBlockContext(IRangeBlockRepository rangeBlockRepository,
            ITaskExecutionRepository taskExecutionRepository,
            string applicationName,
            string taskName,
            string taskExecutionId,
            RangeBlock rangeBlock,
            string blockExecutionId,
            string forcedBlockQueueId = "0")
        {
            _rangeBlockRepository = rangeBlockRepository;
            _taskExecutionRepository = taskExecutionRepository;
            _block = rangeBlock;
            BlockExecutionId = blockExecutionId;
            ForcedBlockQueueId = forcedBlockQueueId;
            _applicationName = applicationName;
            _taskName = taskName;
            _taskExecutionId = taskExecutionId;
        }

        private RangeBlock _block;

        public IDateRangeBlock DateRangeBlock
        {
            get
            {
                return _block;
            }
        }

        public INumericRangeBlock NumericRangeBlock
        {
            get
            {
                return _block;
            }

        }

        public string BlockExecutionId { get; private set; }
        public string ForcedBlockQueueId { get; private set; }

        public void Start()
        {


            var request = new BlockExecutionChangeStatusRequest(new TaskId(_applicationName, _taskName),
                _taskExecutionId,
                _block.RangeType,
                BlockExecutionId,
                BlockExecutionStatus.Started);

            Action<BlockExecutionChangeStatusRequest> actionRequest = _rangeBlockRepository.ChangeStatus;
            RetryService.InvokeWithRetry(actionRequest, request);
        }

        public void Complete()
        {
            Complete(-1);
        }

        public void Complete(int itemsProcessed)
        {
            var request = new BlockExecutionChangeStatusRequest(new TaskId(_applicationName, _taskName),
                _taskExecutionId,
                _block.RangeType,
                BlockExecutionId,
                BlockExecutionStatus.Completed);
            request.ItemsProcessed = itemsProcessed;

            Action<BlockExecutionChangeStatusRequest> actionRequest = _rangeBlockRepository.ChangeStatus;
            RetryService.InvokeWithRetry(actionRequest, request);
        }

        public void Failed()
        {
            var request = new BlockExecutionChangeStatusRequest(new TaskId(_applicationName, _taskName),
                _taskExecutionId,
                _block.RangeType,
                BlockExecutionId,
                BlockExecutionStatus.Failed);

            Action<BlockExecutionChangeStatusRequest> actionRequest = _rangeBlockRepository.ChangeStatus;
            RetryService.InvokeWithRetry(actionRequest, request);
        }

        public void Failed(string message)
        {
            Failed();

            string errorMessage = string.Empty;
            if (_block.RangeType == BlockType.DateRange)
            {
                errorMessage = string.Format("BlockId {0} From: {1} To: {2} Error: {3}",
                    _block.RangeBlockId,
                    _block.RangeBeginAsDateTime().ToString("yyyy-MM-dd HH:mm:ss"),
                    _block.RangeEndAsDateTime().ToString("yyyy-MM-dd HH:mm:ss"),
                    message);
            }
            else
            {
                errorMessage = string.Format("BlockId {0} From: {1} To: {2} Error: {3}",
                        _block.RangeBlockId,
                        _block.RangeBeginAsLong(),
                        _block.RangeEndAsLong(),
                        message);
            }


            var errorRequest = new TaskExecutionErrorRequest()
            {
                TaskId = new TaskId(_applicationName, _taskName),
                TaskExecutionId = _taskExecutionId,
                TreatTaskAsFailed = false,
                Error = errorMessage
            };
            _taskExecutionRepository.Error(errorRequest);
        }
    }
}
