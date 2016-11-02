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

namespace Taskling.Blocks.ObjectBlocks
{
    public class ObjectBlockContext<T> : IObjectBlockContext<T>
    {
        private readonly IObjectBlockRepository _objectBlockRepository;
        private readonly ITaskExecutionRepository _taskExecutionRepository;
        private readonly string _applicationName;
        private readonly string _taskName;
        private readonly string _taskExecutionId;

        public ObjectBlockContext(IObjectBlockRepository objectBlockRepository,
            ITaskExecutionRepository taskExecutionRepository,
            string applicationName,
            string taskName,
            string taskExecutionId,
            ObjectBlock<T> block,
            string blockExecutionId,
            string forcedBlockQueueId = "0")
        {
            _objectBlockRepository = objectBlockRepository;
            _taskExecutionRepository = taskExecutionRepository;
            Block = block;
            BlockExecutionId = blockExecutionId;
            ForcedBlockQueueId = forcedBlockQueueId;
            _applicationName = applicationName;
            _taskName = taskName;
            _taskExecutionId = taskExecutionId;
        }

        public IObjectBlock<T> Block { get; private set; }
        public string BlockExecutionId { get; private set; }
        public string ForcedBlockQueueId { get; private set; }

        public void Start()
        {
            var request = new BlockExecutionChangeStatusRequest(new TaskId(_applicationName, _taskName),
                _taskExecutionId,
                BlockType.Object,
                BlockExecutionId,
                BlockExecutionStatus.Started);

            Action<BlockExecutionChangeStatusRequest> actionRequest = _objectBlockRepository.ChangeStatus;
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
                BlockType.Object,
                BlockExecutionId,
                BlockExecutionStatus.Completed);
            request.ItemsProcessed = itemsProcessed;

            Action<BlockExecutionChangeStatusRequest> actionRequest = _objectBlockRepository.ChangeStatus;
            RetryService.InvokeWithRetry(actionRequest, request);
        }

        public void Failed()
        {
            var request = new BlockExecutionChangeStatusRequest(new TaskId(_applicationName, _taskName),
                _taskExecutionId,
                BlockType.Object,
                BlockExecutionId,
                BlockExecutionStatus.Failed);

            Action<BlockExecutionChangeStatusRequest> actionRequest = _objectBlockRepository.ChangeStatus;
            RetryService.InvokeWithRetry(actionRequest, request);
        }

        public void Failed(string message)
        {
            Failed();

            string errorMessage = errorMessage = string.Format("BlockId {0} Error: {1}",
                    Block.ObjectBlockId,
                    message);

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
