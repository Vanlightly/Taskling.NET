using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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

        public async Task StartAsync()
        {
            var request = new BlockExecutionChangeStatusRequest(new TaskId(_applicationName, _taskName),
                _taskExecutionId,
                BlockType.Object,
                BlockExecutionId,
                BlockExecutionStatus.Started);

            Func<BlockExecutionChangeStatusRequest, Task> actionRequest = _objectBlockRepository.ChangeStatusAsync;
            await RetryService.InvokeWithRetryAsync(actionRequest, request).ConfigureAwait(false);
        }

        public async Task CompleteAsync()
        {
            await CompleteAsync(-1).ConfigureAwait(false);
        }

        public async Task CompleteAsync(int itemsProcessed)
        {
            var request = new BlockExecutionChangeStatusRequest(new TaskId(_applicationName, _taskName),
                _taskExecutionId,
                BlockType.Object,
                BlockExecutionId,
                BlockExecutionStatus.Completed);
            request.ItemsProcessed = itemsProcessed;

            Func<BlockExecutionChangeStatusRequest, Task> actionRequest = _objectBlockRepository.ChangeStatusAsync;
            await RetryService.InvokeWithRetryAsync(actionRequest, request).ConfigureAwait(false);
        }

        public async Task FailedAsync()
        {
            var request = new BlockExecutionChangeStatusRequest(new TaskId(_applicationName, _taskName),
                _taskExecutionId,
                BlockType.Object,
                BlockExecutionId,
                BlockExecutionStatus.Failed);

            Func<BlockExecutionChangeStatusRequest, Task> actionRequest = _objectBlockRepository.ChangeStatusAsync;
            await RetryService.InvokeWithRetryAsync(actionRequest, request).ConfigureAwait(false);
        }

        public async Task FailedAsync(string message)
        {
            await FailedAsync().ConfigureAwait(false);

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
            await _taskExecutionRepository.ErrorAsync(errorRequest).ConfigureAwait(false);
        }
    }
}
