using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using Taskling.Blocks.Common;
using Taskling.Blocks.ListBlocks;
using Taskling.Blocks.ObjectBlocks;
using Taskling.Blocks.RangeBlocks;
using Taskling.Exceptions;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.Blocks.CommonRequests.ForcedBlocks;
using Taskling.InfrastructureContracts.Blocks.ListBlocks;
using Taskling.InfrastructureContracts.Blocks.ObjectBlocks;
using Taskling.InfrastructureContracts.Blocks.RangeBlocks;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.Serialization;
using Taskling.SqlServer.AncilliaryServices;
using Taskling.SqlServer.Blocks.QueryBuilders;
using Taskling.SqlServer.Blocks.Serialization;
using Taskling.Tasks;

namespace Taskling.SqlServer.Blocks
{
    public class BlockRepository : DbOperationsService, IBlockRepository
    {
        #region .: Fields and services :.

        private readonly ITaskRepository _taskRepository;
        private const string UnexpectedBlockTypeMessage = "This block type was not expected. This can occur when changing the block type of an existing process or combining different block types in a single process - which is not supported";

        #endregion .: Fields and services :.

        #region .: Constructor :.

        public BlockRepository(ITaskRepository taskRepository)
        {
            _taskRepository = taskRepository;
        }

        #endregion .: Constructor :.

        #region .: Public Methods :.

        #region .: Force Block Queue :.

        public IList<ForcedRangeBlockQueueItem> GetQueuedForcedRangeBlocks(QueuedForcedBlocksRequest queuedForcedBlocksRequest)
        {
            string query = string.Empty;
            switch (queuedForcedBlocksRequest.BlockType)
            {
                case BlockType.DateRange:
                    return GetForcedDateRangeBlocks(queuedForcedBlocksRequest);
                case BlockType.NumericRange:
                    return GetForcedNumericRangeBlocks(queuedForcedBlocksRequest);
                default:
                    throw new NotSupportedException("This range type is not supported");
            }
        }

        public IList<ForcedListBlockQueueItem> GetQueuedForcedListBlocks(QueuedForcedBlocksRequest queuedForcedBlocksRequest)
        {
            return GetForcedListBlocks(queuedForcedBlocksRequest);
        }

        public IList<ForcedObjectBlockQueueItem<T>> GetQueuedForcedObjectBlocks<T>(QueuedForcedBlocksRequest queuedForcedBlocksRequest)
        {
            return GetForcedObjectBlocks<T>(queuedForcedBlocksRequest);
        }

        public void DequeueForcedBlocks(DequeueForcedBlocksRequest dequeueForcedBlocksRequest)
        {
            UpdateForcedBlocks(dequeueForcedBlocksRequest);
        }

        #endregion .: Force Block Queue :.

        #region .: Range Blocks :.

        public IList<RangeBlock> FindFailedRangeBlocks(FindFailedBlocksRequest failedBlocksRequest)
        {
            string query = string.Empty;
            switch (failedBlocksRequest.BlockType)
            {
                case BlockType.DateRange:
                    query = FailedBlocksQueryBuilder.GetFindFailedDateRangeBlocksQuery(failedBlocksRequest.BlockCountLimit);
                    break;
                case BlockType.NumericRange:
                    query = FailedBlocksQueryBuilder.GetFindFailedNumericRangeBlocksQuery(failedBlocksRequest.BlockCountLimit);
                    break;
                default:
                    throw new NotSupportedException("This range type is not supported");
            }

            return FindFailedDateRangeBlocks(failedBlocksRequest, query);
        }

        public IList<RangeBlock> FindDeadRangeBlocks(FindDeadBlocksRequest deadBlocksRequest)
        {
            string query = string.Empty;
            switch (deadBlocksRequest.BlockType)
            {
                case BlockType.DateRange:
                    if (deadBlocksRequest.TaskDeathMode == TaskDeathMode.KeepAlive)
                        query = DeadBlocksQueryBuilder.GetFindDeadDateRangeBlocksWithKeepAliveQuery(deadBlocksRequest.BlockCountLimit);
                    else
                        query = DeadBlocksQueryBuilder.GetFindDeadDateRangeBlocksQuery(deadBlocksRequest.BlockCountLimit);
                    break;
                case BlockType.NumericRange:
                    if (deadBlocksRequest.TaskDeathMode == TaskDeathMode.KeepAlive)
                        query = DeadBlocksQueryBuilder.GetFindDeadNumericRangeBlocksWithKeepAliveQuery(deadBlocksRequest.BlockCountLimit);
                    else
                        query = DeadBlocksQueryBuilder.GetFindDeadNumericRangeBlocksQuery(deadBlocksRequest.BlockCountLimit);
                    break;
                default:
                    throw new NotSupportedException("This range type is not supported");
            }

            return FindDeadDateRangeBlocks(deadBlocksRequest, query);
        }

        public IList<RangeBlock> FindRangeBlocksOfTask(FindBlocksOfTaskRequest blocksOfTaskRequest)
        {
            string query = string.Empty;
            switch (blocksOfTaskRequest.BlockType)
            {
                case BlockType.DateRange:
                    query = BlocksOfTaskQueryBuilder.GetFindDateRangeBlocksOfTaskQuery(blocksOfTaskRequest.ReprocessOption);
                    break;
                case BlockType.NumericRange:
                    query = BlocksOfTaskQueryBuilder.GetFindNumericRangeBlocksOfTaskQuery(blocksOfTaskRequest.ReprocessOption);
                    break;
                default:
                    throw new NotSupportedException("This range type is not supported");
            }

            return FindRangeBlocksOfTask(blocksOfTaskRequest, query);
        }

        public RangeBlockCreateResponse AddRangeBlock(RangeBlockCreateRequest rangeBlockCreateRequest)
        {
            var taskDefinition = _taskRepository.EnsureTaskDefinition(rangeBlockCreateRequest.TaskId);

            var response = new RangeBlockCreateResponse();
            switch (rangeBlockCreateRequest.BlockType)
            {
                case BlockType.DateRange:
                    response.Block = AddDateRangeRangeBlock(rangeBlockCreateRequest, taskDefinition.TaskDefinitionId);
                    break;
                case BlockType.NumericRange:
                    response.Block = AddNumericRangeRangeBlock(rangeBlockCreateRequest, taskDefinition.TaskDefinitionId);
                    break;
                default:
                    throw new NotSupportedException(UnexpectedBlockTypeMessage);
            }

            return response;
        }

        public string AddRangeBlockExecution(BlockExecutionCreateRequest executionCreateRequest)
        {
            return AddBlockExecution(executionCreateRequest);
        }

        #endregion .: Range Blocks :.

        #region .: List Blocks :.

        public IList<ProtoListBlock> FindFailedListBlocks(FindFailedBlocksRequest failedBlocksRequest)
        {
            if (failedBlocksRequest.BlockType == BlockType.List)
            {
                var query = FailedBlocksQueryBuilder.GetFindFailedListBlocksQuery(failedBlocksRequest.BlockCountLimit);
                return FindFailedListBlocks(failedBlocksRequest, query);
            }

            throw new NotSupportedException(UnexpectedBlockTypeMessage);
        }

        public IList<ProtoListBlock> FindDeadListBlocks(FindDeadBlocksRequest deadBlocksRequest)
        {
            if (deadBlocksRequest.BlockType == BlockType.List)
            {
                string query = string.Empty;
                if (deadBlocksRequest.TaskDeathMode == TaskDeathMode.KeepAlive)
                    query = DeadBlocksQueryBuilder.GetFindDeadListBlocksWithKeepAliveQuery(deadBlocksRequest.BlockCountLimit);
                else
                    query = DeadBlocksQueryBuilder.GetFindDeadListBlocksQuery(deadBlocksRequest.BlockCountLimit);

                return FindDeadListBlocks(deadBlocksRequest, query);
            }

            throw new NotSupportedException(UnexpectedBlockTypeMessage);
        }

        public IList<ProtoListBlock> FindListBlocksOfTask(FindBlocksOfTaskRequest blocksOfTaskRequest)
        {
            if (blocksOfTaskRequest.BlockType == BlockType.List)
            {
                string query = BlocksOfTaskQueryBuilder.GetFindListBlocksOfTaskQuery(blocksOfTaskRequest.ReprocessOption);
                return FindListBlocksOfTask(blocksOfTaskRequest, query, blocksOfTaskRequest.ReprocessOption);
            }

            throw new NotSupportedException(UnexpectedBlockTypeMessage);
        }

        public ListBlockCreateResponse AddListBlock(ListBlockCreateRequest createRequest)
        {
            var taskDefinition = _taskRepository.EnsureTaskDefinition(createRequest.TaskId);

            var response = new ListBlockCreateResponse();
            if (createRequest.BlockType == BlockType.List)
            {
                var blockId = AddNewListBlock(createRequest.TaskId, taskDefinition.TaskDefinitionId, createRequest.SerializedHeader, createRequest.CompressionThreshold);
                AddListBlockItems(blockId, createRequest);

                // we do not populate the items here, they are lazy loaded
                response.Block = new ProtoListBlock()
                {
                    ListBlockId = blockId.ToString(),
                    Header = createRequest.SerializedHeader
                };

                return response;
            }

            throw new NotSupportedException(UnexpectedBlockTypeMessage);
        }

        public string AddListBlockExecution(BlockExecutionCreateRequest executionCreateRequest)
        {
            return AddBlockExecution(executionCreateRequest);
        }

        #endregion .: List Blocks :.

        #region .: Object Blocks :.

        public IList<ObjectBlock<T>> FindObjectBlocksOfTask<T>(FindBlocksOfTaskRequest blocksOfTaskRequest)
        {
            if (blocksOfTaskRequest.BlockType == BlockType.Object)
            {
                string query = BlocksOfTaskQueryBuilder.GetFindObjectBlocksOfTaskQuery(blocksOfTaskRequest.ReprocessOption);
                return FindObjectBlocksOfTask<T>(blocksOfTaskRequest, query, blocksOfTaskRequest.ReprocessOption);
            }

            throw new NotSupportedException(UnexpectedBlockTypeMessage);
        }

        public IList<ObjectBlock<T>> FindFailedObjectBlocks<T>(FindFailedBlocksRequest failedBlocksRequest)
        {
            if (failedBlocksRequest.BlockType == BlockType.Object)
            {
                var query = FailedBlocksQueryBuilder.GetFindFailedObjectBlocksQuery(failedBlocksRequest.BlockCountLimit);
                return FindFailedObjectBlocks<T>(failedBlocksRequest, query);
            }

            throw new NotSupportedException(UnexpectedBlockTypeMessage);
        }

        public IList<ObjectBlock<T>> FindDeadObjectBlocks<T>(FindDeadBlocksRequest deadBlocksRequest)
        {
            if (deadBlocksRequest.BlockType == BlockType.Object)
            {
                string query = string.Empty;
                if (deadBlocksRequest.TaskDeathMode == TaskDeathMode.KeepAlive)
                    query = DeadBlocksQueryBuilder.GetFindDeadObjectBlocksWithKeepAliveQuery(deadBlocksRequest.BlockCountLimit);
                else
                    query = DeadBlocksQueryBuilder.GetFindDeadObjectBlocksQuery(deadBlocksRequest.BlockCountLimit);

                return FindDeadObjectBlocks<T>(deadBlocksRequest, query);
            }

            throw new NotSupportedException(UnexpectedBlockTypeMessage);
        }

        public string AddObjectBlockExecution(BlockExecutionCreateRequest executionCreateRequest)
        {
            return AddBlockExecution(executionCreateRequest);
        }

        public ObjectBlockCreateResponse<T> AddObjectBlock<T>(ObjectBlockCreateRequest<T> createRequest)
        {
            var taskDefinition = _taskRepository.EnsureTaskDefinition(createRequest.TaskId);

            var response = new ObjectBlockCreateResponse<T>();
            if (createRequest.BlockType == BlockType.Object)
            {
                var blockId = AddNewObjectBlock(createRequest.TaskId, taskDefinition.TaskDefinitionId, createRequest.Object, createRequest.CompressionThreshold);
                response.Block = new ObjectBlock<T>()
                {
                    ObjectBlockId = blockId.ToString(),
                    Object = createRequest.Object
                };
                
                return response;
            }

            throw new NotSupportedException(UnexpectedBlockTypeMessage);
        }

        #endregion .: Object Blocks :.

        #endregion .: Public Methods :.

        #region .: Private Methods :.

        #region .: Range Blocks :.

        private IList<RangeBlock> FindFailedDateRangeBlocks(FindFailedBlocksRequest failedBlocksRequest, string query)
        {
            var results = new List<RangeBlock>();
            var taskDefinition = _taskRepository.EnsureTaskDefinition(failedBlocksRequest.TaskId);

            try
            {
                using (var connection = CreateNewConnection(failedBlocksRequest.TaskId))
                {
                    var command = connection.CreateCommand();
                    command.CommandText = query;
                    command.CommandTimeout = ConnectionStore.Instance.GetConnection(failedBlocksRequest.TaskId).QueryTimeoutSeconds;
                    command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinition.TaskDefinitionId;
                    command.Parameters.Add("@SearchPeriodBegin", SqlDbType.DateTime).Value = failedBlocksRequest.SearchPeriodBegin;
                    command.Parameters.Add("@SearchPeriodEnd", SqlDbType.DateTime).Value = failedBlocksRequest.SearchPeriodEnd;
                    command.Parameters.Add("@AttemptLimit", SqlDbType.Int).Value = failedBlocksRequest.RetryLimit + 1; // RetryLimit + 1st attempt
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var blockType = (BlockType)int.Parse(reader["BlockType"].ToString());
                            if (blockType == failedBlocksRequest.BlockType)
                            {
                                var rangeBlockId = reader["BlockId"].ToString();
                                var attempt = int.Parse(reader["Attempt"].ToString());

                                long rangeBegin;
                                long rangeEnd;

                                if (failedBlocksRequest.BlockType == BlockType.DateRange)
                                {
                                    rangeBegin = reader.GetDateTime(1).Ticks;
                                    rangeEnd = reader.GetDateTime(2).Ticks;
                                }
                                else
                                {
                                    rangeBegin = long.Parse(reader["FromNumber"].ToString());
                                    rangeEnd = long.Parse(reader["ToNumber"].ToString());
                                }

                                results.Add(new RangeBlock(rangeBlockId, attempt, rangeBegin, rangeEnd, failedBlocksRequest.BlockType));
                            }
                        }
                    }
                }
            }
            catch (SqlException sqlEx)
            {
                if (TransientErrorDetector.IsTransient(sqlEx))
                    throw new TransientException("A transient exception has occurred", sqlEx);

                throw;
            }

            return results;
        }

        private IList<RangeBlock> FindDeadDateRangeBlocks(FindDeadBlocksRequest deadBlocksRequest, string query)
        {
            var results = new List<RangeBlock>();
            var taskDefinition = _taskRepository.EnsureTaskDefinition(deadBlocksRequest.TaskId);

            try
            {
                using (var connection = CreateNewConnection(deadBlocksRequest.TaskId))
                {
                    var command = connection.CreateCommand();
                    command.CommandText = query;
                    command.CommandTimeout = ConnectionStore.Instance.GetConnection(deadBlocksRequest.TaskId).QueryTimeoutSeconds;
                    command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinition.TaskDefinitionId;
                    command.Parameters.Add("@SearchPeriodBegin", SqlDbType.DateTime).Value = deadBlocksRequest.SearchPeriodBegin;
                    command.Parameters.Add("@SearchPeriodEnd", SqlDbType.DateTime).Value = deadBlocksRequest.SearchPeriodEnd;
                    command.Parameters.Add("@AttemptLimit", SqlDbType.Int).Value = deadBlocksRequest.RetryLimit + 1; // RetryLimit + 1st attempt

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var blockType = (BlockType)int.Parse(reader["BlockType"].ToString());
                            if (blockType == deadBlocksRequest.BlockType)
                            {
                                var rangeBlockId = reader["BlockId"].ToString();
                                var attempt = int.Parse(reader["Attempt"].ToString());

                                long rangeBegin;
                                long rangeEnd;
                                if (deadBlocksRequest.BlockType == BlockType.DateRange)
                                {
                                    rangeBegin = reader.GetDateTime(1).Ticks;
                                    rangeEnd = reader.GetDateTime(2).Ticks;
                                }
                                else
                                {
                                    rangeBegin = long.Parse(reader["FromNumber"].ToString());
                                    rangeEnd = long.Parse(reader["ToNumber"].ToString());
                                }

                                results.Add(new RangeBlock(rangeBlockId, attempt, rangeBegin, rangeEnd, blockType));
                            }
                        }
                    }
                }
            }
            catch (SqlException sqlEx)
            {
                if (TransientErrorDetector.IsTransient(sqlEx))
                    throw new TransientException("A transient exception has occurred", sqlEx);

                throw;
            }

            return results;
        }

        private IList<RangeBlock> FindRangeBlocksOfTask(FindBlocksOfTaskRequest blocksOfTaskRequest, string query)
        {
            var results = new List<RangeBlock>();
            var taskDefinition = _taskRepository.EnsureTaskDefinition(blocksOfTaskRequest.TaskId);

            try
            {
                using (var connection = CreateNewConnection(blocksOfTaskRequest.TaskId))
                {
                    var command = connection.CreateCommand();
                    command.CommandText = query;
                    command.CommandTimeout = ConnectionStore.Instance.GetConnection(blocksOfTaskRequest.TaskId).QueryTimeoutSeconds;
                    command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinition.TaskDefinitionId;
                    command.Parameters.Add("@ReferenceValue", SqlDbType.NVarChar, 200).Value = blocksOfTaskRequest.ReferenceValueOfTask;

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var blockType = (BlockType)int.Parse(reader["BlockType"].ToString());
                            if (blockType != blocksOfTaskRequest.BlockType)
                                throw new ExecutionException("The block with this reference value is of a different BlockType. BlockType resuested: " + blocksOfTaskRequest.BlockType + " BlockType found: " + blockType);

                            var rangeBlockId = reader["BlockId"].ToString();
                            var attempt = int.Parse(reader["Attempt"].ToString());
                            long rangeBegin;
                            long rangeEnd;
                            if (blocksOfTaskRequest.BlockType == BlockType.DateRange)
                            {
                                rangeBegin = reader.GetDateTime(1).Ticks; //DateTime.Parse(reader["FromDate"].ToString()).Ticks;
                                rangeEnd = reader.GetDateTime(2).Ticks;  //DateTime.Parse(reader["ToDate"].ToString()).Ticks;
                            }
                            else
                            {
                                rangeBegin = long.Parse(reader["FromNumber"].ToString());
                                rangeEnd = long.Parse(reader["ToNumber"].ToString());
                            }

                            results.Add(new RangeBlock(rangeBlockId, attempt, rangeBegin, rangeEnd, blocksOfTaskRequest.BlockType));
                        }
                    }
                }
            }
            catch (SqlException sqlEx)
            {
                if (TransientErrorDetector.IsTransient(sqlEx))
                    throw new TransientException("A transient exception has occurred", sqlEx);

                throw;
            }

            return results;
        }

        private RangeBlock AddDateRangeRangeBlock(RangeBlockCreateRequest dateRangeBlockCreateRequest, int taskDefinitionId)
        {
            try
            {
                using (var connection = CreateNewConnection(dateRangeBlockCreateRequest.TaskId))
                {
                    var command = connection.CreateCommand();
                    command.CommandTimeout = ConnectionStore.Instance.GetConnection(dateRangeBlockCreateRequest.TaskId).QueryTimeoutSeconds;
                    command.CommandText = RangeBlockQueryBuilder.InsertDateRangeBlock;
                    command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
                    command.Parameters.Add("@FromDate", SqlDbType.DateTime).Value = new DateTime(dateRangeBlockCreateRequest.From);
                    command.Parameters.Add("@ToDate", SqlDbType.DateTime).Value = new DateTime(dateRangeBlockCreateRequest.To);
                    command.Parameters.Add("@BlockType", SqlDbType.TinyInt).Value = (byte)BlockType.DateRange;
                    var id = command.ExecuteScalar().ToString();

                    return new RangeBlock(id,
                        0,
                        dateRangeBlockCreateRequest.From,
                        dateRangeBlockCreateRequest.To,
                        dateRangeBlockCreateRequest.BlockType);
                }
            }
            catch (SqlException sqlEx)
            {
                if (TransientErrorDetector.IsTransient(sqlEx))
                    throw new TransientException("A transient exception has occurred", sqlEx);

                throw;
            }
        }

        private RangeBlock AddNumericRangeRangeBlock(RangeBlockCreateRequest dateRangeBlockCreateRequest, int taskDefinitionId)
        {
            try
            {
                using (var connection = CreateNewConnection(dateRangeBlockCreateRequest.TaskId))
                {
                    var command = connection.CreateCommand();
                    command.CommandTimeout = ConnectionStore.Instance.GetConnection(dateRangeBlockCreateRequest.TaskId).QueryTimeoutSeconds;
                    command.CommandText = RangeBlockQueryBuilder.InsertNumericRangeBlock;
                    command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
                    command.Parameters.Add("@FromNumber", SqlDbType.BigInt).Value = dateRangeBlockCreateRequest.From;
                    command.Parameters.Add("@ToNumber", SqlDbType.BigInt).Value = dateRangeBlockCreateRequest.To;
                    command.Parameters.Add("@BlockType", SqlDbType.TinyInt).Value = (byte)BlockType.NumericRange;
                    var id = command.ExecuteScalar().ToString();

                    return new RangeBlock(id, 0, dateRangeBlockCreateRequest.From, dateRangeBlockCreateRequest.To, dateRangeBlockCreateRequest.BlockType);
                }
            }
            catch (SqlException sqlEx)
            {
                if (TransientErrorDetector.IsTransient(sqlEx))
                    throw new TransientException("A transient exception has occurred", sqlEx);

                throw;
            }
        }

        #endregion .: Range Blocks :.

        #region .: List Blocks :.

        private IList<ProtoListBlock> FindFailedListBlocks(FindFailedBlocksRequest failedBlocksRequest, string query)
        {
            var results = new List<ProtoListBlock>();
            var taskDefinition = _taskRepository.EnsureTaskDefinition(failedBlocksRequest.TaskId);

            try
            {
                using (var connection = CreateNewConnection(failedBlocksRequest.TaskId))
                {
                    var command = connection.CreateCommand();
                    command.CommandText = query;
                    command.CommandTimeout = ConnectionStore.Instance.GetConnection(failedBlocksRequest.TaskId).QueryTimeoutSeconds;
                    command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinition.TaskDefinitionId;
                    command.Parameters.Add("@SearchPeriodBegin", SqlDbType.DateTime).Value = failedBlocksRequest.SearchPeriodBegin;
                    command.Parameters.Add("@SearchPeriodEnd", SqlDbType.DateTime).Value = failedBlocksRequest.SearchPeriodEnd;
                    command.Parameters.Add("@AttemptLimit", SqlDbType.Int).Value = failedBlocksRequest.RetryLimit + 1; // RetryLimit + 1st attempt
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var blockType = (BlockType)int.Parse(reader["BlockType"].ToString());
                            if (blockType == failedBlocksRequest.BlockType)
                            {
                                var listBlock = new ProtoListBlock();
                                listBlock.ListBlockId = reader["BlockId"].ToString();
                                listBlock.Attempt = int.Parse(reader["Attempt"].ToString());
                                listBlock.Header = SerializedValueReader.ReadValueAsString(reader, "ObjectData", "CompressedObjectData");

                                results.Add(listBlock);
                            }
                        }
                    }
                }
            }
            catch (SqlException sqlEx)
            {
                if (TransientErrorDetector.IsTransient(sqlEx))
                    throw new TransientException("A transient exception has occurred", sqlEx);

                throw;
            }

            return results;
        }

        private IList<ProtoListBlock> FindDeadListBlocks(FindDeadBlocksRequest deadBlocksRequest, string query)
        {
            var results = new List<ProtoListBlock>();
            var taskDefinition = _taskRepository.EnsureTaskDefinition(deadBlocksRequest.TaskId);

            try
            {
                using (var connection = CreateNewConnection(deadBlocksRequest.TaskId))
                {
                    var command = connection.CreateCommand();
                    command.CommandText = query;
                    command.CommandTimeout = ConnectionStore.Instance.GetConnection(deadBlocksRequest.TaskId).QueryTimeoutSeconds;
                    command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinition.TaskDefinitionId;
                    command.Parameters.Add("@SearchPeriodBegin", SqlDbType.DateTime).Value = deadBlocksRequest.SearchPeriodBegin;
                    command.Parameters.Add("@SearchPeriodEnd", SqlDbType.DateTime).Value = deadBlocksRequest.SearchPeriodEnd;
                    command.Parameters.Add("@AttemptLimit", SqlDbType.Int).Value = deadBlocksRequest.RetryLimit + 1; // RetryLimit + 1st attempt

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var blockType = (BlockType)int.Parse(reader["BlockType"].ToString());
                            if (blockType == deadBlocksRequest.BlockType)
                            {
                                var listBlock = new ProtoListBlock();

                                listBlock.ListBlockId = reader["BlockId"].ToString();
                                listBlock.Attempt = int.Parse(reader["Attempt"].ToString());
                                listBlock.Header = SerializedValueReader.ReadValueAsString(reader, "ObjectData", "CompressedObjectData");

                                results.Add(listBlock);
                            }
                        }
                    }
                }
            }
            catch (SqlException sqlEx)
            {
                if (TransientErrorDetector.IsTransient(sqlEx))
                    throw new TransientException("A transient exception has occurred", sqlEx);

                throw;
            }

            return results;
        }

        private IList<ProtoListBlock> FindListBlocksOfTask(FindBlocksOfTaskRequest blocksOfTaskRequest, string query, ReprocessOption reprocessOption)
        {
            var results = new List<ProtoListBlock>();
            var taskDefinition = _taskRepository.EnsureTaskDefinition(blocksOfTaskRequest.TaskId);

            try
            {
                using (var connection = CreateNewConnection(blocksOfTaskRequest.TaskId))
                {
                    var command = connection.CreateCommand();
                    command.CommandText = query;
                    command.CommandTimeout = ConnectionStore.Instance.GetConnection(blocksOfTaskRequest.TaskId).QueryTimeoutSeconds;
                    command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinition.TaskDefinitionId;
                    command.Parameters.Add("@ReferenceValue", SqlDbType.NVarChar, 200).Value = blocksOfTaskRequest.ReferenceValueOfTask;

                    if (reprocessOption == ReprocessOption.PendingOrFailed)
                    {
                        command.Parameters.Add("@NotStarted", SqlDbType.TinyInt).Value = (byte)BlockExecutionStatus.NotStarted;
                        command.Parameters.Add("@Started", SqlDbType.TinyInt).Value = (byte)BlockExecutionStatus.Started;
                        command.Parameters.Add("@Failed", SqlDbType.TinyInt).Value = (byte)BlockExecutionStatus.Failed;
                    }

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var blockType = (BlockType)int.Parse(reader["BlockType"].ToString());
                            if (blockType != blocksOfTaskRequest.BlockType)
                                throw new ExecutionException("The block with this reference value is of a different BlockType. BlockType resuested: " + blocksOfTaskRequest.BlockType + " BlockType found: " + blockType);

                            var listBlock = new ProtoListBlock();
                            listBlock.ListBlockId = reader["BlockId"].ToString();
                            listBlock.Attempt = int.Parse(reader["Attempt"].ToString());
                            listBlock.Header = SerializedValueReader.ReadValueAsString(reader, "ObjectData", "CompressedObjectData");

                            results.Add(listBlock);
                        }
                    }
                }
            }
            catch (SqlException sqlEx)
            {
                if (TransientErrorDetector.IsTransient(sqlEx))
                    throw new TransientException("A transient exception has occurred", sqlEx);

                throw;
            }

            return results;
        }

        private long AddNewListBlock(TaskId taskId, int taskDefinitionId, string header, int compressionThreshold)
        {
            if (header == null)
                header = string.Empty;

            bool isLargeTextValue = false;
            Byte[] compressedData = null;
            if (header.Length > compressionThreshold)
            {
                isLargeTextValue = true;
                compressedData = LargeValueCompressor.Zip(header);
            }

            try
            {
                using (var connection = CreateNewConnection(taskId))
                {
                    var command = connection.CreateCommand();
                    command.CommandTimeout = ConnectionStore.Instance.GetConnection(taskId).QueryTimeoutSeconds;
                    command.CommandText = ListBlockQueryBuilder.InsertListBlock;
                    command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
                    command.Parameters.Add("@BlockType", SqlDbType.TinyInt).Value = (byte)BlockType.List;

                    if (isLargeTextValue)
                    {
                        command.Parameters.Add("@ObjectData", SqlDbType.NVarChar, -1).Value = DBNull.Value;
                        command.Parameters.Add("@CompressedObjectData", SqlDbType.VarBinary, -1).Value = compressedData;
                    }
                    else
                    {
                        command.Parameters.Add("@ObjectData", SqlDbType.NVarChar, -1).Value = header;
                        command.Parameters.Add("@CompressedObjectData", SqlDbType.VarBinary, -1).Value = DBNull.Value;
                    }

                    return (long)command.ExecuteScalar();
                }
            }
            catch (SqlException sqlEx)
            {
                if (TransientErrorDetector.IsTransient(sqlEx))
                    throw new TransientException("A transient exception has occurred", sqlEx);

                throw;
            }
        }

        private void AddListBlockItems(long blockId, ListBlockCreateRequest createRequest)
        {
            using (var connection = CreateNewConnection(createRequest.TaskId))
            {
                var command = connection.CreateCommand();
                var transaction = connection.BeginTransaction();
                command.Connection = connection;
                command.Transaction = transaction;
                command.CommandTimeout = ConnectionStore.Instance.GetConnection(createRequest.TaskId).QueryTimeoutSeconds;

                try
                {
                    DataTable dt = GenerateDataTable(blockId, createRequest.SerializedValues, createRequest.CompressionThreshold);
                    BulkLoadInTransactionOperation(dt, "Taskling.ListBlockItem", connection, transaction);

                    transaction.Commit();
                }
                catch (SqlException sqlEx)
                {
                    TryRollBack(transaction, sqlEx);
                }
                catch (Exception ex)
                {
                    TryRollback(transaction, ex);
                }
            }
        }

        #endregion .: List Blocks :.

        #region .: Object Blocks :.

        private long AddNewObjectBlock<T>(TaskId taskId, int taskDefinitionId, T objectData, int compressionThreshold)
        {
            bool isLargeTextValue = false;
            var jsonValue = JsonGenericSerializer.Serialize<T>(objectData);
            Byte[] compressedData = null;
            if (jsonValue.Length > compressionThreshold)
            {
                isLargeTextValue = true;
                compressedData = LargeValueCompressor.Zip(jsonValue);
            }

            try
            {
                using (var connection = CreateNewConnection(taskId))
                {
                    var command = connection.CreateCommand();
                    command.CommandTimeout = ConnectionStore.Instance.GetConnection(taskId).QueryTimeoutSeconds;
                    command.CommandText = ObjectBlockQueryBuilder.InsertObjectBlock;
                    command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;

                    if (isLargeTextValue)
                    {
                        command.Parameters.Add("@ObjectData", SqlDbType.NVarChar, -1).Value = DBNull.Value;
                        command.Parameters.Add("@CompressedObjectData", SqlDbType.VarBinary, -1).Value = compressedData;
                    }
                    else
                    {
                        command.Parameters.Add("@ObjectData", SqlDbType.NVarChar, -1).Value = jsonValue;
                        command.Parameters.Add("@CompressedObjectData", SqlDbType.VarBinary, -1).Value = DBNull.Value;
                    }

                    command.Parameters.Add("@BlockType", SqlDbType.TinyInt).Value = (byte)BlockType.Object;
                    return (long)command.ExecuteScalar();
                }
            }
            catch (SqlException sqlEx)
            {
                if (TransientErrorDetector.IsTransient(sqlEx))
                    throw new TransientException("A transient exception has occurred", sqlEx);

                throw;
            }
        }

        private IList<ObjectBlock<T>> FindFailedObjectBlocks<T>(FindFailedBlocksRequest failedBlocksRequest, string query)
        {
            var results = new List<ObjectBlock<T>>();
            var taskDefinition = _taskRepository.EnsureTaskDefinition(failedBlocksRequest.TaskId);

            try
            {
                using (var connection = CreateNewConnection(failedBlocksRequest.TaskId))
                {
                    var command = connection.CreateCommand();
                    command.CommandText = query;
                    command.CommandTimeout = ConnectionStore.Instance.GetConnection(failedBlocksRequest.TaskId).QueryTimeoutSeconds;
                    command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinition.TaskDefinitionId;
                    command.Parameters.Add("@SearchPeriodBegin", SqlDbType.DateTime).Value = failedBlocksRequest.SearchPeriodBegin;
                    command.Parameters.Add("@SearchPeriodEnd", SqlDbType.DateTime).Value = failedBlocksRequest.SearchPeriodEnd;
                    command.Parameters.Add("@AttemptLimit", SqlDbType.Int).Value = failedBlocksRequest.RetryLimit + 1; // RetryLimit + 1st attempt
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var blockType = (BlockType)int.Parse(reader["BlockType"].ToString());
                            if (blockType == failedBlocksRequest.BlockType)
                            {
                                var objectBlock = new ObjectBlock<T>();
                                objectBlock.ObjectBlockId = reader["BlockId"].ToString();
                                objectBlock.Attempt = int.Parse(reader["Attempt"].ToString());
                                objectBlock.Object = SerializedValueReader.ReadValue<T>(reader, "ObjectData", "CompressedObjectData");

                                results.Add(objectBlock);
                            }
                            else
                            {
                                throw new NotSupportedException(UnexpectedBlockTypeMessage);
                            }
                        }
                    }
                }
            }
            catch (SqlException sqlEx)
            {
                if (TransientErrorDetector.IsTransient(sqlEx))
                    throw new TransientException("A transient exception has occurred", sqlEx);

                throw;
            }

            return results;
        }

        private IList<ObjectBlock<T>> FindDeadObjectBlocks<T>(FindDeadBlocksRequest deadBlocksRequest, string query)
        {
            var results = new List<ObjectBlock<T>>();
            var taskDefinition = _taskRepository.EnsureTaskDefinition(deadBlocksRequest.TaskId);

            try
            {
                using (var connection = CreateNewConnection(deadBlocksRequest.TaskId))
                {
                    var command = connection.CreateCommand();
                    command.CommandText = query;
                    command.CommandTimeout = ConnectionStore.Instance.GetConnection(deadBlocksRequest.TaskId).QueryTimeoutSeconds;
                    command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinition.TaskDefinitionId;
                    command.Parameters.Add("@SearchPeriodBegin", SqlDbType.DateTime).Value = deadBlocksRequest.SearchPeriodBegin;
                    command.Parameters.Add("@SearchPeriodEnd", SqlDbType.DateTime).Value = deadBlocksRequest.SearchPeriodEnd;
                    command.Parameters.Add("@AttemptLimit", SqlDbType.Int).Value = deadBlocksRequest.RetryLimit + 1; // RetryLimit + 1st attempt

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var blockType = (BlockType)int.Parse(reader["BlockType"].ToString());
                            if (blockType == deadBlocksRequest.BlockType)
                            {
                                var objectBlock = new ObjectBlock<T>();
                                objectBlock.ObjectBlockId = reader["BlockId"].ToString();
                                objectBlock.Attempt = int.Parse(reader["Attempt"].ToString());
                                objectBlock.Object = SerializedValueReader.ReadValue<T>(reader, "ObjectData", "CompressedObjectData");

                                results.Add(objectBlock);
                            }
                            else
                            {
                                throw new NotSupportedException(UnexpectedBlockTypeMessage);
                            }
                        }
                    }
                }
            }
            catch (SqlException sqlEx)
            {
                if (TransientErrorDetector.IsTransient(sqlEx))
                    throw new TransientException("A transient exception has occurred", sqlEx);

                throw;
            }

            return results;
        }

        private IList<ObjectBlock<T>> FindObjectBlocksOfTask<T>(FindBlocksOfTaskRequest blocksOfTaskRequest, string query, ReprocessOption reprocessOption)
        {
            var results = new List<ObjectBlock<T>>();
            var taskDefinition = _taskRepository.EnsureTaskDefinition(blocksOfTaskRequest.TaskId);

            try
            {
                using (var connection = CreateNewConnection(blocksOfTaskRequest.TaskId))
                {
                    var command = connection.CreateCommand();
                    command.CommandText = query;
                    command.CommandTimeout = ConnectionStore.Instance.GetConnection(blocksOfTaskRequest.TaskId).QueryTimeoutSeconds;
                    command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinition.TaskDefinitionId;
                    command.Parameters.Add("@ReferenceValue", SqlDbType.NVarChar, 200).Value = blocksOfTaskRequest.ReferenceValueOfTask;

                    if (reprocessOption == ReprocessOption.PendingOrFailed)
                    {
                        command.Parameters.Add("@NotStarted", SqlDbType.TinyInt).Value = (byte)BlockExecutionStatus.NotStarted;
                        command.Parameters.Add("@Started", SqlDbType.TinyInt).Value = (byte)BlockExecutionStatus.Started;
                        command.Parameters.Add("@Failed", SqlDbType.TinyInt).Value = (byte)BlockExecutionStatus.Failed;
                    }

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var blockType = (BlockType)int.Parse(reader["BlockType"].ToString());
                            if (blockType != blocksOfTaskRequest.BlockType)
                                throw new ExecutionException("The block with this reference value is of a different BlockType. BlockType resuested: " + blocksOfTaskRequest.BlockType + " BlockType found: " + blockType);

                            var objectBlock = new ObjectBlock<T>();
                            objectBlock.ObjectBlockId = reader["BlockId"].ToString();
                            objectBlock.Attempt = int.Parse(reader["Attempt"].ToString());
                            objectBlock.Object = SerializedValueReader.ReadValue<T>(reader, "ObjectData", "CompressedObjectData");

                            results.Add(objectBlock);
                        }
                    }
                }
            }
            catch (SqlException sqlEx)
            {
                if (TransientErrorDetector.IsTransient(sqlEx))
                    throw new TransientException("A transient exception has occurred", sqlEx);

                throw;
            }

            return results;
        }

        #endregion .: Object Blocks :.

        #region .: Force Block Queue :.

        private IList<ForcedRangeBlockQueueItem> GetForcedDateRangeBlocks(QueuedForcedBlocksRequest queuedForcedBlocksRequest)
        {
            var query = ForcedBlockQueueQueryBuilder.GetDateRangeBlocksQuery();
            return GetForcedRangeBlocks(queuedForcedBlocksRequest, query);
        }

        private IList<ForcedRangeBlockQueueItem> GetForcedNumericRangeBlocks(QueuedForcedBlocksRequest queuedForcedBlocksRequest)
        {
            var query = ForcedBlockQueueQueryBuilder.GetNumericRangeBlocksQuery();
            return GetForcedRangeBlocks(queuedForcedBlocksRequest, query);
        }

        private IList<ForcedRangeBlockQueueItem> GetForcedRangeBlocks(QueuedForcedBlocksRequest queuedForcedBlocksRequest, string query)
        {
            var results = new List<ForcedRangeBlockQueueItem>();
            var taskDefinition = _taskRepository.EnsureTaskDefinition(queuedForcedBlocksRequest.TaskId);

            try
            {
                using (var connection = CreateNewConnection(queuedForcedBlocksRequest.TaskId))
                {
                    var command = connection.CreateCommand();
                    command.CommandText = query;
                    command.CommandTimeout = ConnectionStore.Instance.GetConnection(queuedForcedBlocksRequest.TaskId).QueryTimeoutSeconds;
                    command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinition.TaskDefinitionId;
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var blockType = (BlockType)int.Parse(reader["BlockType"].ToString());
                            if (blockType == queuedForcedBlocksRequest.BlockType)
                            {
                                var blockId = reader["BlockId"].ToString();
                                var attempt = int.Parse(reader["Attempt"].ToString());
                                int forceBlockQueueId = 0;

                                long rangeBegin;
                                long rangeEnd;

                                RangeBlock rangeBlock = null;
                                if (queuedForcedBlocksRequest.BlockType == BlockType.DateRange)
                                {
                                    rangeBegin = reader.GetDateTime(1).Ticks;
                                    rangeEnd = reader.GetDateTime(2).Ticks;
                                    rangeBlock = new RangeBlock(blockId, attempt + 1, rangeBegin, rangeEnd, queuedForcedBlocksRequest.BlockType);
                                    forceBlockQueueId = reader.GetInt32(5);
                                }
                                else if (queuedForcedBlocksRequest.BlockType == BlockType.NumericRange)
                                {
                                    rangeBegin = long.Parse(reader["FromNumber"].ToString());
                                    rangeEnd = long.Parse(reader["ToNumber"].ToString());
                                    rangeBlock = new RangeBlock(blockId, attempt + 1, rangeBegin, rangeEnd, queuedForcedBlocksRequest.BlockType);
                                    forceBlockQueueId = reader.GetInt32(5);
                                }

                                var queueItem = new ForcedRangeBlockQueueItem()
                                {
                                    BlockType = queuedForcedBlocksRequest.BlockType,
                                    ForcedBlockQueueId = forceBlockQueueId,
                                    RangeBlock = rangeBlock
                                };

                                results.Add(queueItem);
                            }
                            else
                            {
                                throw new ExecutionException(@"The block type of the process does not match the block type of the queued item. 
This could occur if the block type of the process has been changed during a new development. Expected: " + queuedForcedBlocksRequest.BlockType + " but queued block is: " + blockType);
                            }
                        }
                    }
                }
            }
            catch (SqlException sqlEx)
            {
                if (TransientErrorDetector.IsTransient(sqlEx))
                    throw new TransientException("A transient exception has occurred", sqlEx);

                throw;
            }

            return results;
        }

        private IList<ForcedListBlockQueueItem> GetForcedListBlocks(QueuedForcedBlocksRequest queuedForcedBlocksRequest)
        {
            var results = new List<ForcedListBlockQueueItem>();
            var taskDefinition = _taskRepository.EnsureTaskDefinition(queuedForcedBlocksRequest.TaskId);

            try
            {
                using (var connection = CreateNewConnection(queuedForcedBlocksRequest.TaskId))
                {
                    var command = connection.CreateCommand();
                    command.CommandText = ForcedBlockQueueQueryBuilder.GetListBlocksQuery(); ;
                    command.CommandTimeout = ConnectionStore.Instance.GetConnection(queuedForcedBlocksRequest.TaskId).QueryTimeoutSeconds;
                    command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinition.TaskDefinitionId;
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var blockType = (BlockType)int.Parse(reader["BlockType"].ToString());
                            if (blockType == queuedForcedBlocksRequest.BlockType)
                            {
                                var blockId = reader["BlockId"].ToString();
                                var attempt = int.Parse(reader["Attempt"].ToString());
                                int forceBlockQueueId = reader.GetInt32(3);

                                var listBlock = new ProtoListBlock()
                                {
                                    ListBlockId = blockId,
                                    Attempt = attempt + 1,
                                    Header = SerializedValueReader.ReadValueAsString(reader, "ObjectData", "CompressedObjectData")
                                };

                                var queueItem = new ForcedListBlockQueueItem()
                                {
                                    BlockType = queuedForcedBlocksRequest.BlockType,
                                    ForcedBlockQueueId = forceBlockQueueId,
                                    ListBlock = listBlock
                                };

                                results.Add(queueItem);
                            }
                            else
                            {
                                throw new ExecutionException(@"The block type of the process does not match the block type of the queued item. 
This could occur if the block type of the process has been changed during a new development. Expected: " + queuedForcedBlocksRequest.BlockType + " but queued block is: " + blockType);
                            }
                        }
                    }
                }
            }
            catch (SqlException sqlEx)
            {
                if (TransientErrorDetector.IsTransient(sqlEx))
                    throw new TransientException("A transient exception has occurred", sqlEx);

                throw;
            }

            return results;
        }

        private IList<ForcedObjectBlockQueueItem<T>> GetForcedObjectBlocks<T>(QueuedForcedBlocksRequest queuedForcedBlocksRequest)
        {
            var results = new List<ForcedObjectBlockQueueItem<T>>();
            var taskDefinition = _taskRepository.EnsureTaskDefinition(queuedForcedBlocksRequest.TaskId);

            try
            {
                using (var connection = CreateNewConnection(queuedForcedBlocksRequest.TaskId))
                {
                    var command = connection.CreateCommand();
                    command.CommandText = ForcedBlockQueueQueryBuilder.GetObjectBlocksQuery();
                    command.CommandTimeout = ConnectionStore.Instance.GetConnection(queuedForcedBlocksRequest.TaskId).QueryTimeoutSeconds;
                    command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinition.TaskDefinitionId;
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var blockType = (BlockType)int.Parse(reader["BlockType"].ToString());
                            if (blockType == queuedForcedBlocksRequest.BlockType)
                            {
                                var blockId = reader["BlockId"].ToString();
                                var attempt = int.Parse(reader["Attempt"].ToString());
                                int forceBlockQueueId = reader.GetInt32(4);
                                T objectData = SerializedValueReader.ReadValue<T>(reader, "ObjectData", "CompressedObjectData");

                                var objectBlock = new ObjectBlock<T>()
                                {
                                    ObjectBlockId = blockId,
                                    Attempt = attempt + 1,
                                    Object = objectData
                                };

                                var queueItem = new ForcedObjectBlockQueueItem<T>()
                                {
                                    BlockType = queuedForcedBlocksRequest.BlockType,
                                    ForcedBlockQueueId = forceBlockQueueId,
                                    ObjectBlock = objectBlock
                                };

                                results.Add(queueItem);
                            }
                            else
                            {
                                throw new ExecutionException(@"The block type of the process does not match the block type of the queued item. 
This could occur if the block type of the process has been changed during a new development. Expected: " + queuedForcedBlocksRequest.BlockType + " but queued block is: " + blockType);
                            }
                        }
                    }
                }
            }
            catch (SqlException sqlEx)
            {
                if (TransientErrorDetector.IsTransient(sqlEx))
                    throw new TransientException("A transient exception has occurred", sqlEx);

                throw;
            }

            return results;
        }

        private void UpdateForcedBlocks(DequeueForcedBlocksRequest dequeueForcedBlocksRequest)
        {
            int blockCount = dequeueForcedBlocksRequest.ForcedBlockQueueIds.Count;

            try
            {
                using (var connection = CreateNewConnection(dequeueForcedBlocksRequest.TaskId))
                {
                    var command = connection.CreateCommand();
                    command.CommandText = ForcedBlockQueueQueryBuilder.GetUpdateQuery(blockCount);
                    command.CommandTimeout = ConnectionStore.Instance.GetConnection(dequeueForcedBlocksRequest.TaskId).QueryTimeoutSeconds;

                    for (int i = 0; i < blockCount; i++)
                    {
                        command.Parameters.Add("@P" + i, SqlDbType.Int).Value = int.Parse(dequeueForcedBlocksRequest.ForcedBlockQueueIds[i]);
                    }

                    command.ExecuteNonQuery();
                }
            }
            catch (SqlException sqlEx)
            {
                if (TransientErrorDetector.IsTransient(sqlEx))
                    throw new TransientException("A transient exception has occurred", sqlEx);

                throw;
            }
        }

        #endregion .: Force Block Queue :.

        private DataTable GenerateDataTable(long blockId, List<string> values, int compressionThreshold)
        {
            var dt = GenerateEmptyDataTable();

            foreach (var value in values)
            {
                var dr = dt.NewRow();
                dr["BlockId"] = blockId;

                if (value.Length > compressionThreshold)
                {
                    dr["Value"] = DBNull.Value;
                    dr["CompressedValue"] = LargeValueCompressor.Zip(value);
                }
                else
                {
                    dr["Value"] = value;
                    dr["CompressedValue"] = DBNull.Value;
                }

                dr["Status"] = (byte)ItemStatus.Pending;
                dr["LastUpdated"] = DateTime.UtcNow;
                dt.Rows.Add(dr);
            }

            return dt;
        }

        private DataTable GenerateEmptyDataTable()
        {
            var dt = new DataTable();
            dt.Columns.Add("BlockId", typeof(long));
            dt.Columns.Add("Value", typeof(string));
            dt.Columns.Add("CompressedValue", typeof(byte[]));
            dt.Columns.Add("Status", typeof(byte));
            dt.Columns.Add("LastUpdated", typeof(DateTime));

            return dt;
        }

        private DateTime EnsureSqlSafeDateTime(DateTime dateTime)
        {
            if (dateTime.Year < 1900)
                return new DateTime(1900, 1, 1);

            return dateTime;
        }

        private string AddBlockExecution(BlockExecutionCreateRequest executionCreateRequest)
        {
            string blockExecutionId = string.Empty;
            try
            {
                using (var connection = CreateNewConnection(executionCreateRequest.TaskId))
                {
                    var command = connection.CreateCommand();
                    command.CommandTimeout = ConnectionStore.Instance.GetConnection(executionCreateRequest.TaskId).QueryTimeoutSeconds;
                    command.CommandText = RangeBlockQueryBuilder.InsertBlockExecution;
                    command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = executionCreateRequest.TaskExecutionId;
                    command.Parameters.Add("@BlockId", SqlDbType.BigInt).Value = long.Parse(executionCreateRequest.BlockId);
                    command.Parameters.Add("@Attempt", SqlDbType.Int).Value = executionCreateRequest.Attempt;
                    command.Parameters.Add("@Status", SqlDbType.TinyInt).Value = (byte)BlockExecutionStatus.NotStarted;
                    blockExecutionId = command.ExecuteScalar().ToString();
                }
            }
            catch (SqlException sqlEx)
            {
                if (TransientErrorDetector.IsTransient(sqlEx))
                    throw new TransientException("A transient exception has occurred", sqlEx);

                throw;
            }

            return blockExecutionId;
        }

        private List<string> Serialize<T>(List<T> values)
        {
            var jsonValues = new List<string>();
            foreach (var value in values)
            {
                jsonValues.Add(JsonGenericSerializer.Serialize<T>(value));
            }

            return jsonValues;
        }

        #endregion .: Private Methods :.
    }
}
