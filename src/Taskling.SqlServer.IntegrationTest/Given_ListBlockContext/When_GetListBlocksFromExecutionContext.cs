using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Taskling.Blocks;
using Taskling.ExecutionContext;
using Taskling.ExecutionContext.FluentBlocks.List;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.SqlServer.Configuration;
using Taskling.SqlServer.IntegrationTest.TestHelpers;
using Taskling.SqlServer.TaskExecution;

namespace Taskling.SqlServer.IntegrationTest.Given_ListBlockContext
{
    [TestClass]
    public class When_GetListBlocksFromExecutionContext
    {
        private ExecutionsHelper _executionHelper;
        private BlocksHelper _blocksHelper;

        [TestInitialize]
        public void Initialize()
        {
            _blocksHelper = new BlocksHelper();
            _blocksHelper.DeleteBlocks(TestConstants.ApplicationName, TestConstants.TaskName);
            _executionHelper = new ExecutionsHelper();
            _executionHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);

            var taskDefinitionId = _executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            _executionHelper.InsertExecutionToken(taskDefinitionId, TaskExecutionStatus.Available, "0");
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AsListWithSingleUnitCommit_NumberOfBlocksAndStatusesOfBlockExecutionsCorrectAtEveryStep()
        {
            // ARRANGE
            
            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = executionContext.TryStart();
                Assert.AreEqual(true, startedOk);
                if (startedOk)
                {
                    var values = new List<string>() {"A", "B", "C", "D", "E", "F", "G", "H", "I"};
                    short maxBlockSize = 4;
                    var listBlocks = executionContext.GetListBlocks(x => x.WithSingleUnitCommit(values, maxBlockSize)
                                                                .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0))
                                                                .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), new TimeSpan(0, 1, 0, 0))
                                                                .MaximumBlocksToGenerate(10));
                    // There should be 3 blocks - 4, 4, 1
                    Assert.AreEqual(3, _blocksHelper.GetBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));
                    int expectedNotStartedCount = 3;
                    int expectedCompletedCount = 0;

                    // All three should be registered as not started
                    Assert.AreEqual(expectedNotStartedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
                    Assert.AreEqual(0, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));
                    Assert.AreEqual(expectedCompletedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));

                    foreach (var listBlock in listBlocks)
                    {
                        listBlock.Start();
                        expectedNotStartedCount--;
                        
                        // There should be one less NotStarted block and exactly 1 Started block
                        Assert.AreEqual(expectedNotStartedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
                        Assert.AreEqual(1, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));

                        int expectedCompletedItems = 0;
                        int expectedPendingItems = listBlock.GetAllItems().Count();
                        // All items should be Pending and 0 Completed
                        Assert.AreEqual(expectedPendingItems, _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ListBlockItemStatus.Pending));
                        Assert.AreEqual(expectedCompletedItems, _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ListBlockItemStatus.Completed));
                        foreach (var itemToProcess in listBlock.GetAllItems())
                        {
                            // do the processing

                            listBlock.ItemComplete(itemToProcess);

                            // More more should be Completed
                            expectedCompletedItems++;
                            Assert.AreEqual(expectedCompletedItems, _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ListBlockItemStatus.Completed));
                        }

                        listBlock.Complete();
                        
                        // One more block should be completed
                        expectedCompletedCount++;
                        Assert.AreEqual(expectedCompletedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));
                    }
                }
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AsListWithBatchCommitAtEnd_NumberOfBlocksAndStatusesOfBlockExecutionsCorrectAtEveryStep()
        {
            // ARRANGE
            
            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = executionContext.TryStart();
                Assert.AreEqual(true, startedOk);
                if (startedOk)
                {
                    var values = new List<string>() { "A", "B", "C", "D", "E", "F", "G", "H", "I" };
                    short maxBlockSize = 4;
                    var listBlocks = executionContext.GetListBlocks(x => x.WithBatchCommitAtEnd(values, maxBlockSize)
                                                                .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0))
                                                                .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), new TimeSpan(0, 1, 0, 0))
                                                                .MaximumBlocksToGenerate(10));
                    // There should be 3 blocks - 4, 4, 1
                    Assert.AreEqual(3, _blocksHelper.GetBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));
                    int expectedNotStartedCount = 3;
                    int expectedCompletedCount = 0;

                    // All three should be registered as not started
                    Assert.AreEqual(expectedNotStartedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
                    Assert.AreEqual(0, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));
                    Assert.AreEqual(expectedCompletedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));

                    foreach (var listBlock in listBlocks)
                    {
                        listBlock.Start();
                        expectedNotStartedCount--;

                        // There should be one less NotStarted block and exactly 1 Started block
                        Assert.AreEqual(expectedNotStartedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
                        Assert.AreEqual(1, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));

                        int expectedPendingItems = listBlock.GetAllItems().Count();
                        // All items should be Pending and 0 Completed
                        Assert.AreEqual(expectedPendingItems, _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ListBlockItemStatus.Pending));
                        Assert.AreEqual(0, _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ListBlockItemStatus.Completed));
                        foreach (var itemToProcess in listBlock.GetAllItems())
                        {
                            // do the processing

                            listBlock.ItemComplete(itemToProcess);

                            // There should be 0 Completed because we batch commit at the end
                            Assert.AreEqual(0, _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ListBlockItemStatus.Completed));
                        }

                        listBlock.Complete();
                        
                        // All items should be completed now
                        Assert.AreEqual(listBlock.GetAllItems().Count(), _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ListBlockItemStatus.Completed));

                        // One more block should be completed
                        expectedCompletedCount++;
                        Assert.AreEqual(expectedCompletedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));
                    }
                }
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AsListWithPeriodicCommit_NumberOfBlocksAndStatusesOfBlockExecutionsCorrectAtEveryStep()
        {
            // ARRANGE
            
            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = executionContext.TryStart();
                Assert.AreEqual(true, startedOk);
                if (startedOk)
                {
                    var values = new List<string>() { "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z" };
                    short maxBlockSize = 15;
                    var listBlocks = executionContext.GetListBlocks(x => x.WithPeriodicCommit(values, maxBlockSize, BatchSize.Ten)
                                                                .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0))
                                                                .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), new TimeSpan(0, 1, 0, 0))
                                                                .MaximumBlocksToGenerate(10));
                    // There should be 2 blocks - 15, 11
                    Assert.AreEqual(2, _blocksHelper.GetBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));
                    int expectedNotStartedCount = 2;
                    int expectedCompletedCount = 0;

                    // All three should be registered as not started
                    Assert.AreEqual(expectedNotStartedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
                    Assert.AreEqual(0, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));
                    Assert.AreEqual(expectedCompletedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));

                    foreach (var listBlock in listBlocks)
                    {
                        listBlock.Start();
                        expectedNotStartedCount--;

                        // There should be one less NotStarted block and exactly 1 Started block
                        Assert.AreEqual(expectedNotStartedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
                        Assert.AreEqual(1, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));

                        int expectedPendingItems = listBlock.GetAllItems().Count();
                        int expectedCompletedItems = 0;
                        // All items should be Pending and 0 Completed
                        Assert.AreEqual(expectedPendingItems, _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ListBlockItemStatus.Pending));
                        Assert.AreEqual(expectedCompletedItems, _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ListBlockItemStatus.Completed));
                        int itemsProcessed = 0;
                        int itemsCommitted = 0;
                        foreach (var itemToProcess in listBlock.GetAllItems())
                        {
                            itemsProcessed++;
                            // do the processing

                            listBlock.ItemComplete(itemToProcess);

                            // There should be 0 Completed unless we have reached the batch size 10
                            if(itemsProcessed % 10 == 0)
                            {
                                Assert.AreEqual(10, _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ListBlockItemStatus.Completed));
                                itemsCommitted += 10;
                            }
                            else
                                Assert.AreEqual(itemsCommitted, _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ListBlockItemStatus.Completed));
                        }
                        

                        listBlock.Complete();
                        
                        // All items should be completed now
                        Assert.AreEqual(itemsProcessed, _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ListBlockItemStatus.Completed));

                        // One more block should be completed
                        expectedCompletedCount++;
                        Assert.AreEqual(expectedCompletedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));
                    }
                }
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_PreviousBlock_ThenLastBlockContainsCorrectItems()
        {
            // ARRANGE
            // Create previous blocks
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var values = new List<string>() { "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z" };
                    short maxBlockSize = 15;
                    var listBlocks = executionContext.GetListBlocks(x => x.WithPeriodicCommit(values, maxBlockSize, BatchSize.Ten)
                                                                .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0))
                                                                .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), new TimeSpan(0, 1, 0, 0))
                                                                .MaximumBlocksToGenerate(10));

                    foreach (var listBlock in listBlocks)
                    {
                        listBlock.Start();
                        foreach (var itemToProcess in listBlock.GetAllItems())
                            listBlock.ItemComplete(itemToProcess);

                        listBlock.Complete();
                    }
                }
            }

            var expectedLastBlock = new ListBlock()
            {
                Items = new List<ListBlockItem>()
                {
                    new ListBlockItem() { Value = "P" }, 
                    new ListBlockItem() { Value = "Q" }, 
                    new ListBlockItem() { Value = "R" },
                    new ListBlockItem() { Value = "S" },
                    new ListBlockItem() { Value = "T" },
                    new ListBlockItem() { Value = "U" },
                    new ListBlockItem() { Value = "V" },
                    new ListBlockItem() { Value = "W" },
                    new ListBlockItem() { Value = "X" },
                    new ListBlockItem() { Value = "Y" },
                    new ListBlockItem() { Value = "Z" }
                }
            };


            // ACT
            ListBlock lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    lastBlock = executionContext.GetLastListBlock();
                }
            }

            // ASSERT
            Assert.AreEqual(expectedLastBlock.Items.Count, lastBlock.Items.Count);
            Assert.AreEqual(expectedLastBlock.Items[0].Value, lastBlock.Items[0].Value);
            Assert.AreEqual(expectedLastBlock.Items[1].Value, lastBlock.Items[1].Value);
            Assert.AreEqual(expectedLastBlock.Items[2].Value, lastBlock.Items[2].Value);
            Assert.AreEqual(expectedLastBlock.Items[3].Value, lastBlock.Items[3].Value);
            Assert.AreEqual(expectedLastBlock.Items[4].Value, lastBlock.Items[4].Value);
            Assert.AreEqual(expectedLastBlock.Items[5].Value, lastBlock.Items[5].Value);
            Assert.AreEqual(expectedLastBlock.Items[6].Value, lastBlock.Items[6].Value);
            Assert.AreEqual(expectedLastBlock.Items[7].Value, lastBlock.Items[7].Value);
            Assert.AreEqual(expectedLastBlock.Items[8].Value, lastBlock.Items[8].Value);
            Assert.AreEqual(expectedLastBlock.Items[9].Value, lastBlock.Items[9].Value);
            Assert.AreEqual(expectedLastBlock.Items[10].Value, lastBlock.Items[10].Value);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_NoPreviousBlock_ThenLastBlockIsEmpty()
        {
            // ARRANGE
            // all previous blocks were deleted in TestInitialize

            // ACT
            ListBlock lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    lastBlock = executionContext.GetLastListBlock();
                }
            }

            // ASSERT
            Assert.AreEqual(0, lastBlock.Items.Count);
        }

        private ITaskExecutionContext CreateTaskExecutionContext()
        {
            var settings = new SqlServerClientConnectionSettings()
            {
                ConnectionString = TestConstants.TestConnectionString,
                ConnectTimeout = new TimeSpan(0, 0, 1, 0)
            };

            var taskExecutionOptions = new TaskExecutionOptions()
            {
                TaskDeathMode = TaskDeathMode.KeepAlive,
                KeepAliveInterval = new TimeSpan(0, 0, 0, 30),
                KeepAliveDeathThreshold = new TimeSpan(0, 0, 2, 0)
            };

            var client = new SqlServerTasklingClient(settings);
            return client.CreateTaskExecutionContext(TestConstants.ApplicationName, TestConstants.TaskName, taskExecutionOptions);
        }
    }
}
