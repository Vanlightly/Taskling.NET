using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.Common;
using Taskling.Blocks.ListBlocks;
using Taskling.Contexts;
using Taskling.Events;
using Taskling.SqlServer.IntegrationTest.Helpers;

namespace Taskling.SqlServer.IntegrationTest.Contexts.Given_ListBlockContext
{
    [TestClass]
    public class When_GetListBlocksFromExecutionContext
    {
        private ExecutionsHelper _executionHelper;
        private BlocksHelper _blocksHelper;
        private int _taskDefinitionId = 0;

        [TestInitialize]
        public void Initialize()
        {
            _blocksHelper = new BlocksHelper();
            _blocksHelper.DeleteBlocks(TestConstants.ApplicationName);
            _executionHelper = new ExecutionsHelper();
            _executionHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);

            _taskDefinitionId = _executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            _executionHelper.InsertAvailableExecutionToken(_taskDefinitionId);
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
                    var values = GetPersonList(9);
                    short maxBlockSize = 4;
                    var listBlocks = executionContext.GetListBlocks<PersonDto>(x => x.WithSingleUnitCommit(values, maxBlockSize));
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
                        int expectedPendingItems = listBlock.GetItems(ItemStatus.Pending).Count();
                        // All items should be Pending and 0 Completed
                        Assert.AreEqual(expectedPendingItems, _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Pending));
                        Assert.AreEqual(expectedCompletedItems, _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Completed));
                        foreach (var itemToProcess in listBlock.GetItems(ItemStatus.Pending))
                        {
                            // do the processing

                            itemToProcess.Completed();

                            // More more should be Completed
                            expectedCompletedItems++;
                            Assert.AreEqual(expectedCompletedItems, _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Completed));
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
        public void If_AsListWithSingleUnitCommitAndFailsWithReason_ThenReasonIsPersisted()
        {
            // ARRANGE

            // ACT and // ASSERT
            bool startedOk;
            string listBlockId = string.Empty;
            using (var executionContext = CreateTaskExecutionContext(1))
            {
                startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var values = GetPersonList(9);
                    short maxBlockSize = 4;
                    var listBlock = executionContext.GetListBlocks<PersonDto>(x => x.WithSingleUnitCommit(values, maxBlockSize)).First();
                    listBlockId = listBlock.Block.ListBlockId;
                    listBlock.Start();

                    int counter = 0;
                    foreach (var itemToProcess in listBlock.GetItems(ItemStatus.Pending))
                    {
                        itemToProcess.Failed("Exception");

                        counter++;
                    }

                    listBlock.Complete();
                }
            }

            Assert.IsTrue(_blocksHelper.GetListBlockItems<PersonDto>(listBlockId, ItemStatus.Failed).All(x => x.StatusReason == "Exception"));
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_LargeValues_ThenValuesArePersistedAndRetrievedOk()
        {
            // ARRANGE
            var values = GetLargePersonList(4);

            // ACT and // ASSERT
            bool startedOk;
            string listBlockId = string.Empty;
            using (var executionContext = CreateTaskExecutionContext(1))
            {
                startedOk = executionContext.TryStart();
                if (startedOk)
                {

                    short maxBlockSize = 4;
                    var listBlock = executionContext.GetListBlocks<PersonDto>(x => x.WithSingleUnitCommit(values, maxBlockSize)).First();
                    listBlockId = listBlock.Block.ListBlockId;
                    listBlock.Start();

                    foreach (var itemToProcess in listBlock.GetItems(ItemStatus.Pending))
                        itemToProcess.Failed("Exception");

                    listBlock.Complete();
                }
            }

            using (var executionContext = CreateTaskExecutionContext(1))
            {
                startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var emptyPersonList = new List<PersonDto>();
                    short maxBlockSize = 4;
                    var listBlock = executionContext.GetListBlocks<PersonDto>(x => x.WithSingleUnitCommit(emptyPersonList, maxBlockSize)).First();
                    listBlockId = listBlock.Block.ListBlockId;
                    listBlock.Start();

                    var itemsToProcess = listBlock.GetItems(ItemStatus.Pending, ItemStatus.Failed).ToList();
                    for (int i = 0; i < itemsToProcess.Count; i++)
                    {
                        Assert.AreEqual(values[i].DateOfBirth, itemsToProcess[i].Value.DateOfBirth);
                        Assert.AreEqual(values[i].Id, itemsToProcess[i].Value.Id);
                        Assert.AreEqual(values[i].Name, itemsToProcess[i].Value.Name);
                    }

                    listBlock.Complete();
                }
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AsListWithNoValues_ThenCheckpointIsPersistedAndEmptyBlockGenerated()
        {
            // ARRANGE

            // ACT and // ASSERT
            bool startedOk;
            string listBlockId = string.Empty;
            using (var executionContext = CreateTaskExecutionContext(1))
            {
                startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var values = new List<PersonDto>() { };
                    short maxBlockSize = 4;
                    var listBlock = executionContext.GetListBlocks<PersonDto>(x => x.WithSingleUnitCommit(values, maxBlockSize));
                    Assert.IsFalse(listBlock.Any());
                    var execEvent = _executionHelper.GetLastEvent(_taskDefinitionId);
                    Assert.AreEqual(EventType.CheckPoint, execEvent.Item1);
                    Assert.AreEqual("No values for generate the block. Emtpy Block context returned.", execEvent.Item2);
                }
            }
        }


        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AsListWithSingleUnitCommitAndStepSet_ThenStepIsPersisted()
        {
            // ARRANGE

            // ACT and // ASSERT
            bool startedOk;
            string listBlockId = string.Empty;
            using (var executionContext = CreateTaskExecutionContext(1))
            {
                startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var values = GetPersonList(9);
                    short maxBlockSize = 4;
                    var listBlock = executionContext.GetListBlocks<PersonDto>(x => x.WithSingleUnitCommit(values, maxBlockSize)).First();
                    listBlockId = listBlock.Block.ListBlockId;
                    listBlock.Start();

                    int counter = 0;
                    foreach (var itemToProcess in listBlock.GetItems(ItemStatus.Pending))
                    {
                        itemToProcess.Step = 2;
                        itemToProcess.Failed("Exception");

                        counter++;
                    }

                    listBlock.Complete();
                }
            }

            Assert.IsTrue(_blocksHelper.GetListBlockItems<PersonDto>(listBlockId, ItemStatus.Failed).All(x => x.StatusReason == "Exception" && x.Step == 2));
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
                    var values = GetPersonList(9);
                    short maxBlockSize = 4;
                    var listBlocks = executionContext.GetListBlocks<PersonDto>(x => x.WithBatchCommitAtEnd(values, maxBlockSize));
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

                        int expectedPendingItems = listBlock.GetItems(ItemStatus.Pending).Count();
                        // All items should be Pending and 0 Completed
                        Assert.AreEqual(expectedPendingItems, _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Pending));
                        Assert.AreEqual(0, _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Completed));
                        foreach (var itemToProcess in listBlock.GetItems(ItemStatus.Pending))
                        {
                            // do the processing

                            itemToProcess.Completed();

                            // There should be 0 Completed because we batch commit at the end
                            Assert.AreEqual(0, _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Completed));
                        }

                        listBlock.Complete();

                        // All items should be completed now
                        Assert.AreEqual(listBlock.GetItems(ItemStatus.Completed).Count(), _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Completed));

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
                    var values = GetPersonList(26);
                    short maxBlockSize = 15;
                    var listBlocks = executionContext.GetListBlocks<PersonDto>(x => x.WithPeriodicCommit(values, maxBlockSize, BatchSize.Ten));
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

                        int expectedPendingItems = listBlock.GetItems(ItemStatus.Pending).Count();
                        int expectedCompletedItems = 0;
                        // All items should be Pending and 0 Completed
                        Assert.AreEqual(expectedPendingItems, _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Pending));
                        Assert.AreEqual(expectedCompletedItems, _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Completed));
                        int itemsProcessed = 0;
                        int itemsCommitted = 0;
                        foreach (var itemToProcess in listBlock.GetItems(ItemStatus.Pending))
                        {
                            itemsProcessed++;
                            // do the processing

                            itemToProcess.Completed();

                            // There should be 0 Completed unless we have reached the batch size 10
                            if (itemsProcessed % 10 == 0)
                            {
                                Assert.AreEqual(10, _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Completed));
                                itemsCommitted += 10;
                            }
                            else
                                Assert.AreEqual(itemsCommitted, _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Completed));
                        }


                        listBlock.Complete();

                        // All items should be completed now
                        Assert.AreEqual(itemsProcessed, _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Completed));

                        // One more block should be completed
                        expectedCompletedCount++;
                        Assert.AreEqual(expectedCompletedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));
                    }
                }
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AsListWithPeriodicCommitAndFailsWithReason_ThenReasonIsPersisted()
        {
            // ARRANGE

            // ACT and // ASSERT
            bool startedOk;
            string listBlockId = string.Empty;
            using (var executionContext = CreateTaskExecutionContext(1))
            {
                startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var values = GetPersonList(14);
                    short maxBlockSize = 20;
                    var listBlock = executionContext.GetListBlocks<PersonDto>(x => x.WithPeriodicCommit(values, maxBlockSize, BatchSize.Ten)).First();
                    listBlockId = listBlock.Block.ListBlockId;
                    listBlock.Start();

                    int counter = 0;
                    foreach (var itemToProcess in listBlock.GetItems(ItemStatus.Pending))
                    {
                        itemToProcess.Failed("Exception");

                        counter++;
                    }

                    listBlock.Complete();
                }
            }

            Assert.IsTrue(_blocksHelper.GetListBlockItems<PersonDto>(listBlockId, ItemStatus.Failed).All(x => x.StatusReason == "Exception"));
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AsListWithPeriodicCommitAndStepSet_ThenStepIsPersisted()
        {
            // ARRANGE

            // ACT and // ASSERT
            bool startedOk;
            string listBlockId = string.Empty;
            using (var executionContext = CreateTaskExecutionContext(1))
            {
                startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var values = GetPersonList(14);
                    short maxBlockSize = 20;
                    var listBlock = executionContext.GetListBlocks<PersonDto>(x => x.WithPeriodicCommit(values, maxBlockSize, BatchSize.Ten)).First();
                    listBlockId = listBlock.Block.ListBlockId;
                    listBlock.Start();

                    int counter = 0;
                    foreach (var itemToProcess in listBlock.GetItems(ItemStatus.Pending))
                    {
                        itemToProcess.Step = 2;
                        itemToProcess.Failed("Exception");

                        counter++;
                    }

                    listBlock.Complete();
                }
            }

            var listBlockItems = _blocksHelper.GetListBlockItems<PersonDto>(listBlockId, ItemStatus.Failed);
            Assert.IsTrue(listBlockItems.All(x => x.StatusReason == "Exception" && x.Step == 2));
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
                    var values = GetPersonList(26);
                    short maxBlockSize = 15;
                    var listBlocks = executionContext.GetListBlocks<PersonDto>(x => x.WithPeriodicCommit(values, maxBlockSize, BatchSize.Ten));

                    foreach (var listBlock in listBlocks)
                    {
                        listBlock.Start();
                        foreach (var itemToProcess in listBlock.GetItems(ItemStatus.Pending))
                            itemToProcess.Completed();

                        listBlock.Complete();
                    }
                }
            }

            var expectedPeople = GetPersonList(11, 15);
            var expectedLastBlock = new ListBlock<PersonDto>();
            foreach (var person in expectedPeople)
                expectedLastBlock.Items.Add(new ListBlockItem<PersonDto>() { Value = person });


            // ACT
            IListBlock<PersonDto> lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    lastBlock = executionContext.GetLastListBlock<PersonDto>();
                }
            }

            // ASSERT
            Assert.AreEqual(expectedLastBlock.Items.Count, lastBlock.Items.Count);
            Assert.AreEqual(expectedLastBlock.Items[0].Value.Id, lastBlock.Items[0].Value.Id);
            Assert.AreEqual(expectedLastBlock.Items[1].Value.Id, lastBlock.Items[1].Value.Id);
            Assert.AreEqual(expectedLastBlock.Items[2].Value.Id, lastBlock.Items[2].Value.Id);
            Assert.AreEqual(expectedLastBlock.Items[3].Value.Id, lastBlock.Items[3].Value.Id);
            Assert.AreEqual(expectedLastBlock.Items[4].Value.Id, lastBlock.Items[4].Value.Id);
            Assert.AreEqual(expectedLastBlock.Items[5].Value.Id, lastBlock.Items[5].Value.Id);
            Assert.AreEqual(expectedLastBlock.Items[6].Value.Id, lastBlock.Items[6].Value.Id);
            Assert.AreEqual(expectedLastBlock.Items[7].Value.Id, lastBlock.Items[7].Value.Id);
            Assert.AreEqual(expectedLastBlock.Items[8].Value.Id, lastBlock.Items[8].Value.Id);
            Assert.AreEqual(expectedLastBlock.Items[9].Value.Id, lastBlock.Items[9].Value.Id);
            Assert.AreEqual(expectedLastBlock.Items[10].Value.Id, lastBlock.Items[10].Value.Id);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_NoPreviousBlock_ThenLastBlockIsNull()
        {
            // ARRANGE
            // all previous blocks were deleted in TestInitialize

            // ACT
            IListBlock<PersonDto> lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    lastBlock = executionContext.GetLastListBlock<PersonDto>();
                }
            }

            // ASSERT
            Assert.IsNull(lastBlock);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_PreviousBlockIsPhantom_ThenLastBlockNotThisPhantom()
        {
            // ARRANGE
            // Create previous blocks
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var values = GetPersonList(3);
                    short maxBlockSize = 15;
                    var listBlocks = executionContext.GetListBlocks<PersonDto>(x => x.WithPeriodicCommit(values, maxBlockSize, BatchSize.Ten));

                    foreach (var listBlock in listBlocks)
                    {
                        listBlock.Start();
                        foreach (var itemToProcess in listBlock.GetItems(ItemStatus.Pending))
                            itemToProcess.Completed();

                        listBlock.Complete();
                    }
                }
            }

            _blocksHelper.InsertPhantomListBlock(TestConstants.ApplicationName, TestConstants.TaskName);

            // ACT
            IListBlock<PersonDto> lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    lastBlock = executionContext.GetLastListBlock<PersonDto>();
                }
            }

            // ASSERT
            Assert.AreEqual(3, lastBlock.Items.Count);
            Assert.AreEqual("1", lastBlock.Items[0].Value.Id);
            Assert.AreEqual("2", lastBlock.Items[1].Value.Id);
            Assert.AreEqual("3", lastBlock.Items[2].Value.Id);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AnItemFails_ThenCompleteSetsStatusAsFailed()
        {
            // ARRANGE

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext(1))
            {
                startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var values = GetPersonList(9);
                    short maxBlockSize = 4;
                    var listBlock = executionContext.GetListBlocks<PersonDto>(x => x.WithSingleUnitCommit(values, maxBlockSize)).First();

                    listBlock.Start();

                    int counter = 0;
                    foreach (var itemToProcess in listBlock.GetItems(ItemStatus.Pending))
                    {
                        if (counter == 2)
                            itemToProcess.Failed("Exception");
                        else
                            itemToProcess.Completed();

                        counter++;
                    }

                    listBlock.Complete();
                }
            }

            Assert.AreEqual(1, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Failed));
            Assert.AreEqual(0, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_ReprocessingSpecificExecutionAndItExistsWithMultipleExecutionsAndOnlyOneFailed_ThenBringBackOnFailedBlockWhenRequested()
        {
            string referenceValue = Guid.NewGuid().ToString();
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = executionContext.TryStart(referenceValue);
                if (startedOk)
                {
                    var values = GetPersonList(9);
                    short maxBlockSize = 3;
                    var listBlocks = executionContext.GetListBlocks<PersonDto>(x => x.WithPeriodicCommit(values, maxBlockSize, BatchSize.Fifty)).ToList();

                    // block 0 has one failed item
                    listBlocks[0].Start();

                    int counter = 0;
                    foreach (var itemToProcess in listBlocks[0].GetItems(ItemStatus.Pending))
                    {
                        if (counter == 2)
                            listBlocks[0].ItemFailed(itemToProcess, "Exception", 1);
                        else
                            listBlocks[0].ItemComplete(itemToProcess);

                        counter++;
                    }

                    listBlocks[0].Complete();

                    // block 1 succeeds
                    listBlocks[1].Start();

                    foreach (var itemToProcess in listBlocks[1].GetItems(ItemStatus.Pending))
                    {
                        listBlocks[1].ItemComplete(itemToProcess);

                        counter++;
                    }

                    listBlocks[1].Complete();

                    // block 2 never starts
                }
            }

            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var listBlocksToReprocess = executionContext.GetListBlocks<PersonDto>(x => x.ReprocessWithPeriodicCommit(BatchSize.Fifty)
                                                                .PendingAndFailedBlocks()
                                                                .OfExecutionWith(referenceValue)).ToList();

                    // one failed and one block never started
                    Assert.AreEqual(2, listBlocksToReprocess.Count);

                    // the block that failed has one failed item
                    var itemsOfB1 = listBlocksToReprocess[0].GetItems(ItemStatus.Failed, ItemStatus.Pending).ToList();
                    Assert.AreEqual(1, itemsOfB1.Count);
                    Assert.AreEqual("Exception", itemsOfB1[0].StatusReason);
                    byte expectedStep = 1;
                    Assert.AreEqual(expectedStep, itemsOfB1[0].Step);

                    // the block that never executed has 3 pending items
                    var itemsOfB2 = listBlocksToReprocess[1].GetItems(ItemStatus.Failed, ItemStatus.Pending);
                    Assert.AreEqual(3, itemsOfB2.Count());

                    listBlocksToReprocess[0].Complete();
                }
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AsListWithOverridenConfiguration_ThenOverridenValuesAreUsed()
        {
            // ARRANGE
            CreateFailedTask();
            CreateDeadTask();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                startedOk = executionContext.TryStart();
                Assert.AreEqual(true, startedOk);
                if (startedOk)
                {
                    var values = GetPersonList(8);
                    short maxBlockSize = 4;
                    var listBlocks = executionContext.GetListBlocks<PersonDto>(x => x.WithSingleUnitCommit(values, maxBlockSize)
                                                                .OverrideConfiguration()
                                                                .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                                                                .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                                                                .MaximumBlocksToGenerate(5));
                    // There should be 5 blocks - 3, 3, 3, 3, 4
                    Assert.AreEqual(5, _blocksHelper.GetBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));
                    Assert.IsTrue(listBlocks[0].GetItems().All(x => x.Status == ItemStatus.Failed));
                    Assert.AreEqual(3, listBlocks[0].GetItems().Count());
                    Assert.IsTrue(listBlocks[1].GetItems().All(x => x.Status == ItemStatus.Failed));
                    Assert.AreEqual(3, listBlocks[1].GetItems().Count());
                    Assert.IsTrue(listBlocks[2].GetItems().All(x => x.Status == ItemStatus.Pending));
                    Assert.AreEqual(3, listBlocks[2].GetItems().Count());
                    Assert.IsTrue(listBlocks[3].GetItems().All(x => x.Status == ItemStatus.Pending));
                    Assert.AreEqual(3, listBlocks[3].GetItems().Count());
                    Assert.IsTrue(listBlocks[4].GetItems().All(x => x.Status == ItemStatus.Pending));
                    Assert.AreEqual(4, listBlocks[4].GetItems().Count());
                }
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AsListWithNoOverridenConfiguration_ThenConfigurationValuesAreUsed()
        {
            // ARRANGE
            CreateFailedTask();
            CreateDeadTask();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                startedOk = executionContext.TryStart();
                Assert.AreEqual(true, startedOk);
                if (startedOk)
                {
                    var values = GetPersonList(8);
                    short maxBlockSize = 4;
                    var listBlocks = executionContext.GetListBlocks<PersonDto>(x => x.WithSingleUnitCommit(values, maxBlockSize));
                    // There should be 2 blocks - 4, 4
                    Assert.AreEqual(2, listBlocks.Count);
                    Assert.IsTrue(listBlocks[0].GetItems().All(x => x.Status == ItemStatus.Pending));
                    Assert.AreEqual(4, listBlocks[0].GetItems().Count());
                    Assert.IsTrue(listBlocks[1].GetItems().All(x => x.Status == ItemStatus.Pending));
                    Assert.AreEqual(4, listBlocks[1].GetItems().Count());
                }
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AsList_ThenReturnsBlockInOrderOfBlockId()
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
                    var values = GetPersonList(10);
                    short maxBlockSize = 1;
                    var listBlocks = executionContext.GetListBlocks<PersonDto>(x => x.WithSingleUnitCommit(values, maxBlockSize));

                    int counter = 0;
                    int lastId = 0;
                    foreach (var listBlock in listBlocks)
                    {
                        listBlock.Start();

                        int currentId = int.Parse(listBlock.Block.ListBlockId);
                        if (counter > 0)
                        {
                            Assert.AreEqual(currentId, lastId + 1);
                        }

                        lastId = currentId;

                        listBlock.Complete();
                    }
                }
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_ForceBlock_ThenBlockGetsReprocessedAndDequeued()
        {
            // ARRANGE
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var values = GetPersonList(3);
                    short maxBlockSize = 15;
                    var listBlocks = executionContext.GetListBlocks<PersonDto>(x => x.WithPeriodicCommit(values, maxBlockSize, BatchSize.Ten));

                    foreach (var listBlock in listBlocks)
                    {
                        listBlock.Start();
                        foreach (var itemToProcess in listBlock.GetItems(ItemStatus.Pending))
                            listBlock.ItemComplete(itemToProcess);

                        listBlock.Complete();
                    }
                }
            }

            // add this processed block to the forced queue
            var lastBlockId = _blocksHelper.GetLastBlockId(TestConstants.ApplicationName, TestConstants.TaskName);
            _blocksHelper.EnqueueForcedBlock(lastBlockId);

            // ACT - reprocess the forced block
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var listBlocks = executionContext.GetListBlocks<PersonDto>(x => x.WithBatchCommitAtEnd(new List<PersonDto>(), 10));
                    Assert.AreEqual(1, listBlocks.Count);

                    var items = listBlocks[0].GetItems().ToList();
                    Assert.AreEqual(3, items.Count());
                    Assert.AreEqual("1", items[0].Value.Id);
                    Assert.AreEqual("2", items[1].Value.Id);
                    Assert.AreEqual("3", items[2].Value.Id);
                    foreach (var listBlock in listBlocks)
                    {
                        listBlock.Start();

                        foreach (var item in listBlock.GetItems())
                            listBlock.ItemComplete(item);

                        listBlock.Complete();
                    }
                }
            }

            // The forced block will have been dequeued so it should not be processed again
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var items = new List<PersonDto>();
                    var listBlocks = executionContext.GetListBlocks<PersonDto>(x => x.WithSingleUnitCommit(items, 50));
                    Assert.AreEqual(0, listBlocks.Count);
                }
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_BlockItemsAccessedBeforeGetItemsCalled_ThenItemsAreLoadedOkAnyway()
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
                    var values = GetPersonList(10);
                    short maxBlockSize = 1;
                    var listBlocks = executionContext.GetListBlocks<PersonDto>(x => x.WithSingleUnitCommit(values, maxBlockSize));

                    foreach (var listBlock in listBlocks)
                    {
                        listBlock.Start();

                        var itemsToProcess = listBlock.Block.Items;
                        foreach (var item in itemsToProcess)
                            item.Completed();

                        listBlock.Complete();
                    }
                }
            }
        }

        private void CreateFailedTask()
        {
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var values = GetPersonList(6);
                    short maxBlockSize = 3;
                    var listBlocks = executionContext.GetListBlocks<PersonDto>(x => x.WithPeriodicCommit(values, maxBlockSize, BatchSize.Ten));

                    foreach (var listBlock in listBlocks)
                    {
                        listBlock.Start();
                        foreach (var itemToProcess in listBlock.GetItems(ItemStatus.Pending))
                            listBlock.ItemFailed(itemToProcess, "Exception");

                        listBlock.Failed("Something bad happened");
                    }
                }
            }
        }

        private void CreateDeadTask()
        {
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var values = GetPersonList(6);
                    short maxBlockSize = 3;
                    var listBlocks = executionContext.GetListBlocks<PersonDto>(x => x.WithPeriodicCommit(values, maxBlockSize, BatchSize.Ten));

                    foreach (var listBlock in listBlocks)
                    {
                        listBlock.Start();
                    }
                }
            }

            var executionHelper = new ExecutionsHelper();
            executionHelper.SetLastExecutionAsDead(_taskDefinitionId);
        }

        private ITaskExecutionContext CreateTaskExecutionContext(int maxBlocksToGenerate = 10)
        {
            return ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing(maxBlocksToGenerate));
        }

        private ITaskExecutionContext CreateTaskExecutionContextWithNoReprocessing()
        {
            return ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndNoReprocessing());
        }

        private List<PersonDto> GetPersonList(int count, int skip = 0)
        {
            var people = new List<PersonDto>() {
                        new PersonDto() { DateOfBirth = new DateTime(1980, 1, 1), Id = "1", Name = "Terrence" },
                        new PersonDto() { DateOfBirth = new DateTime(1981, 1, 1), Id = "2", Name = "Boris" },
                        new PersonDto() { DateOfBirth = new DateTime(1982, 1, 1), Id = "3", Name = "Bob" },
                        new PersonDto() { DateOfBirth = new DateTime(1983, 1, 1), Id = "4", Name = "Jane" },
                        new PersonDto() { DateOfBirth = new DateTime(1984, 1, 1), Id = "5", Name = "Rachel" },
                        new PersonDto() { DateOfBirth = new DateTime(1985, 1, 1), Id = "6", Name = "Sarah" },
                        new PersonDto() { DateOfBirth = new DateTime(1986, 1, 1), Id = "7", Name = "Brad" },
                        new PersonDto() { DateOfBirth = new DateTime(1987, 1, 1), Id = "8", Name = "Phillip" },
                        new PersonDto() { DateOfBirth = new DateTime(1988, 1, 1), Id = "9", Name = "Cory" },
                        new PersonDto() { DateOfBirth = new DateTime(1989, 1, 1), Id = "10", Name = "Burt" },
                        new PersonDto() { DateOfBirth = new DateTime(1990, 1, 1), Id = "11", Name = "Gladis" },
                        new PersonDto() { DateOfBirth = new DateTime(1991, 1, 1), Id = "12", Name = "Ethel" },

                        new PersonDto() { DateOfBirth = new DateTime(1992, 1, 1), Id = "13", Name = "Terry" },
                        new PersonDto() { DateOfBirth = new DateTime(1993, 1, 1), Id = "14", Name = "Bernie" },
                        new PersonDto() { DateOfBirth = new DateTime(1994, 1, 1), Id = "15", Name = "Will" },
                        new PersonDto() { DateOfBirth = new DateTime(1995, 1, 1), Id = "16", Name = "Jim" },
                        new PersonDto() { DateOfBirth = new DateTime(1996, 1, 1), Id = "17", Name = "Eva" },
                        new PersonDto() { DateOfBirth = new DateTime(1997, 1, 1), Id = "18", Name = "Susan" },
                        new PersonDto() { DateOfBirth = new DateTime(1998, 1, 1), Id = "19", Name = "Justin" },
                        new PersonDto() { DateOfBirth = new DateTime(1999, 1, 1), Id = "20", Name = "Gerry" },
                        new PersonDto() { DateOfBirth = new DateTime(2000, 1, 1), Id = "21", Name = "Fitz" },
                        new PersonDto() { DateOfBirth = new DateTime(2001, 1, 1), Id = "22", Name = "Ellie" },
                        new PersonDto() { DateOfBirth = new DateTime(2002, 1, 1), Id = "23", Name = "Gordon" },
                        new PersonDto() { DateOfBirth = new DateTime(2003, 1, 1), Id = "24", Name = "Gail" },
                        new PersonDto() { DateOfBirth = new DateTime(2004, 1, 1), Id = "25", Name = "Gary" },
                        new PersonDto() { DateOfBirth = new DateTime(2005, 1, 1), Id = "26", Name = "Gabby" }
                    };

            return people.Skip(skip).Take(count).ToList();
        }

        private List<PersonDto> GetLargePersonList(int count, int skip = 0)
        {
            var people = new List<PersonDto>() {
                        new PersonDto() { DateOfBirth = new DateTime(1980, 1, 1), Id = "1", Name = GetLongName("Terrence") },
                        new PersonDto() { DateOfBirth = new DateTime(1981, 1, 1), Id = "2", Name = GetLongName("Boris") },
                        new PersonDto() { DateOfBirth = new DateTime(1982, 1, 1), Id = "3", Name = GetLongName("Bob") },
                        new PersonDto() { DateOfBirth = new DateTime(1983, 1, 1), Id = "4", Name = GetLongName("Jane") },
                        new PersonDto() { DateOfBirth = new DateTime(1984, 1, 1), Id = "5", Name = GetLongName("Rachel") },
                        new PersonDto() { DateOfBirth = new DateTime(1985, 1, 1), Id = "6", Name = GetLongName("Sarah") },
                        new PersonDto() { DateOfBirth = new DateTime(1986, 1, 1), Id = "7", Name = GetLongName("Brad") },
                        new PersonDto() { DateOfBirth = new DateTime(1987, 1, 1), Id = "8", Name = GetLongName("Phillip") },
                        new PersonDto() { DateOfBirth = new DateTime(1988, 1, 1), Id = "9", Name = GetLongName("Cory") },
                        new PersonDto() { DateOfBirth = new DateTime(1989, 1, 1), Id = "10", Name = GetLongName("Burt") },
                        new PersonDto() { DateOfBirth = new DateTime(1990, 1, 1), Id = "11", Name = GetLongName("Gladis") },
                        new PersonDto() { DateOfBirth = new DateTime(1991, 1, 1), Id = "12", Name = GetLongName("Ethel") },

                        new PersonDto() { DateOfBirth = new DateTime(1992, 1, 1), Id = "13", Name = GetLongName("Terry") },
                        new PersonDto() { DateOfBirth = new DateTime(1993, 1, 1), Id = "14", Name = GetLongName("Bernie") },
                        new PersonDto() { DateOfBirth = new DateTime(1994, 1, 1), Id = "15", Name = GetLongName("Will") },
                        new PersonDto() { DateOfBirth = new DateTime(1995, 1, 1), Id = "16", Name = GetLongName("Jim") },
                        new PersonDto() { DateOfBirth = new DateTime(1996, 1, 1), Id = "17", Name = GetLongName("Eva") },
                        new PersonDto() { DateOfBirth = new DateTime(1997, 1, 1), Id = "18", Name = GetLongName("Susan") },
                        new PersonDto() { DateOfBirth = new DateTime(1998, 1, 1), Id = "19", Name = GetLongName("Justin") },
                        new PersonDto() { DateOfBirth = new DateTime(1999, 1, 1), Id = "20", Name = GetLongName("Gerry") },
                        new PersonDto() { DateOfBirth = new DateTime(2000, 1, 1), Id = "21", Name = GetLongName("Fitz") },
                        new PersonDto() { DateOfBirth = new DateTime(2001, 1, 1), Id = "22", Name = GetLongName("Ellie") },
                        new PersonDto() { DateOfBirth = new DateTime(2002, 1, 1), Id = "23", Name = GetLongName("Gordon") },
                        new PersonDto() { DateOfBirth = new DateTime(2003, 1, 1), Id = "24", Name = GetLongName("Gail") },
                        new PersonDto() { DateOfBirth = new DateTime(2004, 1, 1), Id = "25", Name = GetLongName("Gary") },
                        new PersonDto() { DateOfBirth = new DateTime(2005, 1, 1), Id = "26", Name = GetLongName("Gabby") }
                    };

            return people.Skip(skip).Take(count).ToList();
        }

        public string GetLongName(string name)
        {
            var sb = new StringBuilder();
            for (int i = 0; i < 1000; i++)
                sb.Append(" " + name);

            return sb.ToString();
        }
    }
}
