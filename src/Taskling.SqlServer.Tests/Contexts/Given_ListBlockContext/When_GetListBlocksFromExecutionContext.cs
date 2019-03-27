using Xunit;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.Common;
using Taskling.Blocks.ListBlocks;
using Taskling.Contexts;
using Taskling.Events;
using Taskling.SqlServer.Tests.Helpers;
using System.Threading.Tasks;
using System.Threading;

namespace Taskling.SqlServer.Tests.Contexts.Given_ListBlockContext
{
    public class When_GetListBlocksFromExecutionContext
    {
        private ExecutionsHelper _executionHelper;
        private BlocksHelper _blocksHelper;
        private int _taskDefinitionId = 0;

        public When_GetListBlocksFromExecutionContext()
        {
            _blocksHelper = new BlocksHelper();
            _blocksHelper.DeleteBlocks(TestConstants.ApplicationName);
            _executionHelper = new ExecutionsHelper();
            _executionHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);

            _taskDefinitionId = _executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            _executionHelper.InsertAvailableExecutionToken(_taskDefinitionId);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_AsListWithSingleUnitCommit_NumberOfBlocksAndStatusesOfBlockExecutionsCorrectAtEveryStep()
        {
            // ARRANGE
            
            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var values = GetPersonList(9);
                    short maxBlockSize = 4;
                    var listBlocks = await executionContext.GetListBlocksAsync<PersonDto>(x => x.WithSingleUnitCommit(values, maxBlockSize));
                    // There should be 3 blocks - 4, 4, 1
                    Assert.Equal(3, _blocksHelper.GetBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));
                    int expectedNotStartedCount = 3;
                    int expectedCompletedCount = 0;

                    // All three should be registered as not started
                    Assert.Equal(expectedNotStartedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
                    Assert.Equal(0, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));
                    Assert.Equal(expectedCompletedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));

                    foreach (var listBlock in listBlocks)
                    {
                        await listBlock.StartAsync();
                        expectedNotStartedCount--;

                        // There should be one less NotStarted block and exactly 1 Started block
                        Assert.Equal(expectedNotStartedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
                        Assert.Equal(1, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));

                        int expectedCompletedItems = 0;
                        int expectedPendingItems = (await listBlock.GetItemsAsync(ItemStatus.Pending)).Count();
                        // All items should be Pending and 0 Completed
                        Assert.Equal(expectedPendingItems, _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Pending));
                        Assert.Equal(expectedCompletedItems, _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Completed));
                        foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatus.Pending))
                        {
                            // do the processing

                            await itemToProcess.CompletedAsync();

                            // More more should be Completed
                            expectedCompletedItems++;
                            Assert.Equal(expectedCompletedItems, _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Completed));
                        }

                        await listBlock.CompleteAsync();

                        // One more block should be completed
                        expectedCompletedCount++;
                        Assert.Equal(expectedCompletedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));
                    }
                }
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_AsListWithSingleUnitCommitAndFailsWithReason_ThenReasonIsPersisted()
        {
            // ARRANGE

            // ACT and // ASSERT
            bool startedOk;
            string listBlockId = string.Empty;
            using (var executionContext = CreateTaskExecutionContext(1))
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var values = GetPersonList(9);
                    short maxBlockSize = 4;
                    var listBlock = (await executionContext.GetListBlocksAsync<PersonDto>(x => x.WithSingleUnitCommit(values, maxBlockSize))).First();
                    listBlockId = listBlock.Block.ListBlockId;
                    await listBlock.StartAsync();

                    int counter = 0;
                    foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatus.Pending))
                    {
                        await itemToProcess.FailedAsync("Exception");

                        counter++;
                    }

                    await listBlock.CompleteAsync();
                }
            }

            Assert.True(_blocksHelper.GetListBlockItems<PersonDto>(listBlockId, ItemStatus.Failed).All(x => x.StatusReason == "Exception"));
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_LargeValues_ThenValuesArePersistedAndRetrievedOk()
        {
            // ARRANGE
            var values = GetLargePersonList(4);

            // ACT and // ASSERT
            bool startedOk;
            string listBlockId = string.Empty;
            using (var executionContext = CreateTaskExecutionContext(1))
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {

                    short maxBlockSize = 4;
                    var listBlock = (await executionContext.GetListBlocksAsync<PersonDto>(x => x.WithSingleUnitCommit(values, maxBlockSize))).First();
                    listBlockId = listBlock.Block.ListBlockId;
                    await listBlock.StartAsync();

                    foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatus.Pending))
                        await itemToProcess.FailedAsync("Exception");

                    await listBlock.CompleteAsync();
                }
            }

            using (var executionContext = CreateTaskExecutionContext(1))
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var emptyPersonList = new List<PersonDto>();
                    short maxBlockSize = 4;
                    var listBlock = (await executionContext.GetListBlocksAsync<PersonDto>(x => x.WithSingleUnitCommit(emptyPersonList, maxBlockSize))).First();
                    listBlockId = listBlock.Block.ListBlockId;
                    await listBlock.StartAsync();

                    var itemsToProcess = (await listBlock.GetItemsAsync(ItemStatus.Pending, ItemStatus.Failed)).ToList();
                    for (int i = 0; i < itemsToProcess.Count; i++)
                    {
                        Assert.Equal(values[i].DateOfBirth, itemsToProcess[i].Value.DateOfBirth);
                        Assert.Equal(values[i].Id, itemsToProcess[i].Value.Id);
                        Assert.Equal(values[i].Name, itemsToProcess[i].Value.Name);
                    }

                    await listBlock.CompleteAsync();
                }
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_AsListWithNoValues_ThenCheckpointIsPersistedAndEmptyBlockGenerated()
        {
            // ARRANGE

            // ACT and // ASSERT
            bool startedOk;
            string listBlockId = string.Empty;
            using (var executionContext = CreateTaskExecutionContext(1))
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var values = new List<PersonDto>() { };
                    short maxBlockSize = 4;
                    var listBlock = await executionContext.GetListBlocksAsync<PersonDto>(x => x.WithSingleUnitCommit(values, maxBlockSize));
                    Assert.False(listBlock.Any());
                    var execEvent = _executionHelper.GetLastEvent(_taskDefinitionId);
                    Assert.Equal(EventType.CheckPoint, execEvent.Item1);
                    Assert.Equal("No values for generate the block. Emtpy Block context returned.", execEvent.Item2);
                }
            }
        }


        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_AsListWithSingleUnitCommitAndStepSet_ThenStepIsPersisted()
        {
            // ARRANGE

            // ACT and // ASSERT
            bool startedOk;
            string listBlockId = string.Empty;
            using (var executionContext = CreateTaskExecutionContext(1))
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var values = GetPersonList(9);
                    short maxBlockSize = 4;
                    var listBlock = (await executionContext.GetListBlocksAsync<PersonDto>(x => x.WithSingleUnitCommit(values, maxBlockSize))).First();
                    listBlockId = listBlock.Block.ListBlockId;
                    await listBlock.StartAsync();

                    int counter = 0;
                    foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatus.Pending))
                    {
                        itemToProcess.Step = 2;
                        await itemToProcess.FailedAsync("Exception");

                        counter++;
                    }

                    await listBlock.CompleteAsync();
                }
            }

            Assert.True(_blocksHelper.GetListBlockItems<PersonDto>(listBlockId, ItemStatus.Failed).All(x => x.StatusReason == "Exception" && x.Step == 2));
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_AsListWithBatchCommitAtEnd_NumberOfBlocksAndStatusesOfBlockExecutionsCorrectAtEveryStep()
        {
            // ARRANGE

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var values = GetPersonList(9);
                    short maxBlockSize = 4;
                    var listBlocks = await executionContext.GetListBlocksAsync<PersonDto>(x => x.WithBatchCommitAtEnd(values, maxBlockSize));
                    // There should be 3 blocks - 4, 4, 1
                    Assert.Equal(3, _blocksHelper.GetBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));
                    int expectedNotStartedCount = 3;
                    int expectedCompletedCount = 0;

                    // All three should be registered as not started
                    Assert.Equal(expectedNotStartedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
                    Assert.Equal(0, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));
                    Assert.Equal(expectedCompletedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));

                    foreach (var listBlock in listBlocks)
                    {
                        await listBlock.StartAsync();
                        expectedNotStartedCount--;

                        // There should be one less NotStarted block and exactly 1 Started block
                        Assert.Equal(expectedNotStartedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
                        Assert.Equal(1, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));

                        int expectedPendingItems = (await listBlock.GetItemsAsync(ItemStatus.Pending)).Count();
                        // All items should be Pending and 0 Completed
                        Assert.Equal(expectedPendingItems, _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Pending));
                        Assert.Equal(0, _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Completed));
                        foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatus.Pending))
                        {
                            // do the processing

                            await itemToProcess.CompletedAsync();

                            // There should be 0 Completed because we batch commit at the end
                            Assert.Equal(0, _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Completed));
                        }

                        await listBlock.CompleteAsync();

                        // All items should be completed now
                        Assert.Equal((await listBlock.GetItemsAsync(ItemStatus.Completed)).Count(), _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Completed));

                        // One more block should be completed
                        expectedCompletedCount++;
                        Assert.Equal(expectedCompletedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));
                    }
                }
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_AsListWithPeriodicCommit_NumberOfBlocksAndStatusesOfBlockExecutionsCorrectAtEveryStep()
        {
            // ARRANGE

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var values = GetPersonList(26);
                    short maxBlockSize = 15;
                    var listBlocks = await executionContext.GetListBlocksAsync<PersonDto>(x => x.WithPeriodicCommit(values, maxBlockSize, BatchSize.Ten));
                    // There should be 2 blocks - 15, 11
                    Assert.Equal(2, _blocksHelper.GetBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));
                    int expectedNotStartedCount = 2;
                    int expectedCompletedCount = 0;

                    // All three should be registered as not started
                    Assert.Equal(expectedNotStartedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
                    Assert.Equal(0, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));
                    Assert.Equal(expectedCompletedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));

                    foreach (var listBlock in listBlocks)
                    {
                        await listBlock.StartAsync();
                        expectedNotStartedCount--;

                        // There should be one less NotStarted block and exactly 1 Started block
                        Assert.Equal(expectedNotStartedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
                        Assert.Equal(1, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));

                        int expectedPendingItems = (await listBlock.GetItemsAsync(ItemStatus.Pending)).Count();
                        int expectedCompletedItems = 0;
                        // All items should be Pending and 0 Completed
                        Assert.Equal(expectedPendingItems, _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Pending));
                        Assert.Equal(expectedCompletedItems, _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Completed));
                        int itemsProcessed = 0;
                        int itemsCommitted = 0;
                        foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatus.Pending))
                        {
                            itemsProcessed++;
                            // do the processing

                            await itemToProcess.CompletedAsync();

                            // There should be 0 Completed unless we have reached the batch size 10
                            if (itemsProcessed % 10 == 0)
                            {
                                Assert.Equal(10, _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Completed));
                                itemsCommitted += 10;
                            }
                            else
                                Assert.Equal(itemsCommitted, _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Completed));
                        }


                        await listBlock.CompleteAsync();

                        // All items should be completed now
                        Assert.Equal(itemsProcessed, _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Completed));

                        // One more block should be completed
                        expectedCompletedCount++;
                        Assert.Equal(expectedCompletedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));
                    }
                }
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_AsListWithPeriodicCommitAndFailsWithReason_ThenReasonIsPersisted()
        {
            // ARRANGE

            // ACT and // ASSERT
            bool startedOk;
            string listBlockId = string.Empty;
            using (var executionContext = CreateTaskExecutionContext(1))
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var values = GetPersonList(14);
                    short maxBlockSize = 20;
                    var listBlock = (await executionContext.GetListBlocksAsync<PersonDto>(x => x.WithPeriodicCommit(values, maxBlockSize, BatchSize.Ten))).First();
                    listBlockId = listBlock.Block.ListBlockId;
                    await listBlock.StartAsync();

                    int counter = 0;
                    foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatus.Pending))
                    {
                        await itemToProcess.FailedAsync("Exception");

                        counter++;
                    }

                    await listBlock.CompleteAsync();
                }
            }

            Assert.True(_blocksHelper.GetListBlockItems<PersonDto>(listBlockId, ItemStatus.Failed).All(x => x.StatusReason == "Exception"));
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_AsListWithPeriodicCommitAndStepSet_ThenStepIsPersisted()
        {
            // ARRANGE

            // ACT and // ASSERT
            bool startedOk;
            string listBlockId = string.Empty;
            using (var executionContext = CreateTaskExecutionContext(1))
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var values = GetPersonList(14);
                    short maxBlockSize = 20;
                    var listBlock = (await executionContext.GetListBlocksAsync<PersonDto>(x => x.WithPeriodicCommit(values, maxBlockSize, BatchSize.Ten))).First();
                    listBlockId = listBlock.Block.ListBlockId;
                    await listBlock.StartAsync();

                    int counter = 0;
                    foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatus.Pending))
                    {
                        itemToProcess.Step = 2;
                        await itemToProcess.FailedAsync("Exception");

                        counter++;
                    }

                    await listBlock.CompleteAsync();
                }
            }

            var listBlockItems = _blocksHelper.GetListBlockItems<PersonDto>(listBlockId, ItemStatus.Failed);
            Assert.True(listBlockItems.All(x => x.StatusReason == "Exception" && x.Step == 2));
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_PreviousBlock_ThenLastBlockContainsCorrectItems()
        {
            // ARRANGE
            // Create previous blocks
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var values = GetPersonList(26);
                    short maxBlockSize = 15;
                    var listBlocks = await executionContext.GetListBlocksAsync<PersonDto>(x => x.WithPeriodicCommit(values, maxBlockSize, BatchSize.Ten));

                    foreach (var listBlock in listBlocks)
                    {
                        await listBlock.StartAsync();
                        foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatus.Pending))
                            await itemToProcess.CompletedAsync();

                        await listBlock.CompleteAsync();
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
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    lastBlock = await executionContext.GetLastListBlockAsync<PersonDto>();
                }
            }

            // ASSERT
            var expectedItems = await expectedLastBlock.GetItemsAsync();
            var lastBlockItems = await lastBlock.GetItemsAsync();
            Assert.Equal(expectedItems.Count, lastBlockItems.Count);
            Assert.Equal(expectedItems[0].Value.Id, lastBlockItems[0].Value.Id);
            Assert.Equal(expectedItems[1].Value.Id, lastBlockItems[1].Value.Id);
            Assert.Equal(expectedItems[2].Value.Id, lastBlockItems[2].Value.Id);
            Assert.Equal(expectedItems[3].Value.Id, lastBlockItems[3].Value.Id);
            Assert.Equal(expectedItems[4].Value.Id, lastBlockItems[4].Value.Id);
            Assert.Equal(expectedItems[5].Value.Id, lastBlockItems[5].Value.Id);
            Assert.Equal(expectedItems[6].Value.Id, lastBlockItems[6].Value.Id);
            Assert.Equal(expectedItems[7].Value.Id, lastBlockItems[7].Value.Id);
            Assert.Equal(expectedItems[8].Value.Id, lastBlockItems[8].Value.Id);
            Assert.Equal(expectedItems[9].Value.Id, lastBlockItems[9].Value.Id);
            Assert.Equal(expectedItems[10].Value.Id, lastBlockItems[10].Value.Id);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_NoPreviousBlock_ThenLastBlockIsNull()
        {
            // ARRANGE
            // all previous blocks were deleted in TestInitialize

            // ACT
            IListBlock<PersonDto> lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    lastBlock = await executionContext.GetLastListBlockAsync<PersonDto>();
                }
            }

            // ASSERT
            Assert.Null(lastBlock);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_PreviousBlockIsPhantom_ThenLastBlockNotThisPhantom()
        {
            // ARRANGE
            // Create previous blocks
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var values = GetPersonList(3);
                    short maxBlockSize = 15;
                    var listBlocks = await executionContext.GetListBlocksAsync<PersonDto>(x => x.WithPeriodicCommit(values, maxBlockSize, BatchSize.Ten));

                    foreach (var listBlock in listBlocks)
                    {
                        await listBlock.StartAsync();
                        foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatus.Pending))
                            await itemToProcess.CompletedAsync();

                        await listBlock.CompleteAsync();
                    }
                }
            }

            _blocksHelper.InsertPhantomListBlock(TestConstants.ApplicationName, TestConstants.TaskName);

            // ACT
            IListBlock<PersonDto> lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    lastBlock = await executionContext.GetLastListBlockAsync<PersonDto>();
                }
            }

            // ASSERT
            var lastBlockItems = await lastBlock.GetItemsAsync();
            Assert.Equal(3, lastBlockItems.Count);
            Assert.Equal("1", lastBlockItems[0].Value.Id);
            Assert.Equal("2", lastBlockItems[1].Value.Id);
            Assert.Equal("3", lastBlockItems[2].Value.Id);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_AnItemFails_ThenCompleteSetsStatusAsFailed()
        {
            // ARRANGE

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext(1))
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var values = GetPersonList(9);
                    short maxBlockSize = 4;
                    var listBlock = (await executionContext.GetListBlocksAsync<PersonDto>(x => x.WithSingleUnitCommit(values, maxBlockSize))).First();

                    await listBlock.StartAsync();

                    int counter = 0;
                    foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatus.Pending))
                    {
                        if (counter == 2)
                            await itemToProcess.FailedAsync("Exception");
                        else
                            await itemToProcess.CompletedAsync();

                        counter++;
                    }

                    await listBlock.CompleteAsync();
                }
            }

            Assert.Equal(1, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Failed));
            Assert.Equal(0, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_ReprocessingSpecificExecutionAndItExistsWithMultipleExecutionsAndOnlyOneFailed_ThenBringBackOnFailedBlockWhenRequested()
        {
            string referenceValue = Guid.NewGuid().ToString();
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = await executionContext.TryStartAsync(referenceValue);
                if (startedOk)
                {
                    var values = GetPersonList(9);
                    short maxBlockSize = 3;
                    var listBlocks = (await executionContext.GetListBlocksAsync<PersonDto>(x => x.WithPeriodicCommit(values, maxBlockSize, BatchSize.Fifty))).ToList();

                    // block 0 has one failed item
                    await listBlocks[0].StartAsync();

                    int counter = 0;
                    foreach (var itemToProcess in await listBlocks[0].GetItemsAsync(ItemStatus.Pending))
                    {
                        if (counter == 2)
                            await listBlocks[0].ItemFailedAsync(itemToProcess, "Exception", 1);
                        else
                            await listBlocks[0].ItemCompleteAsync(itemToProcess);

                        counter++;
                    }

                    await listBlocks[0].CompleteAsync();

                    // block 1 succeeds
                    await listBlocks[1].StartAsync();

                    foreach (var itemToProcess in await listBlocks[1].GetItemsAsync(ItemStatus.Pending))
                    {
                        await listBlocks[1].ItemCompleteAsync(itemToProcess);

                        counter++;
                    }

                    await listBlocks[1].CompleteAsync();

                    // block 2 never starts
                }
            }

            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var listBlocksToReprocess = (await executionContext.GetListBlocksAsync<PersonDto>(x => x.ReprocessWithPeriodicCommit(BatchSize.Fifty)
                                                                .PendingAndFailedBlocks()
                                                                .OfExecutionWith(referenceValue))).ToList();

                    // one failed and one block never started
                    Assert.Equal(2, listBlocksToReprocess.Count);

                    // the block that failed has one failed item
                    var itemsOfB1 = (await listBlocksToReprocess[0].GetItemsAsync(ItemStatus.Failed, ItemStatus.Pending)).ToList();
                    Assert.Single(itemsOfB1);
                    Assert.Equal("Exception", itemsOfB1[0].StatusReason);
                    byte expectedStep = 1;
                    Assert.Equal(expectedStep, itemsOfB1[0].Step);

                    // the block that never executed has 3 pending items
                    var itemsOfB2 = await listBlocksToReprocess[1].GetItemsAsync(ItemStatus.Failed, ItemStatus.Pending);
                    Assert.Equal(3, itemsOfB2.Count());

                    await listBlocksToReprocess[0].CompleteAsync();
                }
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_AsListWithOverridenConfiguration_ThenOverridenValuesAreUsed()
        {
            // ARRANGE
            await CreateFailedTaskAsync();
            await CreateDeadTaskAsync();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var values = GetPersonList(8);
                    short maxBlockSize = 4;
                    var listBlocks = await executionContext.GetListBlocksAsync<PersonDto>(x => x.WithSingleUnitCommit(values, maxBlockSize)
                                                                .OverrideConfiguration()
                                                                .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                                                                .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                                                                .MaximumBlocksToGenerate(5));
                    // There should be 5 blocks - 3, 3, 3, 3, 4
                    Assert.Equal(5, _blocksHelper.GetBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));
                    Assert.True((await listBlocks[0].GetItemsAsync()).All(x => x.Status == ItemStatus.Failed));
                    Assert.Equal(3, (await listBlocks[0].GetItemsAsync()).Count());
                    Assert.True((await listBlocks[1].GetItemsAsync()).All(x => x.Status == ItemStatus.Failed));
                    Assert.Equal(3, (await listBlocks[1].GetItemsAsync()).Count());
                    Assert.True((await listBlocks[2].GetItemsAsync()).All(x => x.Status == ItemStatus.Pending));
                    Assert.Equal(3, (await listBlocks[2].GetItemsAsync()).Count());
                    Assert.True((await listBlocks[3].GetItemsAsync()).All(x => x.Status == ItemStatus.Pending));
                    Assert.Equal(3, (await listBlocks[3].GetItemsAsync()).Count());
                    Assert.True((await listBlocks[4].GetItemsAsync()).All(x => x.Status == ItemStatus.Pending));
                    Assert.Equal(4, (await listBlocks[4].GetItemsAsync()).Count());
                }
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_AsListWithNoOverridenConfiguration_ThenConfigurationValuesAreUsed()
        {
            // ARRANGE
            await CreateFailedTaskAsync();
            await CreateDeadTaskAsync();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var values = GetPersonList(8);
                    short maxBlockSize = 4;
                    var listBlocks = await executionContext.GetListBlocksAsync<PersonDto>(x => x.WithSingleUnitCommit(values, maxBlockSize));
                    // There should be 2 blocks - 4, 4
                    Assert.Equal(2, listBlocks.Count);
                    Assert.True((await listBlocks[0].GetItemsAsync()).All(x => x.Status == ItemStatus.Pending));
                    Assert.Equal(4, (await listBlocks[0].GetItemsAsync()).Count());
                    Assert.True((await listBlocks[1].GetItemsAsync()).All(x => x.Status == ItemStatus.Pending));
                    Assert.Equal(4, (await listBlocks[1].GetItemsAsync()).Count());
                }
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_AsList_ThenReturnsBlockInOrderOfBlockId()
        {
            // ARRANGE

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var values = GetPersonList(10);
                    short maxBlockSize = 1;
                    var listBlocks = await executionContext.GetListBlocksAsync<PersonDto>(x => x.WithSingleUnitCommit(values, maxBlockSize));

                    int counter = 0;
                    int lastId = 0;
                    foreach (var listBlock in listBlocks)
                    {
                        await listBlock.StartAsync();

                        int currentId = int.Parse(listBlock.Block.ListBlockId);
                        if (counter > 0)
                        {
                            Assert.Equal(currentId, lastId + 1);
                        }

                        lastId = currentId;

                        await listBlock.CompleteAsync();
                    }
                }
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_ForceBlock_ThenBlockGetsReprocessedAndDequeued()
        {
            // ARRANGE
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var values = GetPersonList(3);
                    short maxBlockSize = 15;
                    var listBlocks = await executionContext.GetListBlocksAsync<PersonDto>(x => x.WithPeriodicCommit(values, maxBlockSize, BatchSize.Ten));

                    foreach (var listBlock in listBlocks)
                    {
                        await listBlock.StartAsync();
                        foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatus.Pending))
                            await listBlock.ItemCompleteAsync(itemToProcess);

                        await listBlock.CompleteAsync();
                    }
                }
            }

            // add this processed block to the forced queue
            var lastBlockId = _blocksHelper.GetLastBlockId(TestConstants.ApplicationName, TestConstants.TaskName);
            _blocksHelper.EnqueueForcedBlock(lastBlockId);

            // ACT - reprocess the forced block
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var listBlocks = await executionContext.GetListBlocksAsync<PersonDto>(x => x.WithBatchCommitAtEnd(new List<PersonDto>(), 10));
                    Assert.Equal(1, listBlocks.Count);

                    var items = (await listBlocks[0].GetItemsAsync()).ToList();
                    Assert.Equal(3, items.Count());
                    Assert.Equal("1", items[0].Value.Id);
                    Assert.Equal("2", items[1].Value.Id);
                    Assert.Equal("3", items[2].Value.Id);
                    foreach (var listBlock in listBlocks)
                    {
                        await listBlock.StartAsync();

                        foreach (var item in await listBlock.GetItemsAsync())
                            await listBlock.ItemCompleteAsync(item);

                        await listBlock.CompleteAsync();
                    }
                }
            }

            // The forced block will have been dequeued so it should not be processed again
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var items = new List<PersonDto>();
                    var listBlocks = await executionContext.GetListBlocksAsync<PersonDto>(x => x.WithSingleUnitCommit(items, 50));
                    Assert.Equal(0, listBlocks.Count);
                }
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_BlockItemsAccessedBeforeGetItemsCalled_ThenItemsAreLoadedOkAnyway()
        {
            // ARRANGE

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var values = GetPersonList(10);
                    short maxBlockSize = 1;
                    var listBlocks = await executionContext.GetListBlocksAsync<PersonDto>(x => x.WithSingleUnitCommit(values, maxBlockSize));

                    foreach (var listBlock in listBlocks)
                    {
                        await listBlock.StartAsync();

                        var itemsToProcess = await listBlock.Block.GetItemsAsync();
                        foreach (var item in itemsToProcess)
                            await item.CompletedAsync();

                        await listBlock.CompleteAsync();
                    }
                }
            }
        }

        private async Task CreateFailedTaskAsync()
        {
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var values = GetPersonList(6);
                    short maxBlockSize = 3;
                    var listBlocks = await executionContext.GetListBlocksAsync<PersonDto>(x => x.WithPeriodicCommit(values, maxBlockSize, BatchSize.Ten));

                    foreach (var listBlock in listBlocks)
                    {
                        await listBlock.StartAsync();
                        foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatus.Pending))
                            await listBlock.ItemFailedAsync(itemToProcess, "Exception");

                        await listBlock.FailedAsync("Something bad happened");
                    }
                }
            }
        }

        private async Task CreateDeadTaskAsync()
        {
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var values = GetPersonList(6);
                    short maxBlockSize = 3;
                    var listBlocks = await executionContext.GetListBlocksAsync<PersonDto>(x => x.WithPeriodicCommit(values, maxBlockSize, BatchSize.Ten));

                    foreach (var listBlock in listBlocks)
                    {
                        await listBlock.StartAsync();
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
