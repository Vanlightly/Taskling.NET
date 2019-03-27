using Xunit;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.Common;
using Taskling.Blocks.ObjectBlocks;
using Taskling.Contexts;
using Taskling.Events;
using Taskling.SqlServer.Tests.Helpers;
using System.Threading.Tasks;
using System.Threading;

namespace Taskling.SqlServer.Tests.Contexts.Given_ObjectBlockContext
{
    public class When_GetObjectBlocksFromExecutionContext
    {
        private ExecutionsHelper _executionHelper;
        private BlocksHelper _blocksHelper;
        private int _taskDefinitionId;

        public When_GetObjectBlocksFromExecutionContext()
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
        public async Task If_NumberOfBlocksAndStatusesOfBlockExecutionsCorrectAtEveryStep()
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

                    var blocks = await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("Testing1"));
                    Assert.Equal(1, _blocksHelper.GetBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));
                    int expectedNotStartedCount = 1;
                    int expectedCompletedCount = 0;
                    Assert.Equal(expectedNotStartedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
                    Assert.Equal(0, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));
                    Assert.Equal(expectedCompletedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));

                    foreach (var block in blocks)
                    {
                        await block.StartAsync();
                        expectedNotStartedCount--;
                        Assert.Equal(expectedNotStartedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
                        Assert.Equal(1, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));



                        // processing here
                        await block.CompleteAsync();
                        expectedCompletedCount++;
                        Assert.Equal(expectedCompletedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));
                    }
                }
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_NoBlockNeeded_ThenEmptyListAndEventPersisted()
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
                    var rangeBlocks = await executionContext.GetObjectBlocksAsync<string>(x => x.WithNoNewBlocks());
                    Assert.Equal(0, _blocksHelper.GetBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));

                    var lastEvent = _executionHelper.GetLastEvent(_taskDefinitionId);
                    Assert.Equal(EventType.CheckPoint, lastEvent.Item1);
                    Assert.Equal("No values for generate the block. Emtpy Block context returned.", lastEvent.Item2);
                }
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_ComplexObjectStored_ThenRetrievedOk()
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
                    var myObject = new MyComplexClass()
                    {
                        Id = 10,
                        Name = "Rupert",
                        DateOfBirth = new DateTime(1955, 1, 1),
                        SomeOtherData = new MyOtherComplexClass()
                        {
                            Value = 12.6m,
                            Notes = new List<string>() { "hello", "goodbye", null }
                        }
                    };

                    var block = (await executionContext.GetObjectBlocksAsync<MyComplexClass>(x => x.WithObject(myObject))).First();
                    Assert.Equal(myObject.Id, block.Block.Object.Id);
                    Assert.Equal(myObject.Name, block.Block.Object.Name);
                    Assert.Equal(myObject.DateOfBirth, block.Block.Object.DateOfBirth);
                    Assert.Equal(myObject.SomeOtherData.Value, block.Block.Object.SomeOtherData.Value);
                    Assert.Equal(myObject.SomeOtherData.Notes[0], block.Block.Object.SomeOtherData.Notes[0]);
                    Assert.Equal(myObject.SomeOtherData.Notes[1], block.Block.Object.SomeOtherData.Notes[1]);
                    Assert.Null(block.Block.Object.SomeOtherData.Notes[2]);
                }
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_LargeComplexObjectStored_ThenRetrievedOk()
        {
            var longList = GetLargeListOfStrings();

            // ARRANGE
            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var myObject = new MyComplexClass()
                    {
                        Id = 10,
                        Name = "Rupert",
                        DateOfBirth = new DateTime(1955, 1, 1),
                        SomeOtherData = new MyOtherComplexClass()
                        {
                            Value = 12.6m,
                            Notes = longList
                        }
                    };

                    var block = (await executionContext.GetObjectBlocksAsync<MyComplexClass>(x => x.WithObject(myObject))).First();
                    Assert.Equal(myObject.Id, block.Block.Object.Id);
                    Assert.Equal(myObject.Name, block.Block.Object.Name);
                    Assert.Equal(myObject.DateOfBirth, block.Block.Object.DateOfBirth);
                    Assert.Equal(myObject.SomeOtherData.Value, block.Block.Object.SomeOtherData.Value);
                    Assert.Equal(myObject.SomeOtherData.Notes.Count, block.Block.Object.SomeOtherData.Notes.Count);

                    for (int i = 0; i < myObject.SomeOtherData.Notes.Count; i++)
                    {
                        Assert.Equal(myObject.SomeOtherData.Notes[i], block.Block.Object.SomeOtherData.Notes[i]);
                    }
                }
            }
        }

        private List<string> GetLargeListOfStrings()
        {
            var list = new List<string>();

            for (int i = 0; i < 1000; i++)
                list.Add("Long value is " + Guid.NewGuid().ToString());

            return list;
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_PreviousBlock_ThenLastBlockHasCorrectObjectValue()
        {
            // ARRANGE
            // Create previous blocks
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var blocks = await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("Testing123"));

                    foreach (var block in blocks)
                    {
                        await block.StartAsync();
                        await block.CompleteAsync();
                    }
                }
            }

            var expectedLastBlock = new ObjectBlock<string>()
            {
                Object = "Testing123"
            };

            // ACT
            IObjectBlock<string> lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    lastBlock = await executionContext.GetLastObjectBlockAsync<string>();
                }
            }

            // ASSERT
            Assert.Equal(expectedLastBlock.Object, lastBlock.Object);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_NoPreviousBlock_ThenLastBlockIsNull()
        {
            // ARRANGE
            // all previous blocks were deleted in TestInitialize

            // ACT
            IObjectBlock<string> lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    lastBlock = await executionContext.GetLastObjectBlockAsync<string>();
                }
            }

            // ASSERT
            Assert.Null(lastBlock);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_PreviousBlockIsPhantom_ThenLastBlockIsNotThePhantom()
        {
            // ARRANGE
            // Create previous blocks
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var blocks = await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("Testing987"));

                    foreach (var block in blocks)
                    {
                        await block.StartAsync();
                        await block.CompleteAsync();
                    }
                }
            }

            _blocksHelper.InsertPhantomObjectBlock(TestConstants.ApplicationName, TestConstants.TaskName);

            // ACT
            IObjectBlock<string> lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    lastBlock = await executionContext.GetLastObjectBlockAsync<string>();
                }
            }

            // ASSERT
            Assert.Equal("Testing987", lastBlock.Object);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_PreviousExecutionHadOneFailedBlockAndMultipleOkOnes_ThenBringBackTheFailedBlockWhenRequested()
        {
            // ARRANGE
            var referenceValue = Guid.NewGuid().ToString();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = await executionContext.TryStartAsync(referenceValue);
                Assert.True(startedOk);
                if (startedOk)
                {
                    var fromDate = DateTime.UtcNow.AddHours(-12);
                    var toDate = DateTime.UtcNow;
                    var maxBlockRange = new TimeSpan(0, 30, 0);

                    var blocks = new List<IObjectBlockContext<string>>();
                    blocks.AddRange(await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("My object1")));
                    blocks.AddRange(await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("My object2")));
                    blocks.AddRange(await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("My object3")));
                    blocks.AddRange(await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("My object4")));
                    blocks.AddRange(await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("My object5")));

                    await blocks[0].StartAsync();
                    await blocks[0].CompleteAsync(); // completed
                    await blocks[1].StartAsync();
                    await blocks[1].FailedAsync("Something bad happened"); // failed
                    // 2 not started
                    await blocks[3].StartAsync(); // started
                    await blocks[4].StartAsync();
                    await blocks[4].CompleteAsync(); // completed
                }
            }

            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var blocks = await executionContext.GetObjectBlocksAsync<string>(x => x.Reprocess()
                                                        .PendingAndFailedBlocks()
                                                        .OfExecutionWith(referenceValue));

                    Assert.Equal(3, blocks.Count);
                }
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_PreviousExecutionHadOneFailedBlockAndMultipleOkOnes_ThenBringBackAllBlocksWhenRequested()
        {
            // ARRANGE
            var referenceValue = Guid.NewGuid().ToString();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = await executionContext.TryStartAsync(referenceValue);
                Assert.True(startedOk);
                if (startedOk)
                {
                    var fromDate = DateTime.UtcNow.AddHours(-12);
                    var toDate = DateTime.UtcNow;
                    var maxBlockRange = new TimeSpan(0, 30, 0);

                    var blocks = new List<IObjectBlockContext<string>>();
                    blocks.AddRange(await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("My object1")));
                    blocks.AddRange(await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("My object2")));
                    blocks.AddRange(await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("My object3")));
                    blocks.AddRange(await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("My object4")));
                    blocks.AddRange(await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("My object5")));

                    await blocks[0].StartAsync();
                    await blocks[0].CompleteAsync(); // completed
                    await blocks[1].StartAsync();
                    await blocks[1].FailedAsync("Something bad happened"); // failed
                    // 2 not started
                    await blocks[3].StartAsync(); // started
                    await blocks[4].StartAsync();
                    await blocks[4].CompleteAsync(); // completed
                }
            }

            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var blocks = await executionContext.GetObjectBlocksAsync<string>(x => x.Reprocess()
                                                        .AllBlocks()
                                                        .OfExecutionWith(referenceValue));

                    Assert.Equal(5, blocks.Count);
                }
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_WithPreviousDeadBlocks_ThenReprocessOk()
        {
            // ARRANGE
            await CreateFailedObjectBlockTaskAsync();
            await CreateDeadObjectBlockTaskAsync();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var blocks = await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("TestingDFG")
                        .OverrideConfiguration()
                        .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .MaximumBlocksToGenerate(8));

                    int counter = 0;
                    foreach (var block in blocks)
                    {
                        await block.StartAsync();

                        await block.CompleteAsync();

                        counter++;
                        Assert.Equal(counter, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));
                    }


                }
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_AsDateRangeWithOverridenConfiguration_ThenOverridenValuesAreUsed()
        {
            // ARRANGE
            await CreateFailedObjectBlockTaskAsync();
            await CreateDeadObjectBlockTaskAsync();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var blocks = await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("TestingYHN")
                        .OverrideConfiguration()
                        .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .MaximumBlocksToGenerate(8));

                    Assert.Equal(3, blocks.Count());
                    Assert.Contains(blocks, x => x.Block.Object == "Dead Task");
                    Assert.Contains(blocks, x => x.Block.Object == "Failed Task");
                    Assert.Contains(blocks, x => x.Block.Object == "TestingYHN");

                }
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_WithNoOverridenConfiguration_ThenConfigurationValuesAreUsed()
        {
            // ARRANGE
            await CreateFailedObjectBlockTaskAsync();
            await CreateDeadObjectBlockTaskAsync();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var blocks = await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("Testing YUI"));
                    Assert.Single(blocks);
                    Assert.True(blocks.First().Block.Object == "Testing YUI");
                }
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_ForceBlock_ThenBlockGetsReprocessedAndDequeued()
        {
            // ARRANGE
            var fromDate = DateTime.UtcNow.AddHours(-12);
            var toDate = DateTime.UtcNow;

            // create a block
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var blocks = await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("Testing Hello"));
                    foreach (var block in blocks)
                    {
                        await block.StartAsync();
                        await block.CompleteAsync();
                    }
                }
            }

            // add this processed block to the forced queue
            var lastBlockId = _blocksHelper.GetLastBlockId(TestConstants.ApplicationName, TestConstants.TaskName);
            _blocksHelper.EnqueueForcedBlock(lastBlockId);

            // ACT - reprocess the forced block
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var blocks = await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("Testing Goodbye"));
                    Assert.Equal(2, blocks.Count);
                    Assert.Equal("Testing Hello", blocks[0].Block.Object);
                    Assert.Equal("Testing Goodbye", blocks[1].Block.Object);
                    foreach (var block in blocks)
                    {
                        await block.StartAsync();
                        await block.CompleteAsync();
                    }
                }
            }

            // The forced block will have been dequeued so it should not be processed again
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var blocks = await executionContext.GetObjectBlocksAsync<string>(x => x.WithNoNewBlocks());
                    Assert.Equal(0, blocks.Count);
                }
            }
        }

        private ITaskExecutionContext CreateTaskExecutionContext()
        {
            return ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing());
        }

        private ITaskExecutionContext CreateTaskExecutionContextWithNoReprocessing()
        {
            return ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndNoReprocessing());
        }

        private async Task CreateFailedObjectBlockTaskAsync()
        {
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var blocks = await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("Failed Task"));

                    foreach (var block in blocks)
                    {
                        await block.StartAsync();
                        await block.FailedAsync();
                    }
                }
            }
        }

        private async Task CreateDeadObjectBlockTaskAsync()
        {
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var from = new DateTime(2016, 1, 4);
                    var to = new DateTime(2016, 1, 7);
                    var maxBlockSize = new TimeSpan(1, 0, 0, 0);
                    var blocks = await executionContext.GetObjectBlocksAsync<string>(x => x.WithObject("Dead Task"));

                    foreach (var block in blocks)
                    {
                        await block.StartAsync();
                    }
                }
            }

            var executionHelper = new ExecutionsHelper();
            executionHelper.SetLastExecutionAsDead(_taskDefinitionId);
        }


    }
}
