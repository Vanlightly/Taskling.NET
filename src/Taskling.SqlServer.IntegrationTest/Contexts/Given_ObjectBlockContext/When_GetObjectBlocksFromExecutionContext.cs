using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.Common;
using Taskling.Blocks.ObjectBlocks;
using Taskling.Contexts;
using Taskling.Events;
using Taskling.SqlServer.IntegrationTest.Helpers;

namespace Taskling.SqlServer.IntegrationTest.Contexts.Given_ObjectBlockContext
{
    [TestClass]
    public class When_GetObjectBlocksFromExecutionContext
    {
        private ExecutionsHelper _executionHelper;
        private BlocksHelper _blocksHelper;
        private int _taskDefinitionId;

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
        public void If_NumberOfBlocksAndStatusesOfBlockExecutionsCorrectAtEveryStep()
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

                    var blocks = executionContext.GetObjectBlocks<string>(x => x.WithObject("Testing1"));
                    Assert.AreEqual(1, _blocksHelper.GetBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));
                    int expectedNotStartedCount = 1;
                    int expectedCompletedCount = 0;
                    Assert.AreEqual(expectedNotStartedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
                    Assert.AreEqual(0, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));
                    Assert.AreEqual(expectedCompletedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));

                    foreach (var block in blocks)
                    {
                        block.Start();
                        expectedNotStartedCount--;
                        Assert.AreEqual(expectedNotStartedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
                        Assert.AreEqual(1, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));



                        // processing here
                        block.Complete();
                        expectedCompletedCount++;
                        Assert.AreEqual(expectedCompletedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));
                    }
                }
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_NoBlockNeeded_ThenEmptyListAndEventPersisted()
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
                    var rangeBlocks = executionContext.GetObjectBlocks<string>(x => x.WithNoNewBlocks());
                    Assert.AreEqual(0, _blocksHelper.GetBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));

                    var lastEvent = _executionHelper.GetLastEvent(_taskDefinitionId);
                    Assert.AreEqual(EventType.CheckPoint, lastEvent.Item1);
                    Assert.AreEqual("No values for generate the block. Emtpy Block context returned.", lastEvent.Item2);
                }
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_ComplexObjectStored_ThenRetrievedOk()
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

                    var block = executionContext.GetObjectBlocks<MyComplexClass>(x => x.WithObject(myObject)).First();
                    Assert.AreEqual(myObject.Id, block.Block.Object.Id);
                    Assert.AreEqual(myObject.Name, block.Block.Object.Name);
                    Assert.AreEqual(myObject.DateOfBirth, block.Block.Object.DateOfBirth);
                    Assert.AreEqual(myObject.SomeOtherData.Value, block.Block.Object.SomeOtherData.Value);
                    Assert.AreEqual(myObject.SomeOtherData.Notes[0], block.Block.Object.SomeOtherData.Notes[0]);
                    Assert.AreEqual(myObject.SomeOtherData.Notes[1], block.Block.Object.SomeOtherData.Notes[1]);
                    Assert.IsNull(block.Block.Object.SomeOtherData.Notes[2]);
                }
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_LargeComplexObjectStored_ThenRetrievedOk()
        {
            var longList = GetLargeListOfStrings();

            // ARRANGE
            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = executionContext.TryStart();
                Assert.AreEqual(true, startedOk);
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

                    var block = executionContext.GetObjectBlocks<MyComplexClass>(x => x.WithObject(myObject)).First();
                    Assert.AreEqual(myObject.Id, block.Block.Object.Id);
                    Assert.AreEqual(myObject.Name, block.Block.Object.Name);
                    Assert.AreEqual(myObject.DateOfBirth, block.Block.Object.DateOfBirth);
                    Assert.AreEqual(myObject.SomeOtherData.Value, block.Block.Object.SomeOtherData.Value);
                    Assert.AreEqual(myObject.SomeOtherData.Notes.Count, block.Block.Object.SomeOtherData.Notes.Count);

                    for (int i = 0; i < myObject.SomeOtherData.Notes.Count; i++)
                    {
                        Assert.AreEqual(myObject.SomeOtherData.Notes[i], block.Block.Object.SomeOtherData.Notes[i]);
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

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_PreviousBlock_ThenLastBlockHasCorrectObjectValue()
        {
            // ARRANGE
            // Create previous blocks
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var blocks = executionContext.GetObjectBlocks<string>(x => x.WithObject("Testing123"));

                    foreach (var block in blocks)
                    {
                        block.Start();
                        block.Complete();
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
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    lastBlock = executionContext.GetLastObjectBlock<string>();
                }
            }

            // ASSERT
            Assert.AreEqual(expectedLastBlock.Object, lastBlock.Object);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_NoPreviousBlock_ThenLastBlockIsNull()
        {
            // ARRANGE
            // all previous blocks were deleted in TestInitialize

            // ACT
            IObjectBlock<string> lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    lastBlock = executionContext.GetLastObjectBlock<string>();
                }
            }

            // ASSERT
            Assert.IsNull(lastBlock);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_PreviousBlockIsPhantom_ThenLastBlockIsNotThePhantom()
        {
            // ARRANGE
            // Create previous blocks
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var blocks = executionContext.GetObjectBlocks<string>(x => x.WithObject("Testing987"));

                    foreach (var block in blocks)
                    {
                        block.Start();
                        block.Complete();
                    }
                }
            }

            _blocksHelper.InsertPhantomObjectBlock(TestConstants.ApplicationName, TestConstants.TaskName);

            // ACT
            IObjectBlock<string> lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    lastBlock = executionContext.GetLastObjectBlock<string>();
                }
            }

            // ASSERT
            Assert.AreEqual("Testing987", lastBlock.Object);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_PreviousExecutionHadOneFailedBlockAndMultipleOkOnes_ThenBringBackTheFailedBlockWhenRequested()
        {
            // ARRANGE
            var referenceValue = Guid.NewGuid().ToString();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = executionContext.TryStart(referenceValue);
                Assert.AreEqual(true, startedOk);
                if (startedOk)
                {
                    var fromDate = DateTime.UtcNow.AddHours(-12);
                    var toDate = DateTime.UtcNow;
                    var maxBlockRange = new TimeSpan(0, 30, 0);

                    var blocks = new List<IObjectBlockContext<string>>();
                    blocks.AddRange(executionContext.GetObjectBlocks<string>(x => x.WithObject("My object1")));
                    blocks.AddRange(executionContext.GetObjectBlocks<string>(x => x.WithObject("My object2")));
                    blocks.AddRange(executionContext.GetObjectBlocks<string>(x => x.WithObject("My object3")));
                    blocks.AddRange(executionContext.GetObjectBlocks<string>(x => x.WithObject("My object4")));
                    blocks.AddRange(executionContext.GetObjectBlocks<string>(x => x.WithObject("My object5")));

                    blocks[0].Start();
                    blocks[0].Complete(); // completed
                    blocks[1].Start();
                    blocks[1].Failed("Something bad happened"); // failed
                    // 2 not started
                    blocks[3].Start(); // started
                    blocks[4].Start();
                    blocks[4].Complete(); // completed
                }
            }

            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = executionContext.TryStart();
                Assert.AreEqual(true, startedOk);
                if (startedOk)
                {
                    var blocks = executionContext.GetObjectBlocks<string>(x => x.Reprocess()
                                                        .PendingAndFailedBlocks()
                                                        .OfExecutionWith(referenceValue)).ToList();

                    Assert.AreEqual(3, blocks.Count);
                }
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_PreviousExecutionHadOneFailedBlockAndMultipleOkOnes_ThenBringBackAllBlocksWhenRequested()
        {
            // ARRANGE
            var referenceValue = Guid.NewGuid().ToString();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = executionContext.TryStart(referenceValue);
                Assert.AreEqual(true, startedOk);
                if (startedOk)
                {
                    var fromDate = DateTime.UtcNow.AddHours(-12);
                    var toDate = DateTime.UtcNow;
                    var maxBlockRange = new TimeSpan(0, 30, 0);

                    var blocks = new List<IObjectBlockContext<string>>();
                    blocks.AddRange(executionContext.GetObjectBlocks<string>(x => x.WithObject("My object1")));
                    blocks.AddRange(executionContext.GetObjectBlocks<string>(x => x.WithObject("My object2")));
                    blocks.AddRange(executionContext.GetObjectBlocks<string>(x => x.WithObject("My object3")));
                    blocks.AddRange(executionContext.GetObjectBlocks<string>(x => x.WithObject("My object4")));
                    blocks.AddRange(executionContext.GetObjectBlocks<string>(x => x.WithObject("My object5")));

                    blocks[0].Start();
                    blocks[0].Complete(); // completed
                    blocks[1].Start();
                    blocks[1].Failed("Something bad happened"); // failed
                    // 2 not started
                    blocks[3].Start(); // started
                    blocks[4].Start();
                    blocks[4].Complete(); // completed
                }
            }

            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = executionContext.TryStart();
                Assert.AreEqual(true, startedOk);
                if (startedOk)
                {
                    var blocks = executionContext.GetObjectBlocks<string>(x => x.Reprocess()
                                                        .AllBlocks()
                                                        .OfExecutionWith(referenceValue)).ToList();

                    Assert.AreEqual(5, blocks.Count);
                }
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_WithPreviousDeadBlocks_ThenReprocessOk()
        {
            // ARRANGE
            CreateFailedObjectBlockTask();
            CreateDeadObjectBlockTask();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                startedOk = executionContext.TryStart();
                Assert.AreEqual(true, startedOk);
                if (startedOk)
                {
                    var blocks = executionContext.GetObjectBlocks<string>(x => x.WithObject("TestingDFG")
                        .OverrideConfiguration()
                        .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .MaximumBlocksToGenerate(8));

                    int counter = 0;
                    foreach (var block in blocks)
                    {
                        block.Start();

                        block.Complete();

                        counter++;
                        Assert.AreEqual(counter, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));
                    }


                }
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AsDateRangeWithOverridenConfiguration_ThenOverridenValuesAreUsed()
        {
            // ARRANGE
            CreateFailedObjectBlockTask();
            CreateDeadObjectBlockTask();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                startedOk = executionContext.TryStart();
                Assert.AreEqual(true, startedOk);
                if (startedOk)
                {
                    var blocks = executionContext.GetObjectBlocks<string>(x => x.WithObject("TestingYHN")
                        .OverrideConfiguration()
                        .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .MaximumBlocksToGenerate(8));

                    Assert.AreEqual(3, blocks.Count());
                    Assert.IsTrue(blocks.Any(x => x.Block.Object == "Dead Task"));
                    Assert.IsTrue(blocks.Any(x => x.Block.Object == "Failed Task"));
                    Assert.IsTrue(blocks.Any(x => x.Block.Object == "TestingYHN"));

                }
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_WithNoOverridenConfiguration_ThenConfigurationValuesAreUsed()
        {
            // ARRANGE
            CreateFailedObjectBlockTask();
            CreateDeadObjectBlockTask();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                startedOk = executionContext.TryStart();
                Assert.AreEqual(true, startedOk);
                if (startedOk)
                {
                    var blocks = executionContext.GetObjectBlocks<string>(x => x.WithObject("Testing YUI"));
                    Assert.AreEqual(1, blocks.Count());
                    Assert.IsTrue(blocks.First().Block.Object == "Testing YUI");
                }
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_ForceBlock_ThenBlockGetsReprocessedAndDequeued()
        {
            // ARRANGE
            var fromDate = DateTime.UtcNow.AddHours(-12);
            var toDate = DateTime.UtcNow;

            // create a block
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var blocks = executionContext.GetObjectBlocks<string>(x => x.WithObject("Testing Hello"));
                    foreach (var block in blocks)
                    {
                        block.Start();
                        block.Complete();
                    }
                }
            }

            // add this processed block to the forced queue
            var lastBlockId = _blocksHelper.GetLastBlockId(TestConstants.ApplicationName, TestConstants.TaskName);
            _blocksHelper.EnqueueForcedBlock(lastBlockId);

            // ACT - reprocess the forced block
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var blocks = executionContext.GetObjectBlocks<string>(x => x.WithObject("Testing Goodbye"));
                    Assert.AreEqual(2, blocks.Count);
                    Assert.AreEqual("Testing Hello", blocks[0].Block.Object);
                    Assert.AreEqual("Testing Goodbye", blocks[1].Block.Object);
                    foreach (var block in blocks)
                    {
                        block.Start();
                        block.Complete();
                    }
                }
            }

            // The forced block will have been dequeued so it should not be processed again
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var blocks = executionContext.GetObjectBlocks<string>(x => x.WithNoNewBlocks());
                    Assert.AreEqual(0, blocks.Count);
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

        private void CreateFailedObjectBlockTask()
        {
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var blocks = executionContext.GetObjectBlocks<string>(x => x.WithObject("Failed Task"));

                    foreach (var block in blocks)
                    {
                        block.Start();
                        block.Failed();
                    }
                }
            }
        }

        private void CreateDeadObjectBlockTask()
        {
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var from = new DateTime(2016, 1, 4);
                    var to = new DateTime(2016, 1, 7);
                    var maxBlockSize = new TimeSpan(1, 0, 0, 0);
                    var blocks = executionContext.GetObjectBlocks<string>(x => x.WithObject("Dead Task"));

                    foreach (var block in blocks)
                    {
                        block.Start();
                    }
                }
            }

            var executionHelper = new ExecutionsHelper();
            executionHelper.SetLastExecutionAsDead(_taskDefinitionId);
        }


    }
}
