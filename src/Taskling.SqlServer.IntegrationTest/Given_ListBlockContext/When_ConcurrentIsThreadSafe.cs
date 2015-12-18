using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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
    public class When_ConcurrentIsThreadSafe
    {
        private ExecutionsHelper _executionHelper;
        private BlocksHelper _blocksHelper;

        [TestInitialize]
        public void Initialize()
        {
            _blocksHelper = new BlocksHelper();
            _blocksHelper.DeleteBlocks(TestConstants.ApplicationName, TestConstants.TaskName);
            _executionHelper = new ExecutionsHelper();
            _executionHelper.DeleteRecordsOfTask(TestConstants.ApplicationName, TestConstants.TaskName);

            var taskDefinitionId = _executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            _executionHelper.InsertExecutionToken(taskDefinitionId, TaskExecutionStatus.Available, "0");
        }

        [TestMethod]
        [TestCategory("SlowIntegrationTest"), TestCategory("Blocks")]
        public void If_AsListWithSingleUnitCommit_BlocksProcessedSequentially_BlocksListItemsProcessedInParallel_ThenNoConcurrencyIssues()
        {
            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = SqlServerClientHelper.GetExecutionContextWithKeepAlive())
            {
                startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var values = GetList(100000);
                    short maxBlockSize = 1000;
                    var listBlocks = executionContext.GetListBlocks(x => x.WithSingleUnitCommit(values, maxBlockSize)
                                                                .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0))
                                                                .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), new TimeSpan(0, 1, 0, 0))
                                                                .MaximumBlocksToGenerate(10000));
                    foreach (var listBlock in listBlocks)
                    {
                        listBlock.Start();
                        Parallel.ForEach(listBlock.GetAllItems(), (currentItem) =>
                        {
                            listBlock.ItemComplete(currentItem);
                        });
                        
                        listBlock.Complete();

                        // All items should be completed now
                        Assert.AreEqual(listBlock.GetAllItems().Count(), _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ListBlockItemStatus.Completed));
                    }
                }
            }
        }

        [TestMethod]
        [TestCategory("SlowIntegrationTest"), TestCategory("Blocks")]
        public void If_AsListWithBatchCommitAtEnd_BlocksProcessedSequentially_BlocksListItemsProcessedInParallel_ThenNoConcurrencyIssues()
        {
            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = SqlServerClientHelper.GetExecutionContextWithKeepAlive())
            {
                startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var values = GetList(100000);
                    short maxBlockSize = 1000;
                    var listBlocks = executionContext.GetListBlocks(x => x.WithBatchCommitAtEnd(values, maxBlockSize)
                                                                .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0))
                                                                .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), new TimeSpan(0, 1, 0, 0))
                                                                .MaximumBlocksToGenerate(10000));
                    foreach (var listBlock in listBlocks)
                    {
                        listBlock.Start();
                        Parallel.ForEach(listBlock.GetAllItems(), (currentItem) =>
                        {
                            listBlock.ItemComplete(currentItem);
                        });

                        listBlock.Complete();

                        // All items should be completed now
                        Assert.AreEqual(listBlock.GetAllItems().Count(), _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ListBlockItemStatus.Completed));
                    }
                }
            }
        }

        [TestMethod]
        [TestCategory("SlowIntegrationTest"), TestCategory("Blocks")]
        public void If_AsListWithPeriodicCommit_BlocksProcessedSequentially_BlocksListItemsProcessedInParallel_ThenNoConcurrencyIssues()
        {
            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = SqlServerClientHelper.GetExecutionContextWithKeepAlive())
            {
                startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var values = GetList(100000);
                    short maxBlockSize = 1000;
                    var listBlocks = executionContext.GetListBlocks(x => x.WithPeriodicCommit(values, maxBlockSize, BatchSize.Hundred)
                                                                .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0))
                                                                .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), new TimeSpan(0, 1, 0, 0))
                                                                .MaximumBlocksToGenerate(10000));
                    foreach (var listBlock in listBlocks)
                    {
                        listBlock.Start();
                        Parallel.ForEach(listBlock.GetAllItems(), (currentItem) =>
                        {
                            listBlock.ItemComplete(currentItem);
                        });

                        listBlock.Complete();

                        // All items should be completed now
                        Assert.AreEqual(listBlock.GetAllItems().Count(), _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ListBlockItemStatus.Completed));
                    }
                }
            }
        }

        [TestMethod]
        [TestCategory("SlowIntegrationTest"), TestCategory("Blocks")]
        public void If_AsListWithSingleUnitCommit_BlocksProcessedInParallel_BlocksListItemsProcessedSequentially_ThenNoConcurrencyIssues()
        {
            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = SqlServerClientHelper.GetExecutionContextWithKeepAlive())
            {
                startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var values = GetList(100000);
                    short maxBlockSize = 1000;
                    var listBlocks = executionContext.GetListBlocks(x => x.WithSingleUnitCommit(values, maxBlockSize)
                                                                .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0))
                                                                .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), new TimeSpan(0, 1, 0, 0))
                                                                .MaximumBlocksToGenerate(10000));

                    Parallel.ForEach(listBlocks, (currentBlock) =>
                    {
                        currentBlock.Start();

                        foreach (var currentItem in currentBlock.GetAllItems())
                        {
                            currentBlock.ItemComplete(currentItem);
                        };

                        currentBlock.Complete();
                        // All items should be completed now
                        Assert.AreEqual(currentBlock.GetAllItems().Count(), _blocksHelper.GetListBlockItemCountByStatus(currentBlock.ListBlockId, ListBlockItemStatus.Completed));
                    });

                    foreach (var listBlock in listBlocks)
                    {
                        listBlock.Start();
                        Parallel.ForEach(listBlock.GetAllItems(), (currentItem) =>
                        {
                            listBlock.ItemComplete(currentItem);
                        });

                        listBlock.Complete();

                        
                    }
                }
            }
        }

        private List<string> GetList(int count)
        {
            var list = new List<string>();
            
            for (int i = 0; i < count; i++)
            {
                list.Add(i.ToString());
            }

            return list;
        }
    }
}
