using Xunit;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Taskling.Blocks.ListBlocks;
using Taskling.SqlServer.Tests.Helpers;

namespace Taskling.SqlServer.Tests.Contexts.Given_ListBlockContext
{
    public class When_ConcurrentIsThreadSafe
    {
        private ExecutionsHelper _executionHelper;
        private BlocksHelper _blocksHelper;

        public When_ConcurrentIsThreadSafe()
        {
            _blocksHelper = new BlocksHelper();
            _blocksHelper.DeleteBlocks(TestConstants.ApplicationName);
            _executionHelper = new ExecutionsHelper();
            _executionHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);

            var taskDefinitionId = _executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            _executionHelper.InsertUnlimitedExecutionToken(taskDefinitionId);
        }

        [Fact]
        [Trait("Speed", "Slow")]
        [Trait("Area", "Blocks")]
        public void If_AsListWithSingleUnitCommit_BlocksProcessedSequentially_BlocksListItemsProcessedInParallel_ThenNoConcurrencyIssues()
        {
            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing(10000)))
            {
                startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var values = GetList(100000);
                    short maxBlockSize = 1000;
                    var listBlocks = executionContext.GetListBlocks<PersonDto>(x => x.WithSingleUnitCommit(values, maxBlockSize));
                    foreach (var listBlock in listBlocks)
                    {
                        listBlock.Start();
                        Parallel.ForEach(listBlock.GetItems(ItemStatus.Failed, ItemStatus.Pending), (currentItem) =>
                        {
                            listBlock.ItemComplete(currentItem);
                        });

                        listBlock.Complete();

                        // All items should be completed now
                        Assert.Equal(listBlock.GetItems(ItemStatus.Completed).Count(), _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Completed));
                    }
                }
            }
        }

        [Fact]
        [Trait("Speed", "Slow")]
        [Trait("Area", "Blocks")]
        public void If_AsListWithBatchCommitAtEnd_BlocksProcessedSequentially_BlocksListItemsProcessedInParallel_ThenNoConcurrencyIssues()
        {
            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing(10000)))
            {
                startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var values = GetList(100000);
                    short maxBlockSize = 1000;
                    var listBlocks = executionContext.GetListBlocks<PersonDto>(x => x.WithBatchCommitAtEnd(values, maxBlockSize));
                    foreach (var listBlock in listBlocks)
                    {
                        listBlock.Start();
                        Parallel.ForEach(listBlock.GetItems(ItemStatus.Failed, ItemStatus.Pending), (currentItem) =>
                        {
                            listBlock.ItemComplete(currentItem);
                        });

                        listBlock.Complete();

                        // All items should be completed now
                        Assert.Equal(listBlock.GetItems(ItemStatus.Completed).Count(), _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Completed));
                    }
                }
            }
        }

        [Fact]
        [Trait("Speed", "Slow")]
        [Trait("Area", "Blocks")]
        public void If_AsListWithPeriodicCommit_BlocksProcessedSequentially_BlocksListItemsProcessedInParallel_ThenNoConcurrencyIssues()
        {
            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing(10000)))
            {
                startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var values = GetList(100000);
                    short maxBlockSize = 1000;
                    var listBlocks = executionContext.GetListBlocks<PersonDto>(x => x.WithPeriodicCommit(values, maxBlockSize, BatchSize.Hundred));
                    foreach (var listBlock in listBlocks)
                    {
                        listBlock.Start();
                        Parallel.ForEach(listBlock.GetItems(ItemStatus.Failed, ItemStatus.Pending), (currentItem) =>
                        {
                            listBlock.ItemComplete(currentItem);
                        });

                        listBlock.Complete();

                        // All items should be completed now
                        Assert.Equal(listBlock.GetItems(ItemStatus.Completed).Count(), _blocksHelper.GetListBlockItemCountByStatus(listBlock.ListBlockId, ItemStatus.Completed));
                    }
                }
            }
        }

        [Fact]
        [Trait("Speed", "Slow")]
        [Trait("Area", "Blocks")]
        public void If_AsListWithSingleUnitCommit_BlocksProcessedInParallel_BlocksListItemsProcessedSequentially_ThenNoConcurrencyIssues()
        {
            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing(10000)))
            {
                startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var values = GetList(100000);
                    short maxBlockSize = 1000;
                    var listBlocks = executionContext.GetListBlocks<PersonDto>(x => x.WithSingleUnitCommit(values, maxBlockSize));

                    Parallel.ForEach(listBlocks, (currentBlock) =>
                    {
                        currentBlock.Start();

                        foreach (var currentItem in currentBlock.GetItems(ItemStatus.Pending))
                        {
                            currentBlock.ItemComplete(currentItem);
                        };

                        currentBlock.Complete();
                        // All items should be completed now
                        Assert.Equal(currentBlock.GetItems(ItemStatus.Completed).Count(), _blocksHelper.GetListBlockItemCountByStatus(currentBlock.ListBlockId, ItemStatus.Completed));
                    });
                }
            }
        }

        private List<PersonDto> GetList(int count)
        {
            var list = new List<PersonDto>();

            for (int i = 0; i < count; i++)
            {
                list.Add(new PersonDto() { DateOfBirth = new DateTime(1980, 1, 1), Id = i.ToString(), Name = "Terrence" + i });
            }

            return list;
        }
    }
}
