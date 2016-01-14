using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Taskling.Blocks;
using Taskling.ExecutionContext;
using Taskling.ExecutionContext.FluentBlocks.List;
using Taskling.SqlServer.Configuration;
using Taskling.SqlServer.IntegrationTest.TestHelpers;

namespace Taskling.SqlServer.IntegrationTest.Examples
{
    [TestClass]
    public class When_ListBlocks
    {
        [TestMethod]
        public void RunMyBatchJobExample()
        {
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var blocks = GetListBlocks(executionContext);
                    foreach (var blockContext in blocks)
                        ProcessBlock(blockContext);
                }
            }
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

        private List<IListBlockContext> GetListBlocks(ITaskExecutionContext executionContext)
        {
            var listToProcess = GetItemsToProcess(executionContext);
            short maxBlockSize = 500;
            return executionContext.GetListBlocks(x => x.WithPeriodicCommit(listToProcess, maxBlockSize, BatchSize.Hundred)
                                    .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0))
                                    .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0))
                                    .MaximumBlocksToGenerate(10))
                                    .ToList();
        }

        private List<string> GetItemsToProcess(ITaskExecutionContext executionContext)
        {
            using (var criticalSection = executionContext.CreateCriticalSection())
            {
                bool csStartedOk = criticalSection.TryStart();
                if (csStartedOk)
                {
                    // data identification here
                    //var lastBlock = executionContext.GetLastListBlock();
                    return new List<string>() { "A", "B", "C", "D", "E", "F", "G", "H", "I" };
                    //return _myDataService.GetDataIdsToProcess(lastBlock.Items);
                }
                else
                {
                    throw new Exception("Failed to acquire a critical section");
                }
            }
        }

        private void ProcessBlock(IListBlockContext blockContext)
        {
            try
            {
                blockContext.Start();

                foreach (var itemToProcess in blockContext.GetAllItems())
                {
                    try
                    {
                        // do the processing
                        // ...
                        // ...

                        blockContext.ItemComplete(itemToProcess);
                    }
                    catch(Exception)
                    {
                        blockContext.ItemFailed(itemToProcess);
                    }
                }
            }
            catch (Exception ex)
            {
                blockContext.Failed();
                //logger.Error(ex);
            }
            finally
            {
                blockContext.Complete();
            }
        }
    }
}
