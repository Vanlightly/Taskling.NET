using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Taskling.Blocks;
using Taskling.ExecutionContext;
using Taskling.SqlServer.Configuration;
using Taskling.SqlServer.IntegrationTest.TestHelpers;

namespace Taskling.SqlServer.IntegrationTest.Examples
{
    [TestClass]
    public class When_NumericRangeBlocks
    {
        [TestMethod]
        public void RunMyBatchJobExample()
        {
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var blocks = GetNumericRangeBlocks(executionContext);
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

        private Tuple<long, long> GetNumericRangeToProcess(ITaskExecutionContext executionContext)
        {
            using (var criticalSection = executionContext.CreateCriticalSection())
            {
                bool csStartedOk = criticalSection.TryStart();
                if (csStartedOk)
                {
                    // data identification here
                    long maxIdYetToProcess = 1000; //_myDataService.GetMaxId();
                    var lastBlock = executionContext.GetLastDateRangeBlock();
                    long defaultValueIfEmpty = maxIdYetToProcess-(long)(maxIdYetToProcess*0.05);
                    return new Tuple<long, long>(lastBlock.RangeEndAsLong(defaultValueIfEmpty), maxIdYetToProcess);
                }
                else
                {
                    throw new Exception("Failed to acquire a critical section");
                }
            }
        }

        private List<IRangeBlockContext> GetNumericRangeBlocks(ITaskExecutionContext executionContext)
        {
            var numericRangeToProcess = GetNumericRangeToProcess(executionContext);
            var maxBlockSize = 500;
            return executionContext.GetRangeBlocks(x => x.AsNumericRange(numericRangeToProcess.Item1, numericRangeToProcess.Item2, maxBlockSize)
                                    .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0))
                                    .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0))
                                    .MaximumBlocksToGenerate(10))
                                    .ToList();
        }

        private void ProcessBlock(IRangeBlockContext blockContext)
        {
            try
            {
                blockContext.Start();

                // process the data here
                //var dataToProcess = _myDataService.LoadData(blockContext.Block.RangeBeginAsLong(), blockContext.Block.RangeEndAsLong());
                // ...
                // ...
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
