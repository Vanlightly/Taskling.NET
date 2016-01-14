using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Taskling.Blocks;
using Taskling.Client;
using Taskling.ExecutionContext;
using Taskling.SqlServer.Configuration;
using Taskling.SqlServer.IntegrationTest.TestHelpers;

namespace Taskling.SqlServer.IntegrationTest.Examples
{
    [TestClass]
    public class When_DateRangeBlocks
    {
        [TestMethod]
        public void RunMyBatchJobExample()
        {
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var blocks = GetDateRangeBlocks(executionContext);
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

        private Tuple<DateTime, DateTime> GetDateRangeToProcess(ITaskExecutionContext executionContext)
        {
            using (var criticalSection = executionContext.CreateCriticalSection())
            {
                bool csStartedOk = criticalSection.TryStart();
                if (csStartedOk)
                {
                    // data identification here
                    var lastBlock = executionContext.GetLastDateRangeBlock();
                    var defaultValueIfEmpty = DateTime.UtcNow.AddHours(-12);
                    return new Tuple<DateTime, DateTime>(lastBlock.RangeEndAsDateTime(defaultValueIfEmpty), DateTime.UtcNow);
                }
                else
                {
                    throw new Exception("Failed to acquire a critical section");
                }
            }
        }

        private List<IRangeBlockContext> GetDateRangeBlocks(ITaskExecutionContext executionContext)
        {
            var dateRangeToProcess = GetDateRangeToProcess(executionContext);
            var maxBlockRange = new TimeSpan(0, 30, 0);
            return executionContext.GetRangeBlocks(x => x.AsDateRange(dateRangeToProcess.Item1, dateRangeToProcess.Item2, maxBlockRange)
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
                //var dataToProcess = _myDataService.LoadData(blockContext.Block.RangeBeginAsDateTime(), blockContext.Block.RangeEndAsDateTime());
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
