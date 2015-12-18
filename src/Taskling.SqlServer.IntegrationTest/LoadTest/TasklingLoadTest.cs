using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Taskling.ExecutionContext.FluentBlocks.List;
using Taskling.SqlServer.IntegrationTest.TestHelpers;

namespace Taskling.SqlServer.IntegrationTest.LoadTest
{
    [TestClass]
    public class TasklingLoadTest
    {
        private List<string> _processes = new List<string>()
        {
            "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"
        };

        private bool _continue;

        private Random _random = new Random();

        [TestMethod]
        [TestCategory("SlowIntegrationTest")]
        public void StartLoadTest()
        {
            CreateTasksAndExecutionTokens();
            var tasks = new List<Task>();
            for (int i = 0; i < 100; i++)
                tasks.Add(Task.Factory.StartNew(() => RunRandomTask(100)));

            Task.WaitAll(tasks.ToArray());
        }

        private void CreateTasksAndExecutionTokens()
        {
            var executionHelper = new ExecutionsHelper();
            foreach (var process in _processes)
            {
                executionHelper.DeleteRecordsOfTask(TestConstants.ApplicationName, "DR" + process);
                executionHelper.DeleteRecordsOfTask(TestConstants.ApplicationName, "NR" + process);
                executionHelper.DeleteRecordsOfTask(TestConstants.ApplicationName, "LSUC" + process);
                executionHelper.DeleteRecordsOfTask(TestConstants.ApplicationName, "LPC" + process);
                executionHelper.DeleteRecordsOfTask(TestConstants.ApplicationName, "LBC" + process);

                int drSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, "DR" + process);
                int nrSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, "NR" + process);
                int lsucSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, "LSUC" + process);
                int lpcSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, "LPC" + process);
                int lbcSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, "LBC" + process);

                executionHelper.InsertAvailableExecutionToken(drSecondaryId);
                executionHelper.InsertAvailableExecutionToken(nrSecondaryId);
                executionHelper.InsertAvailableExecutionToken(lsucSecondaryId);
                executionHelper.InsertAvailableExecutionToken(lpcSecondaryId);
                executionHelper.InsertAvailableExecutionToken(lbcSecondaryId);
            }
        }

        private void RunRandomTask(int repeatCount)
        {
            for(int i=0; i<repeatCount; i++)
            {
                var num = _random.Next(9);
                switch (num)
                {
                    case 0:
                        RunKeepAliveDateRangeTask();
                        break;
                    case 1:
                        RunKeepAliveNumericRangeTask();
                        break;
                    case 2:
                        RunKeepAliveListTaskWithSingleUnitCommit();
                        break;
                    case 3:
                        RunKeepAliveListTaskWithPeriodicCommit();
                        break;
                    case 4:
                        RunKeepAliveListTaskWithBatchCommit();
                        break;
                    case 5:
                        RunOverrideDateRangeTask();
                        break;
                    case 6:
                        RunOverrideNumericRangeTask();
                        break;
                    case 7:
                        RunOverrideListTaskWithSingleUnitCommit();
                        break;
                    case 8:
                        RunOverrideListTaskWithPeriodicCommit();
                        break;
                    case 9:
                        RunOverrideListTaskWithBatchCommit();
                        break;
                }
            }
        }

        private void RunKeepAliveDateRangeTask()
        {
            var taskName = "DR" + _processes[_random.Next(25)];
            Console.WriteLine(taskName);
            using (var executionContext = SqlServerClientHelper.GetExecutionContextWithKeepAlive(taskName, new TimeSpan(0, 0, 5)))
            {
                var startedOk = executionContext.TryStart();
                using (var cs = executionContext.CreateCriticalSection())
                {
                    cs.TryStart();

                }

                var blocks = executionContext.GetRangeBlocks(x => x.AsDateRange(DateTime.UtcNow.AddDays(-1), DateTime.UtcNow, new TimeSpan(0, 1, 0, 0))
                    .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), new TimeSpan(0, 1, 0, 0))
                    .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0))
                    .MaximumBlocksToGenerate(50));

                foreach (var block in blocks)
                {
                    block.Start();
                    block.Complete();
                }
            }
        }

        private void RunKeepAliveNumericRangeTask()
        {
            var taskName = "NR" + _processes[_random.Next(25)];
            Console.WriteLine(taskName);
            using (var executionContext = SqlServerClientHelper.GetExecutionContextWithKeepAlive(taskName, new TimeSpan(0, 0, 5)))
            {
                var startedOk = executionContext.TryStart();
                using (var cs = executionContext.CreateCriticalSection())
                {
                    cs.TryStart();

                }

                var blocks = executionContext.GetRangeBlocks(x => x.AsNumericRange(1, 10000, 100)
                    .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), new TimeSpan(0, 1, 0, 0))
                    .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0))
                    .MaximumBlocksToGenerate(50));

                foreach (var block in blocks)
                {
                    block.Start();
                    block.Complete();
                }
            }
        }

        private void RunKeepAliveListTaskWithSingleUnitCommit()
        {
            var taskName = "LSUC" + _processes[_random.Next(25)];
            Console.WriteLine(taskName);
            using (var executionContext = SqlServerClientHelper.GetExecutionContextWithKeepAlive(taskName, new TimeSpan(0, 0, 5)))
            {
                var startedOk = executionContext.TryStart();
                using (var cs = executionContext.CreateCriticalSection())
                {
                    cs.TryStart();

                }

                var values = GetList("SUC", 1000);
                var blocks = executionContext.GetListBlocks(x => x.WithSingleUnitCommit(values, 50)
                    .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), new TimeSpan(0, 1, 0, 0))
                    .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0))
                    .MaximumBlocksToGenerate(50));

                foreach (var block in blocks)
                {
                    block.Start();
                    block.Complete();
                }
            }
        }

        private void RunKeepAliveListTaskWithPeriodicCommit()
        {
            var taskName = "LPC" + _processes[_random.Next(25)];
            Console.WriteLine(taskName);
            using (var executionContext = SqlServerClientHelper.GetExecutionContextWithKeepAlive(taskName, new TimeSpan(0, 0, 5)))
            {
                var startedOk = executionContext.TryStart();
                using (var cs = executionContext.CreateCriticalSection())
                {
                    cs.TryStart();

                }

                var values = GetList("PC", 1000);
                var blocks = executionContext.GetListBlocks(x => x.WithPeriodicCommit(values, 50, BatchSize.Hundred)
                    .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), new TimeSpan(0, 1, 0, 0))
                    .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0))
                    .MaximumBlocksToGenerate(50));

                foreach (var block in blocks)
                {
                    block.Start();
                    block.Complete();
                }
            }
        }

        private void RunKeepAliveListTaskWithBatchCommit()
        {
            var taskName = "LBC" + _processes[_random.Next(25)];
            Console.WriteLine(taskName);
            using (var executionContext = SqlServerClientHelper.GetExecutionContextWithKeepAlive(taskName, new TimeSpan(0, 0, 5)))
            {
                var startedOk = executionContext.TryStart();
                using (var cs = executionContext.CreateCriticalSection())
                {
                    cs.TryStart();

                }

                var values = GetList("BC", 1000);
                var blocks = executionContext.GetListBlocks(x => x.WithBatchCommitAtEnd(values, 50)
                    .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), new TimeSpan(0, 1, 0, 0))
                    .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0))
                    .MaximumBlocksToGenerate(50));

                foreach (var block in blocks)
                {
                    block.Start();
                    block.Complete();
                }
            }
        }


        private void RunOverrideDateRangeTask()
        {
            var taskName = "OV_DR" + _processes[_random.Next(25)];
            Console.WriteLine(taskName);
            using (var executionContext = SqlServerClientHelper.GetExecutionContextWithOverride(taskName, new TimeSpan(0, 1, 0)))
            {
                var startedOk = executionContext.TryStart();
                using (var cs = executionContext.CreateCriticalSection())
                {
                    cs.TryStart();

                }

                var blocks = executionContext.GetRangeBlocks(x => x.AsDateRange(DateTime.UtcNow.AddDays(-1), DateTime.UtcNow, new TimeSpan(0, 1, 0, 0))
                    .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), new TimeSpan(0, 1, 0, 0))
                    .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0))
                    .MaximumBlocksToGenerate(50));

                foreach (var block in blocks)
                {
                    block.Start();
                    block.Complete();
                }
            }
        }

        private void RunOverrideNumericRangeTask()
        {
            var taskName = "OV_NR" + _processes[_random.Next(25)];
            Console.WriteLine(taskName);
            using (var executionContext = SqlServerClientHelper.GetExecutionContextWithOverride(taskName, new TimeSpan(0, 1, 0)))
            {
                var startedOk = executionContext.TryStart();
                using (var cs = executionContext.CreateCriticalSection())
                {
                    cs.TryStart();

                }

                var blocks = executionContext.GetRangeBlocks(x => x.AsNumericRange(1, 10000, 100)
                    .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), new TimeSpan(0, 1, 0, 0))
                    .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0))
                    .MaximumBlocksToGenerate(50));

                foreach (var block in blocks)
                {
                    block.Start();
                    block.Complete();
                }
            }
        }

        private void RunOverrideListTaskWithSingleUnitCommit()
        {
            var taskName = "OV_LSUC" + _processes[_random.Next(25)];
            Console.WriteLine(taskName);
            using (var executionContext = SqlServerClientHelper.GetExecutionContextWithOverride(taskName, new TimeSpan(0, 1, 0)))
            {
                var startedOk = executionContext.TryStart();
                using (var cs = executionContext.CreateCriticalSection())
                {
                    cs.TryStart();

                }

                var values = GetList("SUC", 1000);
                var blocks = executionContext.GetListBlocks(x => x.WithSingleUnitCommit(values, 50)
                    .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), new TimeSpan(0, 1, 0, 0))
                    .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0))
                    .MaximumBlocksToGenerate(50));

                foreach (var block in blocks)
                {
                    block.Start();
                    block.Complete();
                }
            }
        }

        private void RunOverrideListTaskWithPeriodicCommit()
        {
            var taskName = "OV_LPC" + _processes[_random.Next(25)];
            Console.WriteLine(taskName);
            using (var executionContext = SqlServerClientHelper.GetExecutionContextWithOverride(taskName, new TimeSpan(0, 1, 0)))
            {
                var startedOk = executionContext.TryStart();
                using (var cs = executionContext.CreateCriticalSection())
                {
                    cs.TryStart();

                }

                var values = GetList("PC", 1000);
                var blocks = executionContext.GetListBlocks(x => x.WithPeriodicCommit(values, 50, BatchSize.Hundred)
                    .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), new TimeSpan(0, 1, 0, 0))
                    .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0))
                    .MaximumBlocksToGenerate(50));

                foreach (var block in blocks)
                {
                    block.Start();
                    block.Complete();
                }
            }
        }

        private void RunOverrideListTaskWithBatchCommit()
        {
            var taskName = "OV_LBC" + _processes[_random.Next(25)];
            Console.WriteLine(taskName);
            using (var executionContext = SqlServerClientHelper.GetExecutionContextWithOverride(taskName, new TimeSpan(0, 1, 0)))
            {
                var startedOk = executionContext.TryStart();
                using (var cs = executionContext.CreateCriticalSection())
                {
                    cs.TryStart();

                }

                var values = GetList("BC", 1000);
                var blocks = executionContext.GetListBlocks(x => x.WithBatchCommitAtEnd(values, 50)
                    .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), new TimeSpan(0, 1, 0, 0))
                    .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0))
                    .MaximumBlocksToGenerate(50));

                foreach (var block in blocks)
                {
                    block.Start();
                    block.Complete();
                }
            }
        }

        private List<string> GetList(string prefix, int count)
        {
            var list = new List<string>();

            for (int i = 0; i < count; i++)
                list.Add(prefix + i);

            return list;
        }

        
    }
}
