using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Taskling.Blocks.ListBlocks;
using Taskling.SqlServer.IntegrationTest.Helpers;

namespace Taskling.SqlServer.IntegrationTest.Contexts.StressTest
{
    [TestClass]
    public class TasklingStressTest
    {
        private List<string> _processes = new List<string>()
        {
            "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"
        };

        private Random _random = new Random();

        [TestMethod]
        [TestCategory("SlowIntegrationTest")]
        public void StartStressTest()
        {
            CreateTasksAndExecutionTokens();
            var tasks = new List<Task>();
            for (int i = 0; i < 10; i++)
                tasks.Add(Task.Factory.StartNew(() => RunRandomTask(20)));

            Task.WaitAll(tasks.ToArray());
        }

        private void CreateTasksAndExecutionTokens()
        {
            var executionHelper = new ExecutionsHelper();
            executionHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);
            foreach (var process in _processes)
            {
                int drSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, "DR_" + process);
                int nrSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, "NR_" + process);
                int lsucSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, "LSUC_" + process);
                int lpcSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, "LPC_" + process);
                int lbcSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, "LBC_" + process);

                int ovdrSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, "OV_DR_" + process);
                int ovnrSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, "OV_NR_" + process);
                int ovlsucSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, "OV_LSUC_" + process);
                int ovlpcSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, "OV_LPC_" + process);
                int ovlbcSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, "OV_LBC_" + process);

                executionHelper.InsertAvailableExecutionToken(drSecondaryId);
                executionHelper.InsertAvailableExecutionToken(nrSecondaryId);
                executionHelper.InsertAvailableExecutionToken(lsucSecondaryId);
                executionHelper.InsertAvailableExecutionToken(lpcSecondaryId);
                executionHelper.InsertAvailableExecutionToken(lbcSecondaryId);

                executionHelper.InsertAvailableExecutionToken(ovdrSecondaryId);
                executionHelper.InsertAvailableExecutionToken(ovnrSecondaryId);
                executionHelper.InsertAvailableExecutionToken(ovlsucSecondaryId);
                executionHelper.InsertAvailableExecutionToken(ovlpcSecondaryId);
                executionHelper.InsertAvailableExecutionToken(ovlbcSecondaryId);
            }
        }

        private void RunRandomTask(int repeatCount)
        {
            for (int i = 0; i < repeatCount; i++)
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
            var taskName = "DR_" + _processes[_random.Next(25)];
            Console.WriteLine(taskName);
            using (var executionContext = ClientHelper.GetExecutionContext(taskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    using (var cs = executionContext.CreateCriticalSection())
                    {
                        cs.TryStart();

                    }

                    var blocks =
                        executionContext.GetDateRangeBlocks(
                            x => x.WithRange(DateTime.UtcNow.AddDays(-1), DateTime.UtcNow, new TimeSpan(0, 1, 0, 0))
                                .OverrideConfiguration()
                                .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                                .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                                .MaximumBlocksToGenerate(50));

                    foreach (var block in blocks)
                    {
                        block.Start();
                        block.Complete();
                    }
                }
            }
        }

        private void RunKeepAliveNumericRangeTask()
        {
            var taskName = "NR_" + _processes[_random.Next(25)];
            Console.WriteLine(taskName);
            using (var executionContext = ClientHelper.GetExecutionContext(taskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    using (var cs = executionContext.CreateCriticalSection())
                    {
                        cs.TryStart();

                    }

                    var blocks = executionContext.GetNumericRangeBlocks(x => x.WithRange(1, 10000, 100)
                        .OverrideConfiguration()
                        .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .MaximumBlocksToGenerate(50));

                    foreach (var block in blocks)
                    {
                        block.Start();
                        block.Complete();
                    }
                }
            }
        }

        private void RunKeepAliveListTaskWithSingleUnitCommit()
        {
            var taskName = "LSUC_" + _processes[_random.Next(25)];
            Console.WriteLine(taskName);
            using (var executionContext = ClientHelper.GetExecutionContext(taskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    using (var cs = executionContext.CreateCriticalSection())
                    {
                        cs.TryStart();

                    }

                    var values = GetList("SUC", 1000);
                    var blocks = executionContext.GetListBlocks<PersonDto>(x => x.WithSingleUnitCommit(values, 50)
                        .OverrideConfiguration()
                        .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .MaximumBlocksToGenerate(50));

                    foreach (var block in blocks)
                    {
                        block.Start();
                        block.Complete();
                    }
                }
            }
        }

        private void RunKeepAliveListTaskWithPeriodicCommit()
        {
            var taskName = "LPC_" + _processes[_random.Next(25)];
            Console.WriteLine(taskName);
            using (var executionContext = ClientHelper.GetExecutionContext(taskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    using (var cs = executionContext.CreateCriticalSection())
                    {
                        cs.TryStart();

                    }

                    var values = GetList("PC", 1000);
                    var blocks = executionContext.GetListBlocks<PersonDto>(x => x.WithPeriodicCommit(values, 50, BatchSize.Hundred)
                        .OverrideConfiguration()
                        .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .MaximumBlocksToGenerate(50));

                    foreach (var block in blocks)
                    {
                        block.Start();
                        block.Complete();
                    }
                }
            }
        }

        private void RunKeepAliveListTaskWithBatchCommit()
        {
            var taskName = "LBC_" + _processes[_random.Next(25)];
            Console.WriteLine(taskName);
            using (var executionContext = ClientHelper.GetExecutionContext(taskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    using (var cs = executionContext.CreateCriticalSection())
                    {
                        cs.TryStart();

                    }

                    var values = GetList("BC", 1000);
                    var blocks = executionContext.GetListBlocks<PersonDto>(x => x.WithBatchCommitAtEnd(values, 50)
                        .OverrideConfiguration()
                        .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .MaximumBlocksToGenerate(50));

                    foreach (var block in blocks)
                    {
                        block.Start();
                        block.Complete();
                    }
                }
            }
        }


        private void RunOverrideDateRangeTask()
        {
            var taskName = "OV_DR_" + _processes[_random.Next(25)];
            Console.WriteLine(taskName);
            using (var executionContext = ClientHelper.GetExecutionContext(taskName, ClientHelper.GetDefaultTaskConfigurationWithTimePeriodOverrideAndReprocessing()))
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    using (var cs = executionContext.CreateCriticalSection())
                    {
                        cs.TryStart();

                    }

                    var blocks =
                        executionContext.GetDateRangeBlocks(
                            x => x.WithRange(DateTime.UtcNow.AddDays(-1), DateTime.UtcNow, new TimeSpan(0, 1, 0, 0))
                                .OverrideConfiguration()
                                .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                                .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                                .MaximumBlocksToGenerate(50));

                    foreach (var block in blocks)
                    {
                        block.Start();
                        block.Complete();
                    }
                }
            }
        }

        private void RunOverrideNumericRangeTask()
        {
            var taskName = "OV_NR_" + _processes[_random.Next(25)];
            Console.WriteLine(taskName);
            using (var executionContext = ClientHelper.GetExecutionContext(taskName, ClientHelper.GetDefaultTaskConfigurationWithTimePeriodOverrideAndReprocessing()))
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    using (var cs = executionContext.CreateCriticalSection())
                    {
                        cs.TryStart();

                    }

                    var blocks = executionContext.GetNumericRangeBlocks(x => x.WithRange(1, 10000, 100)
                        .OverrideConfiguration()
                        .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .MaximumBlocksToGenerate(50));

                    foreach (var block in blocks)
                    {
                        block.Start();
                        block.Complete();
                    }
                }
            }
        }

        private void RunOverrideListTaskWithSingleUnitCommit()
        {
            var taskName = "OV_LSUC_" + _processes[_random.Next(25)];
            Console.WriteLine(taskName);
            using (var executionContext = ClientHelper.GetExecutionContext(taskName, ClientHelper.GetDefaultTaskConfigurationWithTimePeriodOverrideAndReprocessing()))
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    using (var cs = executionContext.CreateCriticalSection())
                    {
                        cs.TryStart();

                    }

                    var values = GetList("SUC", 1000);
                    var blocks = executionContext.GetListBlocks<PersonDto>(x => x.WithSingleUnitCommit(values, 50)
                        .OverrideConfiguration()
                        .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .MaximumBlocksToGenerate(50));

                    foreach (var block in blocks)
                    {
                        block.Start();
                        block.Complete();
                    }
                }
            }
        }

        private void RunOverrideListTaskWithPeriodicCommit()
        {
            var taskName = "OV_LPC_" + _processes[_random.Next(25)];
            Console.WriteLine(taskName);
            using (var executionContext = ClientHelper.GetExecutionContext(taskName, ClientHelper.GetDefaultTaskConfigurationWithTimePeriodOverrideAndReprocessing()))
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    using (var cs = executionContext.CreateCriticalSection())
                    {
                        cs.TryStart();

                    }

                    var values = GetList("PC", 1000);
                    var blocks = executionContext.GetListBlocks<PersonDto>(x => x.WithPeriodicCommit(values, 50, BatchSize.Hundred)
                        .OverrideConfiguration()
                        .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .MaximumBlocksToGenerate(50));

                    foreach (var block in blocks)
                    {
                        block.Start();
                        block.Complete();
                    }
                }
            }
        }

        private void RunOverrideListTaskWithBatchCommit()
        {
            var taskName = "OV_LBC_" + _processes[_random.Next(25)];
            Console.WriteLine(taskName);
            using (var executionContext = ClientHelper.GetExecutionContext(taskName, ClientHelper.GetDefaultTaskConfigurationWithTimePeriodOverrideAndReprocessing()))
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    using (var cs = executionContext.CreateCriticalSection())
                    {
                        cs.TryStart();

                    }

                    var values = GetList("BC", 1000);
                    var blocks = executionContext.GetListBlocks<PersonDto>(x => x.WithBatchCommitAtEnd(values, 50)
                        .OverrideConfiguration()
                        .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .MaximumBlocksToGenerate(50));

                    foreach (var block in blocks)
                    {
                        block.Start();
                        block.Complete();
                    }
                }
            }
        }

        private List<PersonDto> GetList(string prefix, int count)
        {
            var list = new List<PersonDto>();

            for (int i = 0; i < count; i++)
                list.Add(new PersonDto() { DateOfBirth = DateTime.Now, Id = i.ToString(), Name = prefix + i });

            return list;
        }


    }
}
