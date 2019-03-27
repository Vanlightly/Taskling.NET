using Xunit;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Taskling.Blocks.ListBlocks;
using Taskling.SqlServer.Tests.Helpers;

namespace Taskling.SqlServer.Tests.Contexts.StressTest
{
    public class TasklingStressTest
    {
        private List<string> _processes = new List<string>()
        {
            "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"
        };

        private Random _random = new Random();

        [Fact]
        [Trait("Speed", "Slow")]
        public void StartStressTest()
        {
            CreateTasksAndExecutionTokens();
            var tasks = new List<Task>();
            for (int i = 0; i < 10; i++)
                tasks.Add(Task.Factory.StartNew(async () => await RunRandomTaskAsync(20)));

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

        private async Task RunRandomTaskAsync(int repeatCount)
        {
            for (int i = 0; i < repeatCount; i++)
            {
                var num = _random.Next(9);
                switch (num)
                {
                    case 0:
                        await RunKeepAliveDateRangeTaskAsync();
                        break;
                    case 1:
                        await RunKeepAliveNumericRangeTaskAsync();
                        break;
                    case 2:
                        await RunKeepAliveListTaskWithSingleUnitCommitAsync();
                        break;
                    case 3:
                        await RunKeepAliveListTaskWithPeriodicCommitAsync();
                        break;
                    case 4:
                        await RunKeepAliveListTaskWithBatchCommitAsync();
                        break;
                    case 5:
                        await RunOverrideDateRangeTaskAsync();
                        break;
                    case 6:
                        await RunOverrideNumericRangeTaskAsync();
                        break;
                    case 7:
                        await RunOverrideListTaskWithSingleUnitCommitAsync();
                        break;
                    case 8:
                        await RunOverrideListTaskWithPeriodicCommitAsync();
                        break;
                    case 9:
                        await RunOverrideListTaskWithBatchCommitAsync();
                        break;
                }
            }
        }

        private async Task RunKeepAliveDateRangeTaskAsync()
        {
            var taskName = "DR_" + _processes[_random.Next(25)];
            Console.WriteLine(taskName);
            using (var executionContext = ClientHelper.GetExecutionContext(taskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    using (var cs = executionContext.CreateCriticalSection())
                    {
                        await cs.TryStartAsync();

                    }

                    var blocks =
                        await executionContext.GetDateRangeBlocksAsync(
                            x => x.WithRange(DateTime.UtcNow.AddDays(-1), DateTime.UtcNow, new TimeSpan(0, 1, 0, 0))
                                .OverrideConfiguration()
                                .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                                .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                                .MaximumBlocksToGenerate(50));

                    foreach (var block in blocks)
                    {
                        await block.StartAsync();
                        await block.CompleteAsync();
                    }
                }
            }
        }

        private async Task RunKeepAliveNumericRangeTaskAsync()
        {
            var taskName = "NR_" + _processes[_random.Next(25)];
            Console.WriteLine(taskName);
            using (var executionContext = ClientHelper.GetExecutionContext(taskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    using (var cs = executionContext.CreateCriticalSection())
                    {
                        await cs.TryStartAsync();

                    }

                    var blocks = await executionContext.GetNumericRangeBlocksAsync(x => x.WithRange(1, 10000, 100)
                        .OverrideConfiguration()
                        .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .MaximumBlocksToGenerate(50));

                    foreach (var block in blocks)
                    {
                        await block.StartAsync();
                        await block.CompleteAsync();
                    }
                }
            }
        }

        private async Task RunKeepAliveListTaskWithSingleUnitCommitAsync()
        {
            var taskName = "LSUC_" + _processes[_random.Next(25)];
            Console.WriteLine(taskName);
            using (var executionContext = ClientHelper.GetExecutionContext(taskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    using (var cs = executionContext.CreateCriticalSection())
                    {
                        await cs.TryStartAsync();

                    }

                    var values = GetList("SUC", 1000);
                    var blocks = await executionContext.GetListBlocksAsync<PersonDto>(x => x.WithSingleUnitCommit(values, 50)
                        .OverrideConfiguration()
                        .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .MaximumBlocksToGenerate(50));

                    foreach (var block in blocks)
                    {
                        await block.StartAsync();
                        await block.CompleteAsync();
                    }
                }
            }
        }

        private async Task RunKeepAliveListTaskWithPeriodicCommitAsync()
        {
            var taskName = "LPC_" + _processes[_random.Next(25)];
            Console.WriteLine(taskName);
            using (var executionContext = ClientHelper.GetExecutionContext(taskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    using (var cs = executionContext.CreateCriticalSection())
                    {
                        await cs.TryStartAsync();

                    }

                    var values = GetList("PC", 1000);
                    var blocks = await executionContext.GetListBlocksAsync<PersonDto>(x => x.WithPeriodicCommit(values, 50, BatchSize.Hundred)
                        .OverrideConfiguration()
                        .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .MaximumBlocksToGenerate(50));

                    foreach (var block in blocks)
                    {
                        await block.StartAsync();
                        await block.CompleteAsync();
                    }
                }
            }
        }

        private async Task RunKeepAliveListTaskWithBatchCommitAsync()
        {
            var taskName = "LBC_" + _processes[_random.Next(25)];
            Console.WriteLine(taskName);
            using (var executionContext = ClientHelper.GetExecutionContext(taskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    using (var cs = executionContext.CreateCriticalSection())
                    {
                        await cs.TryStartAsync();

                    }

                    var values = GetList("BC", 1000);
                    var blocks = await executionContext.GetListBlocksAsync<PersonDto>(x => x.WithBatchCommitAtEnd(values, 50)
                        .OverrideConfiguration()
                        .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .MaximumBlocksToGenerate(50));

                    foreach (var block in blocks)
                    {
                        await block.StartAsync();
                        await block.CompleteAsync();
                    }
                }
            }
        }


        private async Task RunOverrideDateRangeTaskAsync()
        {
            var taskName = "OV_DR_" + _processes[_random.Next(25)];
            Console.WriteLine(taskName);
            using (var executionContext = ClientHelper.GetExecutionContext(taskName, ClientHelper.GetDefaultTaskConfigurationWithTimePeriodOverrideAndReprocessing()))
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    using (var cs = executionContext.CreateCriticalSection())
                    {
                        await cs.TryStartAsync();

                    }

                    var blocks =
                        await executionContext.GetDateRangeBlocksAsync(
                            x => x.WithRange(DateTime.UtcNow.AddDays(-1), DateTime.UtcNow, new TimeSpan(0, 1, 0, 0))
                                .OverrideConfiguration()
                                .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                                .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                                .MaximumBlocksToGenerate(50));

                    foreach (var block in blocks)
                    {
                        await block.StartAsync();
                        await block.CompleteAsync();
                    }
                }
            }
        }

        private async Task RunOverrideNumericRangeTaskAsync()
        {
            var taskName = "OV_NR_" + _processes[_random.Next(25)];
            Console.WriteLine(taskName);
            using (var executionContext = ClientHelper.GetExecutionContext(taskName, ClientHelper.GetDefaultTaskConfigurationWithTimePeriodOverrideAndReprocessing()))
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    using (var cs = executionContext.CreateCriticalSection())
                    {
                        await cs.TryStartAsync();

                    }

                    var blocks = await executionContext.GetNumericRangeBlocksAsync(x => x.WithRange(1, 10000, 100)
                        .OverrideConfiguration()
                        .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .MaximumBlocksToGenerate(50));

                    foreach (var block in blocks)
                    {
                        await block.StartAsync();
                        await block.CompleteAsync();
                    }
                }
            }
        }

        private async Task RunOverrideListTaskWithSingleUnitCommitAsync()
        {
            var taskName = "OV_LSUC_" + _processes[_random.Next(25)];
            Console.WriteLine(taskName);
            using (var executionContext = ClientHelper.GetExecutionContext(taskName, ClientHelper.GetDefaultTaskConfigurationWithTimePeriodOverrideAndReprocessing()))
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    using (var cs = executionContext.CreateCriticalSection())
                    {
                        await cs.TryStartAsync();

                    }

                    var values = GetList("SUC", 1000);
                    var blocks = await executionContext.GetListBlocksAsync<PersonDto>(x => x.WithSingleUnitCommit(values, 50)
                        .OverrideConfiguration()
                        .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .MaximumBlocksToGenerate(50));

                    foreach (var block in blocks)
                    {
                        await block.StartAsync();
                        await block.CompleteAsync();
                    }
                }
            }
        }

        private async Task RunOverrideListTaskWithPeriodicCommitAsync()
        {
            var taskName = "OV_LPC_" + _processes[_random.Next(25)];
            Console.WriteLine(taskName);
            using (var executionContext = ClientHelper.GetExecutionContext(taskName, ClientHelper.GetDefaultTaskConfigurationWithTimePeriodOverrideAndReprocessing()))
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    using (var cs = executionContext.CreateCriticalSection())
                    {
                        await cs.TryStartAsync();
                    }

                    var values = GetList("PC", 1000);
                    var blocks = await executionContext.GetListBlocksAsync<PersonDto>(x => x.WithPeriodicCommit(values, 50, BatchSize.Hundred)
                        .OverrideConfiguration()
                        .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .MaximumBlocksToGenerate(50));

                    foreach (var block in blocks)
                    {
                        await block.StartAsync();
                        await block.CompleteAsync();
                    }
                }
            }
        }

        private async Task RunOverrideListTaskWithBatchCommitAsync()
        {
            var taskName = "OV_LBC_" + _processes[_random.Next(25)];
            Console.WriteLine(taskName);
            using (var executionContext = ClientHelper.GetExecutionContext(taskName, ClientHelper.GetDefaultTaskConfigurationWithTimePeriodOverrideAndReprocessing()))
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    using (var cs = executionContext.CreateCriticalSection())
                    {
                        await cs.TryStartAsync();

                    }

                    var values = GetList("BC", 1000);
                    var blocks = await executionContext.GetListBlocksAsync<PersonDto>(x => x.WithBatchCommitAtEnd(values, 50)
                        .OverrideConfiguration()
                        .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .MaximumBlocksToGenerate(50));

                    foreach (var block in blocks)
                    {
                        await block.StartAsync();
                        await block.CompleteAsync();
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
