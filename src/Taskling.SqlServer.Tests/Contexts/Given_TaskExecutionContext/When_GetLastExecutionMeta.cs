using Xunit;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Taskling.SqlServer.Tests.Helpers;
using System.Threading.Tasks;
using Xunit.Abstractions;

namespace Taskling.SqlServer.Tests.Contexts.Given_TaskExecutionContext
{
    public class When_GetLastExecutionMeta
    {
        private int _taskDefinitionId;

        public When_GetLastExecutionMeta()
        {
            var executionHelper = new ExecutionsHelper();
            executionHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);

            _taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        }
        
        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "TaskExecutions")]
        public async Task If_MultipleExecutionsAndGetLastExecutionMeta_ThenReturnLastOne()
        {
            // ARRANGE

            for (int i = 0; i < 5; i++)
            {
                using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
                {
                    await executionContext.TryStartAsync("My reference value" + i);
                }
                Thread.Sleep(200);
            }

            // ACT and ASSERT
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var executionMeta = await executionContext.GetLastExecutionMetaAsync();
                Assert.Equal("My reference value4", executionMeta.ReferenceValue);
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "TaskExecutions")]
        public async Task If_MultipleExecutionsAndGetLastExecutionMetas_ThenReturnLastXItems()
        {
            // ARRANGE

            for (int i = 0; i < 5; i++)
            {
                using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
                {
                    await executionContext.TryStartAsync("My reference value" + i);
                }
                Thread.Sleep(200);
            }

            // ACT and ASSERT
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var executionMetas = await executionContext.GetLastExecutionMetasAsync(3);
                Assert.Equal(3, executionMetas.Count);
                Assert.Equal("My reference value4", executionMetas[0].ReferenceValue);
                Assert.Equal("My reference value3", executionMetas[1].ReferenceValue);
                Assert.Equal("My reference value2", executionMetas[2].ReferenceValue);
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "TaskExecutions")]
        public async Task If_NoPreviousExecutionsAndGetLastExecutionMeta_ThenReturnNull()
        {
            // ARRANGE

            // ACT and ASSERT
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var executionMeta = await executionContext.GetLastExecutionMetaAsync();
                Assert.Null(executionMeta);
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "TaskExecutions")]
        public async Task If_MultipleExecutionsAndGetLastExecutionMetaWithHeader_ThenReturnLastOne()
        {
            // ARRANGE

            for (int i = 0; i < 5; i++)
            {
                var myHeader = new MyHeader()
                {
                    Name = "Test",
                    Id = i
                };

                using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
                {
                    await executionContext.TryStartAsync(myHeader);
                }
                Thread.Sleep(200);
            }

            // ACT and ASSERT
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var executionMeta = await executionContext.GetLastExecutionMetaAsync<MyHeader>();
                Assert.Equal(4, executionMeta.Header.Id);
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "TaskExecutions")]
        public async Task If_MultipleExecutionsAndGetLastExecutionMetasWithHeader_ThenReturnLastXItems()
        {
            // ARRANGE

            for (int i = 0; i < 5; i++)
            {
                var myHeader = new MyHeader()
                {
                    Name = "Test",
                    Id = i
                };

                using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
                {
                    await executionContext.TryStartAsync(myHeader);
                }
                Thread.Sleep(200);
            }

            // ACT and ASSERT
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var executionMetas = await executionContext.GetLastExecutionMetasAsync<MyHeader>(3);
                Assert.Equal(3, executionMetas.Count);
                Assert.Equal(4, executionMetas[0].Header.Id);
                Assert.Equal(3, executionMetas[1].Header.Id);
                Assert.Equal(2, executionMetas[2].Header.Id);
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "TaskExecutions")]
        public async Task If_NoPreviousExecutionsAndGetLastExecutionMetaWithHeader_ThenReturnNull()
        {
            // ARRANGE
            // ACT

            // ASSERT
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var executionMeta = await executionContext.GetLastExecutionMetaAsync<MyHeader>();
                Assert.Null(executionMeta);
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "TaskExecutions")]
        public async Task If_LastExecutionCompleted_ThenReturnStatusIsCompleted()
        {
            // ARRANGE
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                await executionContext.TryStartAsync();
            }
            Thread.Sleep(200);


            // ACT and ASSERT
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var executionMeta = await executionContext.GetLastExecutionMetaAsync();
                Assert.Equal(Taskling.Tasks.TaskExecutionStatus.Completed, executionMeta.Status);
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "TaskExecutions")]
        public async Task If_LastExecutionFailed_ThenReturnStatusIsFailed()
        {
            // ARRANGE
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                await executionContext.TryStartAsync();
                await executionContext.ErrorAsync("", true);
            }
            Thread.Sleep(200);


            // ACT and ASSERT
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var executionMeta = await executionContext.GetLastExecutionMetaAsync();
                Assert.Equal(Taskling.Tasks.TaskExecutionStatus.Failed, executionMeta.Status);
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "TaskExecutions")]
        public async Task If_LastExecutionBlocked_ThenReturnStatusIsBlockedAsync()
        {
            // ARRANGE
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                await executionContext.TryStartAsync();
                Thread.Sleep(200);

                using (var executionContext2 = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
                {
                    await executionContext2.TryStartAsync();
                    await executionContext2.CompleteAsync();
                }

                await executionContext.CompleteAsync();
            }



            // ACT and ASSERT
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var executionMeta = await executionContext.GetLastExecutionMetaAsync();
                Assert.Equal(Taskling.Tasks.TaskExecutionStatus.Blocked, executionMeta.Status);

                await executionContext.CompleteAsync();
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "TaskExecutions")]
        public async Task If_LastExecutionInProgress_ThenReturnStatusIsInProgress()
        {
            // ARRANGE, ACT, ASSERT
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                await executionContext.TryStartAsync();
                Thread.Sleep(200);

                using (var executionContext2 = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
                {
                    var executionMeta = await executionContext2.GetLastExecutionMetaAsync();
                    Assert.Equal(Taskling.Tasks.TaskExecutionStatus.InProgress, executionMeta.Status);
                }
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "TaskExecutions")]
        public async Task If_LastExecutionDead_ThenReturnStatusIsDead()
        {
            // ARRANGE, ACT, ASSERT
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                await executionContext.TryStartAsync();
                Thread.Sleep(200);
                var helper = new ExecutionsHelper();
                helper.SetLastExecutionAsDead(_taskDefinitionId);

                using (var executionContext2 = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
                {
                    var executionMeta = await executionContext2.GetLastExecutionMetaAsync();
                    Assert.Equal(Taskling.Tasks.TaskExecutionStatus.Dead, executionMeta.Status);
                }
            }
        }
    }
}
