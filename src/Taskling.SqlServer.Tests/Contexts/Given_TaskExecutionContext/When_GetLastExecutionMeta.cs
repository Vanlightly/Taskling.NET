using Xunit;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Taskling.SqlServer.Tests.Helpers;

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
        public void If_MultipleExecutionsAndGetLastExecutionMeta_ThenReturnLastOne()
        {
            // ARRANGE

            for (int i = 0; i < 5; i++)
            {
                using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
                {
                    executionContext.TryStart("My reference value" + i);
                }
                Thread.Sleep(200);
            }

            // ACT and ASSERT
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var executionMeta = executionContext.GetLastExecutionMeta();
                Assert.Equal("My reference value4", executionMeta.ReferenceValue);
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "TaskExecutions")]
        public void If_MultipleExecutionsAndGetLastExecutionMetas_ThenReturnLastXItems()
        {
            // ARRANGE

            for (int i = 0; i < 5; i++)
            {
                using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
                {
                    executionContext.TryStart("My reference value" + i);
                }
                Thread.Sleep(200);
            }

            // ACT and ASSERT
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var executionMetas = executionContext.GetLastExecutionMetas(3);
                Assert.Equal(3, executionMetas.Count);
                Assert.Equal("My reference value4", executionMetas[0].ReferenceValue);
                Assert.Equal("My reference value3", executionMetas[1].ReferenceValue);
                Assert.Equal("My reference value2", executionMetas[2].ReferenceValue);
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "TaskExecutions")]
        public void If_NoPreviousExecutionsAndGetLastExecutionMeta_ThenReturnNull()
        {
            // ARRANGE

            // ACT and ASSERT
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var executionMeta = executionContext.GetLastExecutionMeta();
                Assert.Null(executionMeta);
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "TaskExecutions")]
        public void If_MultipleExecutionsAndGetLastExecutionMetaWithHeader_ThenReturnLastOne()
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
                    executionContext.TryStart(myHeader);
                }
                Thread.Sleep(200);
            }

            // ACT and ASSERT
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var executionMeta = executionContext.GetLastExecutionMeta<MyHeader>();
                Assert.Equal(4, executionMeta.Header.Id);
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "TaskExecutions")]
        public void If_MultipleExecutionsAndGetLastExecutionMetasWithHeader_ThenReturnLastXItems()
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
                    executionContext.TryStart(myHeader);
                }
                Thread.Sleep(200);
            }

            // ACT and ASSERT
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var executionMetas = executionContext.GetLastExecutionMetas<MyHeader>(3);
                Assert.Equal(3, executionMetas.Count);
                Assert.Equal(4, executionMetas[0].Header.Id);
                Assert.Equal(3, executionMetas[1].Header.Id);
                Assert.Equal(2, executionMetas[2].Header.Id);
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "TaskExecutions")]
        public void If_NoPreviousExecutionsAndGetLastExecutionMetaWithHeader_ThenReturnNull()
        {
            // ARRANGE
            // ACT

            // ASSERT
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var executionMeta = executionContext.GetLastExecutionMeta<MyHeader>();
                Assert.Null(executionMeta);
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "TaskExecutions")]
        public void If_LastExecutionCompleted_ThenReturnStatusIsCompleted()
        {
            // ARRANGE
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                executionContext.TryStart();
            }
            Thread.Sleep(200);


            // ACT and ASSERT
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var executionMeta = executionContext.GetLastExecutionMeta();
                Assert.Equal(Taskling.Tasks.TaskExecutionStatus.Completed, executionMeta.Status);
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "TaskExecutions")]
        public void If_LastExecutionFailed_ThenReturnStatusIsFailed()
        {
            // ARRANGE
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                executionContext.TryStart();
                executionContext.Error("", true);
            }
            Thread.Sleep(200);


            // ACT and ASSERT
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var executionMeta = executionContext.GetLastExecutionMeta();
                Assert.Equal(Taskling.Tasks.TaskExecutionStatus.Failed, executionMeta.Status);
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "TaskExecutions")]
        public void If_LastExecutionBlocked_ThenReturnStatusIsBlocked()
        {
            // ARRANGE
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                executionContext.TryStart();
                Thread.Sleep(200);

                using (var executionContext2 = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
                {
                    executionContext2.TryStart();
                }
            }



            // ACT and ASSERT
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var executionMeta = executionContext.GetLastExecutionMeta();
                Assert.Equal(Taskling.Tasks.TaskExecutionStatus.Blocked, executionMeta.Status);
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "TaskExecutions")]
        public void If_LastExecutionInProgress_ThenReturnStatusIsInProgress()
        {
            // ARRANGE, ACT, ASSERT
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                executionContext.TryStart();
                Thread.Sleep(200);

                using (var executionContext2 = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
                {
                    var executionMeta = executionContext2.GetLastExecutionMeta();
                    Assert.Equal(Taskling.Tasks.TaskExecutionStatus.InProgress, executionMeta.Status);
                }
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "TaskExecutions")]
        public void If_LastExecutionDead_ThenReturnStatusIsDead()
        {
            // ARRANGE, ACT, ASSERT
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                executionContext.TryStart();
                Thread.Sleep(200);
                var helper = new ExecutionsHelper();
                helper.SetLastExecutionAsDead(_taskDefinitionId);

                using (var executionContext2 = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
                {
                    var executionMeta = executionContext2.GetLastExecutionMeta();
                    Assert.Equal(Taskling.Tasks.TaskExecutionStatus.Dead, executionMeta.Status);
                }
            }
        }
    }
}
