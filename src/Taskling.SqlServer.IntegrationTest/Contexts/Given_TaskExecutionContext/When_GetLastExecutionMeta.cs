using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Taskling.SqlServer.IntegrationTest.Helpers;

namespace Taskling.SqlServer.IntegrationTest.Contexts.Given_TaskExecutionContext
{
    [TestClass]
    public class When_GetLastExecutionMeta
    {
        private int _taskDefinitionId;

        [TestInitialize]
        public void Initialize()
        {
            var executionHelper = new ExecutionsHelper();
            executionHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);

            _taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("TaskExecutions")]
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
                Assert.AreEqual("My reference value4", executionMeta.ReferenceValue);
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("TaskExecutions")]
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
                Assert.AreEqual(3, executionMetas.Count);
                Assert.AreEqual("My reference value4", executionMetas[0].ReferenceValue);
                Assert.AreEqual("My reference value3", executionMetas[1].ReferenceValue);
                Assert.AreEqual("My reference value2", executionMetas[2].ReferenceValue);
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("TaskExecutions")]
        public void If_NoPreviousExecutionsAndGetLastExecutionMeta_ThenReturnNull()
        {
            // ARRANGE

            // ACT and ASSERT
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var executionMeta = executionContext.GetLastExecutionMeta();
                Assert.IsNull(executionMeta);
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("TaskExecutions")]
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
                Assert.AreEqual(4, executionMeta.Header.Id);
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("TaskExecutions")]
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
                Assert.AreEqual(3, executionMetas.Count);
                Assert.AreEqual(4, executionMetas[0].Header.Id);
                Assert.AreEqual(3, executionMetas[1].Header.Id);
                Assert.AreEqual(2, executionMetas[2].Header.Id);
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("TaskExecutions")]
        public void If_NoPreviousExecutionsAndGetLastExecutionMetaWithHeader_ThenReturnNull()
        {
            // ARRANGE
            // ACT

            // ASSERT
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                var executionMeta = executionContext.GetLastExecutionMeta<MyHeader>();
                Assert.IsNull(executionMeta);
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("TaskExecutions")]
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
                Assert.AreEqual(Taskling.Tasks.TaskExecutionStatus.Completed, executionMeta.Status);
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("TaskExecutions")]
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
                Assert.AreEqual(Taskling.Tasks.TaskExecutionStatus.Failed, executionMeta.Status);
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("TaskExecutions")]
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
                Assert.AreEqual(Taskling.Tasks.TaskExecutionStatus.Blocked, executionMeta.Status);
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("TaskExecutions")]
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
                    Assert.AreEqual(Taskling.Tasks.TaskExecutionStatus.InProgress, executionMeta.Status);
                }
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("TaskExecutions")]
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
                    Assert.AreEqual(Taskling.Tasks.TaskExecutionStatus.Dead, executionMeta.Status);
                }
            }
        }
    }
}
