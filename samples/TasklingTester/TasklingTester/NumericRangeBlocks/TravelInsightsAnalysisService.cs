using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Taskling;
using Taskling.Blocks.Common;
using Taskling.Contexts;

namespace TasklingTester.NumericRangeBlocks
{
    public class TravelInsightsAnalysisService
    {
        private ITasklingClient _tasklingClient;
        private IMyApplicationConfiguration _configuration;
        private IJourneysRepository _travelDataService;
        private ITravelInsightsRepository _travelInsightsService;

        public TravelInsightsAnalysisService(ITasklingClient tasklingClient,
            IMyApplicationConfiguration configuration,
            IJourneysRepository myTravelDataService,
            ITravelInsightsRepository travelInsightsService)
        {
            _tasklingClient = tasklingClient;
            _configuration = configuration;
            _travelDataService = myTravelDataService;
            _travelInsightsService = travelInsightsService;
        }

        public void RunBatchJob()
        {
            using (var taskExecutionContext = _tasklingClient.CreateTaskExecutionContext("MyApplication", "MyNumericBasedBatchJob"))
            {
                if (taskExecutionContext.TryStart())
                {
                    RunTask(taskExecutionContext);
                }
            }
        }

        private void RunTask(ITaskExecutionContext taskExecutionContext)
        {
            try
            {
                var numericRangeBlocks = GetNumericRangeBlocks(taskExecutionContext);
                foreach (var block in numericRangeBlocks)
                    ProcessBlock(block);

                taskExecutionContext.Complete();
            }
            catch (Exception ex)
            {
                taskExecutionContext.Error(ex.ToString(), true);
            }
        }

        private IList<INumericRangeBlockContext> GetNumericRangeBlocks(ITaskExecutionContext taskExecutionContext)
        {
            using (var cs = taskExecutionContext.CreateCriticalSection())
            {
                if (cs.TryStart())
                {
                    long startNumber;
                    var lastBlock = taskExecutionContext.GetLastNumericRangeBlock(LastBlockOrder.LastCreated);
                    var maxJourneyId = _travelDataService.GetMaxJourneyId();

                    // if this is the first run then just process the last 1000
                    if (lastBlock == null)
                    {
                        startNumber = maxJourneyId-1000;
                    }
                    // if there is no new data then just return any old blocks that have failed or died
                    else if (lastBlock.EndNumber == maxJourneyId)
                    {
                        return taskExecutionContext.GetNumericRangeBlocks(x => x.OnlyOldNumericBlocks());
                    }
                    // startNumber is the next unprocessed id
                    else
                    {
                        startNumber = lastBlock.EndNumber + 1;
                    }

                    int maxBlockSize = 500;
                    return taskExecutionContext.GetNumericRangeBlocks(x => x.WithRange(startNumber, maxJourneyId, maxBlockSize));
                }
                throw new Exception("Could not acquire a critical section, aborted task");
            }
        }

        private void ProcessBlock(INumericRangeBlockContext blockContext)
        {
            try
            {
                blockContext.Start();

                var journeys = _travelDataService.GetJourneys(blockContext.NumericRangeBlock.StartNumber, blockContext.NumericRangeBlock.EndNumber);
                var travelInsights = new List<TravelInsight>();

                foreach (var journey in journeys)
                {
                    var insight = new TravelInsight()
                    {
                        InsightDate = journey.TravelDate.Date,
                        InsightText = "Some useful insight",
                        PassengerName = journey.PassengerName
                    };

                    travelInsights.Add(insight);
                }

                _travelInsightsService.Add(travelInsights);

                int itemCountProcessed = travelInsights.Count;
                blockContext.Complete(itemCountProcessed);
            }
            catch (Exception ex)
            {
                blockContext.Failed(ex.ToString());
            }
        }
    }
}
