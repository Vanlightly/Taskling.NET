using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Taskling;
using Taskling.Blocks.Common;
using Taskling.Contexts;
using TasklingTesterAsync.Repositories;

namespace TasklingTesterAsync.NumericRangeBlocks
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

        public async Task RunBatchJobAsync()
        {
            using (var taskExecutionContext = _tasklingClient.CreateTaskExecutionContext("MyApplication", "MyNumericBasedBatchJob"))
            {
                if (await taskExecutionContext.TryStartAsync())
                {
                    await RunTaskAsync(taskExecutionContext);
                }
            }
        }

        private async Task RunTaskAsync(ITaskExecutionContext taskExecutionContext)
        {
            try
            {
                var numericRangeBlocks = await GetNumericRangeBlocksAsync(taskExecutionContext);
                foreach (var block in numericRangeBlocks)
                    await ProcessBlockAsync(block);

                await taskExecutionContext.CompleteAsync();
            }
            catch (Exception ex)
            {
                await taskExecutionContext.ErrorAsync(ex.ToString(), true);
            }
        }

        private async Task<IList<INumericRangeBlockContext>> GetNumericRangeBlocksAsync(ITaskExecutionContext taskExecutionContext)
        {
            using (var cs = taskExecutionContext.CreateCriticalSection())
            {
                if (await cs.TryStartAsync())
                {
                    long startNumber;
                    var lastBlock = await taskExecutionContext.GetLastNumericRangeBlockAsync(LastBlockOrder.LastCreated);
                    var maxJourneyId = await _travelDataService.GetMaxJourneyIdAsync();

                    // if this is the first run then just process the last 1000
                    if (lastBlock == null)
                    {
                        startNumber = maxJourneyId-1000;
                    }
                    // if there is no new data then just return any old blocks that have failed or died
                    else if (lastBlock.EndNumber == maxJourneyId)
                    {
                        return await taskExecutionContext.GetNumericRangeBlocksAsync(x => x.OnlyOldNumericBlocks());
                    }
                    // startNumber is the next unprocessed id
                    else
                    {
                        startNumber = lastBlock.EndNumber + 1;
                    }

                    int maxBlockSize = 500;
                    return await taskExecutionContext.GetNumericRangeBlocksAsync(x => x.WithRange(startNumber, maxJourneyId, maxBlockSize));
                }
                throw new Exception("Could not acquire a critical section, aborted task");
            }
        }

        private async Task ProcessBlockAsync(INumericRangeBlockContext blockContext)
        {
            try
            {
                await blockContext.StartAsync();

                var journeys = await _travelDataService.GetJourneysAsync(blockContext.NumericRangeBlock.StartNumber, blockContext.NumericRangeBlock.EndNumber);
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

                await _travelInsightsService.AddAsync(travelInsights);

                int itemCountProcessed = travelInsights.Count;
                await blockContext.CompleteAsync(itemCountProcessed);
            }
            catch (Exception ex)
            {
                await blockContext.FailedAsync(ex.ToString());
            }
        }
    }
}
