using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Taskling;
using Taskling.Blocks.Common;
using Taskling.Contexts;
using TasklingTesterAsync.Repositories;

namespace TasklingTesterAsync.DateRangeBlocks
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
            using (var taskExecutionContext = _tasklingClient.CreateTaskExecutionContext("MyApplication", "MyDateBasedBatchJob"))
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
                var dateRangeBlocks = await GetDateRangeBlocksAsync(taskExecutionContext);
                foreach (var block in dateRangeBlocks)
                    await ProcessBlockAsync(block);

                await taskExecutionContext.CompleteAsync();
            }
            catch (Exception ex)
            {
                await taskExecutionContext.ErrorAsync(ex.ToString(), true);
            }
        }

        private async Task<IList<IDateRangeBlockContext>> GetDateRangeBlocksAsync(ITaskExecutionContext taskExecutionContext)
        {
            using (var cs = taskExecutionContext.CreateCriticalSection())
            {
                if (await cs.TryStartAsync())
                {
                    var startDate = await GetDateRangeStartDateAsync(taskExecutionContext);
                    var endDate = DateTime.Now;

                    return await taskExecutionContext.GetDateRangeBlocksAsync(x => x.WithRange(startDate, endDate, TimeSpan.FromMinutes(30)));
                }
                throw new Exception("Could not acquire a critical section, aborted task");
            }
        }

        private async Task<DateTime> GetDateRangeStartDateAsync(ITaskExecutionContext taskExecutionContext)
        {
            var lastBlock = await taskExecutionContext.GetLastDateRangeBlockAsync(LastBlockOrder.LastCreated);
            if (lastBlock == null)
                return _configuration.FirstRunDate;
            else
                return lastBlock.EndDate;
        }

        private async Task ProcessBlockAsync(IDateRangeBlockContext blockContext)
        {
            try
            {
                await blockContext.StartAsync();

                var journeys = await _travelDataService.GetJourneysAsync(blockContext.DateRangeBlock.StartDate, blockContext.DateRangeBlock.EndDate);
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
