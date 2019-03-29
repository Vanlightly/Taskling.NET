using System;
using System.Threading.Tasks;
using Taskling.SqlServer;
using TasklingTesterAsync.Configuration;
using TasklingTesterAsync.ListBlocks;
using TasklingTesterAsync.Repositories;

namespace TasklingTesterAsync
{
    class Program
    {
        static void Main(string[] args)
        {
            MainAsync(args).GetAwaiter().GetResult();
        }

        static async Task MainAsync(string[] args)
        { 
            //var insightService = GetDateRangeInsightService();
            //await insightService.RunBatchJobAsync();

            //var insightService = GetNumericRangeInsightService();
            //await insightService.RunBatchJobAsync();

            var insightService = GetListInsightService();
            await insightService.RunBatchJobAsync();
        }

        private static DateRangeBlocks.TravelInsightsAnalysisService GetDateRangeInsightService()
        {
            return new DateRangeBlocks.TravelInsightsAnalysisService(new TasklingClient(new MyConfigReader()),
                new MyApplicationConfiguration(),
                new JourneysRepository(),
                new TravelInsightsRepository());

        }

        private static NumericRangeBlocks.TravelInsightsAnalysisService GetNumericRangeInsightService()
        {
            return new NumericRangeBlocks.TravelInsightsAnalysisService(new TasklingClient(new MyConfigReader()),
                new MyApplicationConfiguration(),
                new JourneysRepository(),
                new TravelInsightsRepository());

        }

        private static ListBlocks.TravelInsightsAnalysisService GetListInsightService()
        {
            return new ListBlocks.TravelInsightsAnalysisService(new TasklingClient(new MyConfigReader()),
                new MyApplicationConfiguration(),
                new JourneysRepository(),
                new NotificationService());

        }
    }
}
