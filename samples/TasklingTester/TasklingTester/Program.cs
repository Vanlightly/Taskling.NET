using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Taskling.SqlServer;
using TasklingTester.Configuration;
using TasklingTester.Repositories;
using TasklingTester.ListBlocks;

namespace TasklingTester
{
    class Program
    {
        static void Main(string[] args)
        {
            //var insightService = GetDateRangeInsightService();
            //insightService.RunBatchJob();

            //var insightService = GetNumericRangeInsightService();
            //insightService.RunBatchJob();

            var insightService = GetListInsightService();
            insightService.RunBatchJob();
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
