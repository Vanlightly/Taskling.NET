using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Taskling.Configuration;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.CleanUp;

namespace Taskling.CleanUp
{
    public class CleanUpService : ICleanUpService
    {
        private readonly ICleanUpRepository _cleanUpRepository;
        private readonly ITasklingConfiguration _tasklingConfiguration;

        public CleanUpService(ITasklingConfiguration tasklingConfiguration, ICleanUpRepository cleanUpRepository)
        {
            _cleanUpRepository = cleanUpRepository;
            _tasklingConfiguration = tasklingConfiguration;
        }

        public void CleanOldData(string applicationName, string taskName)
        {
            Task.Run(async () => await StartCleanOldDataAsync(applicationName, taskName));
        }

        private async Task StartCleanOldDataAsync(string applicationName, string taskName)
        {
            try
            {
                var configuration = _tasklingConfiguration.GetTaskConfiguration(applicationName, taskName);
                var request = new CleanUpRequest()
                {
                    TaskId = new TaskId(applicationName, taskName),
                    GeneralDateThreshold = DateTime.UtcNow.AddDays(-1 * configuration.KeepGeneralDataForDays),
                    ListItemDateThreshold = DateTime.UtcNow.AddDays(-1 * configuration.KeepListItemsForDays),
                    TimeSinceLastCleaningThreashold = new TimeSpan(configuration.MinimumCleanUpIntervalHours, 0, 0)
                };
                await _cleanUpRepository.CleanOldDataAsync(request);
            }
            catch (Exception ex)
            {
                Trace.TraceError("Failed to clean old data. If this continues, data size could grow very large. " + ex);
            }
        }
    }
}
