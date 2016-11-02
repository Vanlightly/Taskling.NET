using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Taskling.Configuration;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.CleanUp;

namespace Taskling.CleanUp
{
    public class CleanUpService : ICleanUpService
    {
        private readonly ICleanUpRepository _cleanUpRepository;
        private readonly ITasklingConfiguration _tasklingConfiguration;

        private delegate void CleanUpDelegate(string applicationName, string taskName);

        public CleanUpService(ITasklingConfiguration tasklingConfiguration, ICleanUpRepository cleanUpRepository)
        {
            _cleanUpRepository = cleanUpRepository;
            _tasklingConfiguration = tasklingConfiguration;
        }

        public void CleanOldData(string applicationName, string taskName)
        {
            var cleanUpDelegate = new CleanUpDelegate(StartCleanOldData);
            cleanUpDelegate.BeginInvoke(applicationName, taskName, new AsyncCallback(CleanUpCallback), cleanUpDelegate);
        }

        private void StartCleanOldData(string applicationName, string taskName)
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
                _cleanUpRepository.CleanOldData(request);
            }
            catch (Exception ex)
            {
                Trace.TraceError("Failed to clean old data. If this continues, data size could grow very large. " + ex);
            }
        }

        private void CleanUpCallback(IAsyncResult ar)
        {
            try
            {
                var caller = (CleanUpDelegate)ar.AsyncState;
                caller.EndInvoke(ar);
            }
            catch (Exception ex)
            {
                Trace.TraceError("Failed to clean old data. If this continues, data size could grow very large. " + ex);
            }
        }
    }
}
