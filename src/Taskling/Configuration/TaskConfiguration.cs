using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Configuration
{
    public class TaskConfiguration
    {
        public void SetDefaultValues(string applicationName, string taskName, string databaseConnString)
        {
            ApplicationName = applicationName;
            TaskName = taskName;
            DatabaseConnectionString = databaseConnString;

            DatabaseTimeoutSeconds = 120;
            Enabled = true;
            ConcurrencyLimit = -1;
            KeepListItemsForDays = 14;
            KeepGeneralDataForDays = 40;
            MinimumCleanUpIntervalHours = 1;
            UsesKeepAliveMode = true;
            KeepAliveIntervalMinutes = 1;
            KeepAliveDeathThresholdMinutes = 10;
            ReprocessFailedTasks = false;
            ReprocessDeadTasks = false;
            MaxBlocksToGenerate = 10000;
            MaxLengthForNonCompressedData = 2000;
            MaxStatusReason = 1000;
        }

        public string ApplicationName { get; set; }
        public string TaskName { get; set; }
        public int DatabaseTimeoutSeconds { get; set; }
        public string DatabaseConnectionString { get; set; }

        // concurrency
        public bool Enabled { get; set; }
        public int ConcurrencyLimit { get; set; }

        // clean up
        public int KeepListItemsForDays { get; set; }
        public int KeepGeneralDataForDays { get; set; }
        public int MinimumCleanUpIntervalHours { get; set; }

        // death detection configuration
        public bool UsesKeepAliveMode { get; set; }
        public double KeepAliveIntervalMinutes { get; set; }
        public double KeepAliveDeathThresholdMinutes { get; set; }
        public double TimePeriodDeathThresholdMinutes { get; set; }

        // reprocess failed tasks
        public bool ReprocessFailedTasks { get; set; }
        public TimeSpan ReprocessFailedTasksDetectionRange { get; set; }
        public short FailedTaskRetryLimit { get; set; }

        // reprocess dead tasks
        public bool ReprocessDeadTasks { get; set; }
        public TimeSpan ReprocessDeadTasksDetectionRange { get; set; }
        public short DeadTaskRetryLimit { get; set; }

        // Blocks
        public int MaxBlocksToGenerate { get; set; }
        public int MaxLengthForNonCompressedData { get; set; }
        public int MaxStatusReason { get; set; }

        public DateTime DateLoaded { get; set; }
    }
}
