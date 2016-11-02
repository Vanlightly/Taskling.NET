using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Taskling.Exceptions;

namespace Taskling.Configuration
{
    public class TasklingConfiguration : ITasklingConfiguration
    {
        private readonly IConfigurationReader _configurationReader;
        private Dictionary<string, TaskConfiguration> _taskConfigurations;
        private object _cacheSync = new object();

        public TasklingConfiguration(IConfigurationReader configurationReader)
        {
            _configurationReader = configurationReader;
            _taskConfigurations = new Dictionary<string, TaskConfiguration>();
        }

        public TaskConfiguration GetTaskConfiguration(string applicationName, string taskName)
        {
            if (string.IsNullOrEmpty(applicationName))
                throw new TaskConfigurationException("Cannot load a TaskConfiguration, ApplicationName is null or empty");

            if (string.IsNullOrEmpty(taskName))
                throw new TaskConfigurationException("Cannot load a TaskConfiguration, TaskName is null or empty");

            lock (_cacheSync)
            {
                var key = GetCacheKey(applicationName, taskName);
                bool loadFromConfigFile = false;
                if (!_taskConfigurations.ContainsKey(key))
                    loadFromConfigFile = true;
                else if ((DateTime.UtcNow - _taskConfigurations[key].DateLoaded).Minutes > 10)
                    loadFromConfigFile = true;

                if (loadFromConfigFile)
                {
                    var configuration = LoadConfiguration(applicationName, taskName);
                    configuration.ApplicationName = applicationName;
                    configuration.TaskName = taskName;
                    configuration.DateLoaded = DateTime.UtcNow;

                    if (!_taskConfigurations.ContainsKey(key))
                        _taskConfigurations.Add(key, configuration);
                    else
                        _taskConfigurations[key] = configuration;
                }

                return _taskConfigurations[key];
            }
        }

        private string GetCacheKey(string applicationName, string taskName)
        {
            return applicationName + "::" + taskName;
        }

        private TaskConfiguration LoadConfiguration(string applicationName, string taskName)
        {
            var configString = FindKey(applicationName, taskName);
            var taskConfiguration = ParseConfigString(configString, applicationName, taskName);

            return taskConfiguration;
        }

        private TaskConfiguration ParseConfigString(string configString, string applicationName, string taskName)
        {
            var databaseConnString = GetConnStringElement(configString, "DB", true);

            var taskConfiguration = new TaskConfiguration();
            taskConfiguration.SetDefaultValues(applicationName,
                taskName,
                databaseConnString.Value);

            var concurrencyLimit = GetIntElement(configString, "CON", false);
            if (concurrencyLimit.Exists)
                taskConfiguration.ConcurrencyLimit = concurrencyLimit.Value;

            var databaseTimeoutSeconds = GetIntElement(configString, "TO", false);
            if (databaseTimeoutSeconds.Exists)
                taskConfiguration.DatabaseTimeoutSeconds = databaseTimeoutSeconds.Value;

            var enabled = GetBoolElement(configString, "E", false);
            if (enabled.Exists)
                taskConfiguration.Enabled = enabled.Value;

            var keepAliveDeathThresholdMinutes = GetDoubleElement(configString, "KADT", false);
            if (keepAliveDeathThresholdMinutes.Exists)
                taskConfiguration.KeepAliveDeathThresholdMinutes = keepAliveDeathThresholdMinutes.Value;

            var keepAliveIntervalMinutes = GetDoubleElement(configString, "KAINT", false);
            if (keepAliveIntervalMinutes.Exists)
                taskConfiguration.KeepAliveIntervalMinutes = keepAliveIntervalMinutes.Value;

            var keepGeneralDataForDays = GetIntElement(configString, "KPDT", false);
            if (keepGeneralDataForDays.Exists)
                taskConfiguration.KeepGeneralDataForDays = keepGeneralDataForDays.Value;

            var keepListItemsForDays = GetIntElement(configString, "KPLT", false);
            if (keepListItemsForDays.Exists)
                taskConfiguration.KeepListItemsForDays = keepListItemsForDays.Value;

            var minimumCleanUpIntervalHours = GetIntElement(configString, "MCI", false);
            if (minimumCleanUpIntervalHours.Exists)
                taskConfiguration.MinimumCleanUpIntervalHours = minimumCleanUpIntervalHours.Value;

            var timePeriodDeathThresholdMinutes = GetDoubleElement(configString, "TPDT", false);
            if (timePeriodDeathThresholdMinutes.Exists)
                taskConfiguration.TimePeriodDeathThresholdMinutes = timePeriodDeathThresholdMinutes.Value;

            var usesKeepAliveMode = GetBoolElement(configString, "KA", false);
            if (usesKeepAliveMode.Exists)
                taskConfiguration.UsesKeepAliveMode = usesKeepAliveMode.Value;

            var reprocessFailedTasks = GetBoolElement(configString, "RPC_FAIL", false);
            if (reprocessFailedTasks.Exists)
                taskConfiguration.ReprocessFailedTasks = reprocessFailedTasks.Exists;

            var reprocessFailedTasksDetectionRange = GetIntElement(configString, "RPC_FAIL_MTS", false);
            if (reprocessFailedTasksDetectionRange.Exists)
                taskConfiguration.ReprocessFailedTasksDetectionRange = new TimeSpan(0, reprocessFailedTasksDetectionRange.Value, 0);

            var failedTaskRetryLimit = GetIntElement(configString, "RPC_FAIL_RTYL", false);
            if (failedTaskRetryLimit.Exists)
                taskConfiguration.FailedTaskRetryLimit = (short)failedTaskRetryLimit.Value;

            var reprocessDeadTasks = GetBoolElement(configString, "RPC_DEAD", false);
            if (reprocessDeadTasks.Exists)
                taskConfiguration.ReprocessDeadTasks = reprocessDeadTasks.Value;

            var reprocessDeadTasksDetectionRange = GetIntElement(configString, "RPC_DEAD_MTS", false);
            if (reprocessDeadTasksDetectionRange.Exists)
                taskConfiguration.ReprocessDeadTasksDetectionRange = new TimeSpan(0, reprocessDeadTasksDetectionRange.Value, 0);

            var deadTaskRetryLimit = GetIntElement(configString, "RPC_DEAD_RTYL", false);
            if (deadTaskRetryLimit.Exists)
                taskConfiguration.DeadTaskRetryLimit = (short)deadTaskRetryLimit.Value;

            var maxBlocksToGenerate = GetIntElement(configString, "MXBL", false);
            if (maxBlocksToGenerate.Exists)
                taskConfiguration.MaxBlocksToGenerate = maxBlocksToGenerate.Value;

            var maxNonCompressedLength = GetIntElement(configString, "MXCOMP", false);
            if (maxNonCompressedLength.Exists)
                taskConfiguration.MaxLengthForNonCompressedData = maxNonCompressedLength.Value;

            var maxStatusReason = GetIntElement(configString, "MXRSN", false);
            if (maxStatusReason.Exists)
                taskConfiguration.MaxStatusReason = maxStatusReason.Value;

            return taskConfiguration;
        }

        //

        private ConfigElement<string> GetConnStringElement(string configString, string element, bool required)
        {
            var pattern = element + @"\[([^\]]+)\]";
            var value = GetElement(configString, pattern, element, required);
            if (string.IsNullOrEmpty(value))
                return new ConfigElement<string>();

            return new ConfigElement<string>(value);
        }

        private ConfigElement<string> GetStringElement(string configString, string element, bool required)
        {
            var pattern = element + @"\[(\w+)\]";
            var value = GetElement(configString, pattern, element, required);
            if (string.IsNullOrEmpty(value))
                return new ConfigElement<string>();

            return new ConfigElement<string>(value);
        }

        private ConfigElement<int> GetIntElement(string configString, string element, bool required)
        {
            var pattern = element + @"\[(-?\d+)\]";
            var value = GetElement(configString, pattern, element, required);
            if (string.IsNullOrEmpty(value))
                return new ConfigElement<int>();

            int intValue = 0;
            var success = int.TryParse(value, out intValue);
            if (success)
                return new ConfigElement<int>(intValue);

            throw new TaskConfigurationException("Element " + element + " is expected to be an int, but it is not");
        }

        private ConfigElement<bool> GetBoolElement(string configString, string element, bool required)
        {
            var pattern = element + @"\[(\w+)\]";
            var value = GetElement(configString, pattern, element, required);
            if (string.IsNullOrEmpty(value))
                return new ConfigElement<bool>();

            bool boolValue = false;
            var success = bool.TryParse(value, out boolValue);
            if (success)
                return new ConfigElement<bool>(boolValue);

            throw new TaskConfigurationException("Element " + element + " is expected to be an bool, but it is not");
        }

        private ConfigElement<double> GetDoubleElement(string configString, string element, bool required)
        {
            var pattern = element + @"\[(\d+(?:\.\d+)*)\]";
            var value = GetElement(configString, pattern, element, required);
            if (string.IsNullOrEmpty(value))
                return new ConfigElement<double>();

            double doubleValue = 0;
            var success = double.TryParse(value, out doubleValue);
            if (success)
                return new ConfigElement<double>(doubleValue);

            throw new TaskConfigurationException("Element " + element + " is expected to be an double, but it is not");
        }

        private string GetElement(string configString, string pattern, string element, bool required)
        {
            var match = Regex.Match(configString, pattern);
            if (match.Success && match.Groups.Count == 2)
            {
                return match.Groups[1].Value;
            }

            if (required)
                throw new TaskConfigurationException("No " + element + " setting was found");
            else
                return string.Empty;
        }

        private string FindKey(string applicationName, string taskName)
        {
            return _configurationReader.GetTaskConfigurationString(applicationName, taskName);
        }

    }
}
