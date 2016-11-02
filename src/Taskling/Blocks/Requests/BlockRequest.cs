using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.Common;
using Taskling.Tasks;

namespace Taskling.Blocks.Requests
{
    public class BlockRequest
    {
        public string ApplicationName { get; set; }
        public string TaskName { get; set; }
        public string TaskExecutionId { get; set; }

        public int MaxBlocks { get; set; }

        public bool ReprocessFailedTasks { get; set; }
        public TimeSpan FailedTaskDetectionRange { get; set; }
        public short FailedTaskRetryLimit { get; set; }

        public TaskDeathMode TaskDeathMode { get; set; }
        public bool ReprocessDeadTasks { get; set; }
        public short DeadTaskRetryLimit { get; set; }
        public TimeSpan OverrideDeathThreshold { get; set; }
        public TimeSpan DeadTaskDetectionRange { get; set; }
        public TimeSpan KeepAliveDeathThreshold { get; set; }

        public BlockType BlockType { get; protected set; }

        public string ReprocessReferenceValue { get; set; }
        public ReprocessOption ReprocessOption { get; set; }
    }
}
