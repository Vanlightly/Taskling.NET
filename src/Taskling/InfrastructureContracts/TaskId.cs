using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.InfrastructureContracts
{
    public class TaskId
    {
        public TaskId(string applicationName, string taskName)
        {
            ApplicationName = applicationName;
            TaskName = taskName;
        }

        public string ApplicationName { get; set; }
        public string TaskName { get; set; }

        public override bool Equals(object obj)
        {
            if (obj == null)
                return false;

            var taskId = obj as TaskId;
            if (taskId == null)
                return false;

            return taskId.ApplicationName == ApplicationName
                   && taskId.TaskName == TaskName;
        }

        public override int GetHashCode()
        {
            unchecked // Overflow is fine, just wrap
            {
                int hash = 17;

                if (ApplicationName != null)
                    hash = hash * 23 + ApplicationName.GetHashCode();

                if (TaskName != null)
                    hash = hash * 23 + TaskName.GetHashCode();

                return hash;
            }
        }
    }
}
