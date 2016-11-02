using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.Fluent
{
    public interface IFluentBlockSettingsDescriptor
    {
        IFluentBlockSettingsDescriptor ReprocessFailedTasks(TimeSpan detectionRange, short retryLimit);
        IFluentBlockSettingsDescriptor ReprocessDeadTasks(TimeSpan detectionRange, short retryLimit);
        IComplete MaximumBlocksToGenerate(int maximumNumberOfBlocks);
    }
}
