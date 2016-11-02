using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Tasks;

namespace Taskling.Fluent
{
    public interface IReprocessSettings
    {
        string CurrentReferenceValue { get; set; }
        ReprocessOption ReprocessOption { get; set; }
        string ReferenceValueToReprocess { get; set; }
    }
}
