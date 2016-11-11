using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TasklingTester
{
    public interface IJourneysRepository
    {
        long GetMaxJourneyId();
        DateTime GetMaxJourneyDate();
        IList<Journey> GetJourneys(long startId, long endId);
        IList<Journey> GetJourneys(DateTime startDate, DateTime endDate);
    }
}
