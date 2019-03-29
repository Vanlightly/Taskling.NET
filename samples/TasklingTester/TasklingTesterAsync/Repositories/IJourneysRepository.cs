using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TasklingTesterAsync.Repositories
{
    public interface IJourneysRepository
    {
        Task<long> GetMaxJourneyIdAsync();
        Task<DateTime> GetMaxJourneyDateAsync();
        Task<IList<Journey>> GetJourneysAsync(long startId, long endId);
        Task<IList<Journey>> GetJourneysAsync(DateTime startDate, DateTime endDate);
    }
}
